"""
Relationship builder for the food knowledge graph.

This module provides functionality for creating complex relationships between
entities in the Neo4j database, such as connecting recipes to diets, identifying
recipes that may contain allergens, and generating personalized recipe recommendations.
"""
from typing import Dict, Any, Optional, List

from neo4j import Driver
from tqdm import tqdm


class RelationshipBuilder:
    """
    Creates and manages relationships between entities in the food knowledge graph.
    
    This class handles the creation of non-trivial relationships that involve
    complex queries across multiple node types, such as dietary restrictions,
    allergen warnings, and price categorization.
    """
    
    def __init__(self, driver: Optional[Driver] = None):
        """
        Initialize the relationship builder.
        
        Args:
            driver: Neo4j driver instance (optional)
        """
        self.driver = driver
        
        # Define relationship types and queries
        self.relationship_definitions = [
            {
                "name": "non_vegetarian_exclusions",
                "description": "Flag recipes containing meat products as excluded from vegetarian diet",
                "query": """
                MATCH (r:Recipe)-[:CONTAINS]->(i:Ingredient)
                WHERE any(term IN ['meat', 'beef', 'chicken', 'pork', 'fish'] 
                          WHERE toLower(i.name) CONTAINS term)
                WITH DISTINCT r
                MERGE (d:DietPreference {name: 'Vegetarian'})
                MERGE (d)-[:EXCLUDES]->(r)
                """
            },
            {
                "name": "vegetarian_inclusions",
                "description": "Connect vegetarian-friendly recipes to vegetarian diet preference",
                "query": """
                MATCH (r:Recipe)
                WHERE NOT EXISTS {
                    MATCH (r)-[:CONTAINS]->(i:Ingredient)
                    WHERE any(term IN ['meat', 'beef', 'chicken', 'pork', 'fish'] 
                              WHERE toLower(i.name) CONTAINS term)
                }
                WITH r
                MERGE (d:DietPreference {name: 'Vegetarian'})
                MERGE (d)-[:INCLUDES]->(r)
                """
            },
            {
                "name": "allergen_relationships",
                "description": "Flag recipes that may contain allergens based on their ingredients",
                "query": """
                MATCH (a:Allergy)-[:PROHIBITS]->(f:FoodItem)
                MATCH (r:Recipe)-[:CONTAINS]->(i:Ingredient)
                WHERE toLower(i.name) CONTAINS toLower(f.name)
                MERGE (r)-[:MAY_CONTAIN_ALLERGEN]->(a)
                """
            },
            {
                "name": "price_categories",
                "description": "Categorize recipes into price ranges based on calorie content",
                "query": """
                MATCH (r:Recipe)
                WHERE r.calories > 0
                WITH r, 
                CASE 
                    WHEN r.calories < 300 THEN 'low'
                    WHEN r.calories < 600 THEN 'medium'
                    ELSE 'high'
                END AS priceCategory
                SET r.price_range = priceCategory
                """
            },
            {
                "name": "recipe_recommendations",
                "description": "Generate personalized recipe recommendations based on diet preferences and allergies",
                "query": """
                MATCH (p:Person)-[:FOLLOWS]->(d:DietPreference)-[:INCLUDES]->(r:Recipe)
                WHERE p.budget IS NULL OR r.price_range = p.budget OR r.price_range IS NULL
                AND NOT EXISTS {
                    MATCH (p)-[:HAS_ALLERGY]->(a:Allergy)<-[:MAY_CONTAIN_ALLERGEN]-(r)
                }
                MERGE (p)-[:RECOMMENDED_RECIPE]->(r)
                """
            }
        ]
    
    def set_driver(self, driver: Driver) -> None:
        """
        Set the Neo4j driver.
        
        Args:
            driver: Neo4j driver instance
        """
        self.driver = driver
    
    def create_relationships(self) -> Dict[str, Any]:
        """
        Create all relationships between entities in the graph.
        
        This method executes the defined relationship queries in sequence.
        The relationships include:
        1. Identifying non-vegetarian recipes
        2. Identifying vegetarian recipes
        3. Flagging recipes with potential allergens
        4. Categorizing recipes by price
        5. Generating personalized recipe recommendations
        
        Returns:
            Dict with relationship creation results
        """
        if not self.driver:
            return {"status": "error", "error": "Neo4j driver not set. Call set_driver() first."}
        
        results = []
        
        with self.driver.session() as session:
            # Process each relationship definition with progress bar
            for rel_def in tqdm(self.relationship_definitions, 
                             desc="Creating relationships", 
                             unit="relationship"):
                try:
                    # Show which relationship is being created
                    tqdm.write(f"Creating {rel_def['name']} relationships: {rel_def['description']}")
                    
                    # Execute the relationship query
                    session.run(rel_def["query"])
                    
                    # Record success
                    results.append({
                        "relationship": rel_def["name"],
                        "description": rel_def["description"],
                        "status": "created",
                        "error": None
                    })
                except Exception as e:
                    # Record failure with error message
                    results.append({
                        "relationship": rel_def["name"],
                        "description": rel_def["description"],
                        "status": "failed",
                        "error": str(e)
                    })
        
        # Determine overall status
        status = "success"
        if not results:
            status = "error"
        elif any(r["status"] == "failed" for r in results):
            status = "partial_success"
        
        return {
            "status": status,
            "relationships": results,
            "summary": {
                "total": len(results),
                "successful": sum(1 for r in results if r["status"] == "created"),
                "failed": sum(1 for r in results if r["status"] == "failed")
            }
        }
    
    def get_relationship_definitions(self) -> List[Dict[str, str]]:
        """
        Get all relationship definitions.
        
        Returns:
            List of relationship definition dictionaries
        """
        return self.relationship_definitions
