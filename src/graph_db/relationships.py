"""
Relationship builder for the food knowledge graph.
"""
from typing import Dict, Any, Optional

from neo4j import Driver
from tqdm import tqdm


class RelationshipBuilder:
    """Creates complex relationships between entities in the knowledge graph."""
    
    def __init__(self, driver: Optional[Driver] = None):
        """
        Initialize the relationship builder.
        
        Args:
            driver: Neo4j driver instance
        """
        self.driver = driver
    
    def set_driver(self, driver: Driver) -> None:
        """
        Set the Neo4j driver.
        
        Args:
            driver: Neo4j driver instance
        """
        self.driver = driver
    
    def create_relationships(self) -> Dict[str, Any]:
        """
        Create additional relationships between entities in the graph.
        
        Returns:
            Dict with relationship creation results
        """
        if not self.driver:
            return {"status": "error", "error": "Neo4j driver not set. Call set_driver() first."}
        
        relationships = [
            # 1. Connect recipes to diet preferences based on ingredients
            {
                "name": "non_vegetarian_exclusions",
                "query": """
                MATCH (r:Recipe)-[:CONTAINS]->(i:Ingredient)
                WHERE any(term IN ['meat', 'beef', 'chicken', 'pork', 'fish'] 
                          WHERE toLower(i.name) CONTAINS term)
                WITH DISTINCT r
                MERGE (d:DietPreference {name: 'Vegetarian'})
                MERGE (d)-[:EXCLUDES]->(r)
                """
            },
            # 2. Connect recipes to diet preferences for vegetarian recipes
            {
                "name": "vegetarian_inclusions",
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
            # 3. Flag recipes that may contain allergens
            {
                "name": "allergen_relationships",
                "query": """
                MATCH (a:Allergy)-[:PROHIBITS]->(f:FoodItem)
                MATCH (r:Recipe)-[:CONTAINS]->(i:Ingredient)
                WHERE toLower(i.name) CONTAINS toLower(f.name)
                MERGE (r)-[:MAY_CONTAIN_ALLERGEN]->(a)
                """
            },
            # 4. Create pricing relationships
            {
                "name": "price_categories",
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
            # 5. Connect persons to recommended recipes
            {
                "name": "recipe_recommendations",
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
        
        results = []
        
        with self.driver.session() as session:
            # Add tqdm for progress tracking of relationship creation
            for rel in tqdm(relationships, desc="Creating relationships", unit="relationship"):
                try:
                    # Show which relationship is being created
                    tqdm.write(f"Creating {rel['name']} relationships...")
                    session.run(rel["query"])
                    results.append({
                        "relationship": rel["name"],
                        "status": "created",
                        "error": None
                    })
                except Exception as e:
                    results.append({
                        "relationship": rel["name"],
                        "status": "failed",
                        "error": str(e)
                    })
        
        return {
            "status": "success" if all(r["status"] == "created" for r in results) else "partial_success",
            "relationships": results
        }