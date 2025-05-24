"""
Relationship builder for the food knowledge graph.

This module creates relationships between various nodes in the graph,
including recipe-ingredient relationships, diet preferences, allergens,
price categorization, and meal typing.
"""
from typing import Dict, Any, Optional, List
import logging
from neo4j import Driver
from tqdm import tqdm

class RelationshipBuilder:
    """
    Scalable relationship manager for the food knowledge graph.
    Handles relationships between recipes, diets, allergens, and more.
    """

    def __init__(self, driver: Optional[Driver] = None):
        self.driver = driver
        self.logger = logging.getLogger(__name__)

    def set_driver(self, driver: Driver) -> None:
        """Set the Neo4j driver for database connections."""
        self.driver = driver

    def create_relationships(self) -> Dict[str, Any]:
        """
        Create all relationship types in the knowledge graph.
        
        Returns:
            Dict with relationship creation results
        """
        if not self.driver:
            return {"status": "error", "error": "Neo4j driver not set. Call set_driver() first."}

        definitions = [
            self._non_vegetarian_exclusions(),
            self._vegetarian_inclusions(),
            self._allergen_links(),
            self._price_category_assignment(),
            self._personalized_recommendations(),
            self._meal_type_categorization(),
        ]

        results = []
        with self.driver.session() as session:
            for rel_def in tqdm(definitions, desc="Creating relationships", unit="relationship"):
                try:
                    self.logger.info(f"Creating {rel_def['name']} relationships")
                    session.run(rel_def["query"])
                    results.append({"relationship": rel_def["name"], "description": rel_def["description"], "status": "created", "error": None})
                except Exception as e:
                    self.logger.error(f"Failed to create {rel_def['name']} relationships: {str(e)}")
                    results.append({"relationship": rel_def["name"], "description": rel_def["description"], "status": "failed", "error": str(e)})

        return {
            "status": "success" if all(r["status"] == "created" for r in results) else "partial_success",
            "relationships": results,
            "summary": {
                "total": len(results),
                "successful": sum(1 for r in results if r["status"] == "created"),
                "failed": sum(1 for r in results if r["status"] == "failed")
            }
        }

    def _non_vegetarian_exclusions(self) -> Dict[str, str]:
        """Create exclusion relationships for non-vegetarian recipes."""
        return {
            "name": "non_vegetarian_exclusions",
            "description": "Flag recipes containing meat products as excluded from vegetarian diet",
            "query": """
                MATCH (r:Recipe)-[:CONTAINS]->(i:Ingredient)
                WHERE i.is_meat = true
                WITH DISTINCT r
                MERGE (d:DietPreference {name: 'Vegetarian'})
                MERGE (d)-[:EXCLUDES]->(r)
            """
        }

    def _vegetarian_inclusions(self) -> Dict[str, str]:
        """Create inclusion relationships for vegetarian-friendly recipes."""
        return {
            "name": "vegetarian_inclusions",
            "description": "Connect vegetarian-friendly food recipes to vegetarian diet preference",
            "query": """
                // Identify vegetarian-friendly recipes (excluding drinks and non-food items)
                MATCH (r:Recipe)
                WHERE NOT EXISTS {
                    MATCH (r)-[:CONTAINS]->(i:Ingredient)
                    WHERE i.is_meat = true
                }
                // Exclude drinks from vegetarian diet inclusions
                AND NOT EXISTS {
                    MATCH (r)-[:IS_TYPE]->(:MealType {name: 'Drink'})
                }
                // Only include recipes with ingredients (real food items)
                AND EXISTS {
                    MATCH (r)-[:CONTAINS]->(:Ingredient)
                }
                MERGE (d:DietPreference {name: 'Vegetarian'})
                MERGE (d)-[:INCLUDES]->(r)
            """
        }

    def _allergen_links(self) -> Dict[str, str]:
        """Create allergen relationships between recipes and allergies."""
        return {
            "name": "allergen_relationships",
            "description": "Flag recipes that may contain allergens",
            "query": """
                MATCH (a:Allergy)-[:PROHIBITS]->(f:FoodItem)
                MATCH (r:Recipe)-[:CONTAINS]->(i:Ingredient)
                WHERE i.name = f.name OR i.name CONTAINS f.name
                MERGE (r)-[:MAY_CONTAIN_ALLERGEN]->(a)
            """
        }

    def _price_category_assignment(self) -> Dict[str, str]:
        """Categorize recipes into price ranges based on calorie content."""
        return {
            "name": "price_categories",
            "description": "Categorize recipes into price ranges based on calorie content",
            "query": """
                MATCH (r:Recipe)
                WHERE r.calories IS NOT NULL AND r.calories > 0
                WITH r, 
                     CASE 
                         WHEN r.calories < 300 THEN 'low'
                         WHEN r.calories < 600 THEN 'medium'
                         ELSE 'high'
                     END AS category
                SET r.price_range = category
            """
        }

    def _personalized_recommendations(self) -> Dict[str, str]:
        """Create personalized recipe recommendations for users."""
        return {
            "name": "recipe_recommendations",
            "description": "Generate personalized recipe recommendations based on diet and allergy",
            "query": """
                // First create sample diet preference nodes if none exist
                MERGE (vegetarian:DietPreference {name: 'Vegetarian'})
                MERGE (vegan:DietPreference {name: 'Vegan'})
                MERGE (glutenFree:DietPreference {name: 'Gluten-Free'})
                
                // Create sample persons if they don't exist
                MERGE (p1:Person {id: 'user_1'})
                MERGE (p2:Person {id: 'user_2'})
                MERGE (p3:Person {id: 'user_3'})
                
                // Connect persons to diet preferences
                MERGE (p1)-[:HAS_DIETARY_PREFERENCE]->(vegetarian)
                MERGE (p2)-[:HAS_DIETARY_PREFERENCE]->(vegan)
                MERGE (p3)-[:HAS_DIETARY_PREFERENCE]->(glutenFree)
                
                // Now create the recipe recommendations
                WITH p1, p2, p3, vegetarian, vegan, glutenFree
                MATCH (p:Person)
                WITH p
                MATCH (p)-[:HAS_DIETARY_PREFERENCE]->(d:DietPreference)-[:INCLUDES]->(r:Recipe)
                WHERE (p.budget IS NULL OR r.price_range = p.budget OR r.price_range IS NULL)
                  AND NOT EXISTS {
                      MATCH (p)-[:HAS_ALLERGY]->(a:Allergy)<-[:MAY_CONTAIN_ALLERGEN]-(r)
                  }
                WITH p, r
                LIMIT 500
                MERGE (p)-[:RECOMMENDED_RECIPE]->(r)
                
                // Also create basic recommendations for users without diet preferences
                WITH *
                MATCH (p:Person)
                WHERE NOT EXISTS { 
                    MATCH (p)-[:RECOMMENDED_RECIPE]->(:Recipe) 
                }
                WITH p
                MATCH (r:Recipe)
                WHERE EXISTS { 
                    MATCH (r)-[:CONTAINS]->(:Ingredient) 
                }
                WITH p, r
                ORDER BY r.calories ASC
                WITH p, collect(r)[0..5] AS topRecipes
                UNWIND topRecipes AS recipe
                MERGE (p)-[:RECOMMENDED_RECIPE]->(recipe)
            """
        }
        
    def _meal_type_categorization(self) -> Dict[str, str]:
        """Create MealType nodes and link recipes using the precomputed `meal_type` property."""
        return {
            "name": "meal_type_categorization",
            "description": "Link each recipe to its corresponding MealType based on preprocessed `meal_type` property.",
            "query": """
                // Create meal type nodes
                FOREACH (name IN ['Breakfast', 'Lunch', 'Dinner', 'Drink', 'Other'] |
                    MERGE (:MealType {name: name})
                )

                // Use WITH to separate FOREACH from next MATCH
                WITH 1 as dummy

                // Match recipes with a known meal_type
                MATCH (r:Recipe)
                WHERE r.meal_type IS NOT NULL

                // Link recipe to its corresponding MealType
                MATCH (m:MealType {name: r.meal_type})
                MERGE (r)-[:IS_TYPE]->(m)
            """
        }


    def get_relationship_definitions(self) -> List[Dict[str, str]]:
        """Get all relationship definitions for documentation purposes."""
        return [
            self._non_vegetarian_exclusions(),
            self._vegetarian_inclusions(),
            self._allergen_links(),
            self._price_category_assignment(),
            self._personalized_recommendations(),
            self._meal_type_categorization()
        ]
