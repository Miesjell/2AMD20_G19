from typing import Dict, Any, Optional, List
from neo4j import Driver
from tqdm import tqdm

class RelationshipBuilder:
    """
    Scalable relationship manager for the food knowledge graph.
    Handles relationships between recipes, diets, allergens, and more.
    """

    def __init__(self, driver: Optional[Driver] = None):
        self.driver = driver

    def set_driver(self, driver: Driver) -> None:
        self.driver = driver

    def create_relationships(self) -> Dict[str, Any]:
        if not self.driver:
            return {"status": "error", "error": "Neo4j driver not set. Call set_driver() first."}

        definitions = [
            self._non_vegetarian_exclusions(),
            self._vegetarian_inclusions(),
            self._allergen_links(),
            self._price_category_assignment(),
            self._personalized_recommendations(),
        ]

        results = []
        with self.driver.session() as session:
            for rel_def in tqdm(definitions, desc="Creating relationships", unit="relationship"):
                try:
                    tqdm.write(f"Creating {rel_def['name']} relationships: {rel_def['description']}")
                    session.run(rel_def["query"])
                    results.append({"relationship": rel_def["name"], "description": rel_def["description"], "status": "created", "error": None})
                except Exception as e:
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
        return {
            "name": "vegetarian_inclusions",
            "description": "Connect vegetarian-friendly recipes to vegetarian diet preference",
            "query": """
                MATCH (r:Recipe)
                WHERE NOT EXISTS {
                    MATCH (r)-[:CONTAINS]->(i:Ingredient)
                    WHERE i.is_meat = true
                }
                MERGE (d:DietPreference {name: 'Vegetarian'})
                MERGE (d)-[:INCLUDES]->(r)
            """
        }

    def _allergen_links(self) -> Dict[str, str]:
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
        return {
            "name": "recipe_recommendations",
            "description": "Generate personalized recipe recommendations based on diet and allergy",
            "query": """
                MATCH (p:Person)-[:HAS_DIETARY_PREFERENCE]->(d:DietPreference)-[:INCLUDES]->(r:Recipe)
                WHERE (p.budget IS NULL OR r.price_range = p.budget OR r.price_range IS NULL)
                  AND NOT EXISTS {
                      MATCH (p)-[:HAS_ALLERGY]->(a:Allergy)<-[:MAY_CONTAIN_ALLERGEN]-(r)
                  }
                WITH p, r
                LIMIT 500
                MERGE (p)-[:RECOMMENDED_RECIPE]->(r)
            """
        }

    def get_relationship_definitions(self) -> List[Dict[str, str]]:
        return [
            self._non_vegetarian_exclusions(),
            self._vegetarian_inclusions(),
            self._allergen_links(),
            self._price_category_assignment(),
            self._personalized_recommendations()
        ]
