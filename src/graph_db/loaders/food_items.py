"""
Food item loader for the knowledge graph.
"""
from typing import Dict, Any, List, Optional

import pandas as pd
from neo4j import Driver

from .base import DataLoader


class FoodItemLoader(DataLoader):
    """Loader for food items and their allergen relationships."""
    
    def __init__(self, driver: Optional[Driver] = None):
        """Initialize the food item loader."""
        super().__init__(driver)
    
    def load_data(self, data: pd.DataFrame, batch_size: int = 50) -> Dict[str, Any]:
        """
        Load food items into the Neo4j database.
        
        Args:
            data: DataFrame with food data
            batch_size: Number of records to process in each batch
            
        Returns:
            Dict with load results
        """
        if not self.driver:
            return {"status": "error", "error": "Neo4j driver not set. Call set_driver() first."}
        
        # Create batches for efficient loading
        batches = self.batch_data(data, batch_size)
        
        # Cypher query for batch loading
        query = """
        UNWIND $foods AS food
        MERGE (f:FoodItem {name: food.name})
        SET f.class = food.class,
            f.type = food.type,
            f.group = food.group,
            f.allergens = food.allergy
        WITH f, food
        WHERE food.allergy IS NOT NULL AND food.allergy <> 'NaN'
        MERGE (a:Allergy {name: food.allergy})
        MERGE (f)-[:CAUSES_ALLERGY]->(a)
        MERGE (a)-[:PROHIBITS]->(f)
        """
        
        total_processed = 0
        errors = []
        
        with self.driver.session() as session:
            for i, batch in enumerate(batches):
                # Prepare data for this batch
                foods = []
                for _, row in batch.iterrows():
                    food = {
                        "name": row["Food"],
                        "class": row["Class"],
                        "type": row["Type"],
                        "group": row["Group"],
                        "allergy": row["Allergy"] if pd.notna(row["Allergy"]) else None
                    }
                    foods.append(food)
                
                # Execute the batch
                try:
                    session.run(query, foods=foods)
                    total_processed += len(foods)
                except Exception as e:
                    errors.append(str(e))
        
        return {
            "status": "success" if not errors else "partial_success" if total_processed > 0 else "error",
            "total_processed": total_processed,
            "total_records": len(data),
            "errors": errors
        }