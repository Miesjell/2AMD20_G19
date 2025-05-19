"""
Food item loader for the knowledge graph.
"""
from typing import Dict, Any, List, Optional

import pandas as pd
from neo4j import Driver
from tqdm import tqdm

from .base import DataLoader


class FoodItemLoader(DataLoader):
    """Loader for food items and their allergen relationships."""
    
    def __init__(self, driver: Optional[Driver] = None):
        """Initialize the food item loader."""
        super().__init__(driver)
    
    def load_data(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Load food items into the Neo4j database.
        
        Args:
            data: DataFrame with food item data
            
        Returns:
            Dict with load results
        """
        if not self.driver:
            return {"status": "error", "error": "Neo4j driver not set. Call set_driver() first."}
        
        # Create batches for efficient loading
        batches = self.batch_data(data)
        
        # Cypher query for batch loading food items and their allergen relationships
        query = """
        UNWIND $foods AS food
        MERGE (f:FoodItem {name: food.name})
        SET f.class = food.class,
            f.type = food.type,
            f.group = food.group
        WITH f, food
        WHERE food.allergen IS NOT NULL
        MERGE (a:Allergy {name: food.allergen})
        MERGE (f)-[:CAUSES_ALLERGY]->(a)
        MERGE (a)-[:PROHIBITS]->(f)
        """
        
        total_processed = 0
        errors = []
        
        with self.driver.session() as session:
            # Add tqdm progress bar for batches
            for batch_idx, batch in enumerate(tqdm(batches, desc="Loading food items", unit="batch")):
                # Prepare data for this batch
                foods = []
                
                for idx, row in batch.iterrows():
                    try:
                        food = {
                            "name": self.clean_text(row["Food"]),
                            "class": self.clean_text(row["Class"]),
                            "type": self.clean_text(row["Type"]),
                            "group": self.clean_text(row["Group"]),
                            "allergen": self.clean_text(row["Allergy"])
                        }
                        foods.append(food)
                    except Exception as e:
                        errors.append(f"Error processing food item {idx}: {str(e)}")
                
                # Execute the batch
                try:
                    if foods:  # Only run if we have valid food items
                        session.run(query, foods=foods)
                        total_processed += len(foods)
                except Exception as e:
                    errors.append(f"Error in batch {batch_idx}: {str(e)}")
        
        return {
            "status": "success" if not errors else "partial_success" if total_processed > 0 else "error",
            "total_processed": total_processed,
            "total_records": len(data),
            "errors": errors[:10] if len(errors) > 10 else errors  # Limit error output
        }