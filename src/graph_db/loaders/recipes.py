from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
from neo4j import Driver
from tqdm import tqdm
import logging

from .base import DataLoader

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RecipeLoader(DataLoader):
    """
    Loader for recipes and their relationships into a Neo4j database.
    
    This class handles loading recipe data from pandas DataFrames, supporting multiple
    data formats, and creates appropriate nodes and relationships in Neo4j.
    """
    
    def __init__(self, driver: Optional[Driver] = None):
        """Initialize the recipe loader with an optional Neo4j driver."""
        super().__init__(driver)
    
    def load_data(self, data: pd.DataFrame, source_name: str, 
                  sample_size: Optional[int] = None, 
                  batch_size: int = 25) -> Dict[str, Any]:
        """
        Load recipes into the Neo4j database using vectorized operations.
        
        Args:
            data: DataFrame with recipe data.
            source_name: Name of the data source (e.g., 'full_format_recipes', 'recipes_parquet').
            sample_size: Optional sample size to limit processing.
            batch_size: Number of records to process in each batch.
            
        Returns:
            Dict with load results including status and statistics.
        """
        # Validate inputs
        if not isinstance(data, pd.DataFrame):
            logger.error("Input data must be a pandas DataFrame.")
            return {"status": "error", "error": "Input data must be a pandas DataFrame."}
        if not source_name or not isinstance(source_name, str):
            logger.error("Source name must be a non-empty string.")
            return {"status": "error", "error": "Source name must be a non-empty string."}
        if not self.driver:
            logger.error("Neo4j driver not set. Call set_driver() first.")
            return {"status": "error", "error": "Neo4j driver not set. Call set_driver() first."}
        
        logger.info(f"Loading recipes from {source_name}...")
        
        # Sample data if needed
        if sample_size and isinstance(sample_size, int) and sample_size > 0 and len(data) > sample_size:
            data = data.sample(sample_size, random_state=42).copy()
        
        # Create batches
        batches = self.batch_data(data, batch_size)
        
        # Cypher query for batch loading (aligned with logs: using FoodItem instead of Ingredient)
        query = """
        UNWIND $recipes AS recipe
        MERGE (r:Recipe {id: recipe.id})
        SET r.name = recipe.name,
            r.source = recipe.source,
            r.description = recipe.description,
            r.Calories = recipe.Calories,  // Changed to match expected property name
            r.fat = recipe.fat,
            r.protein = recipe.protein,
            r.sodium = recipe.sodium,
            r.preparation_description = recipe.preparation
        WITH r, recipe
        UNWIND [food_item IN recipe.food_items WHERE food_item IS NOT NULL AND food_item <> ''] AS food_item
        MERGE (f:FoodItem {name: food_item})
        MERGE (r)-[:CONTAINS]->(f)
        WITH r, recipe
        WHERE recipe.price_range IS NOT NULL
        SET r.price_range = recipe.price_range
        """
        
        # Initialize counters
        total_processed = 0
        errors = []
        recipes_with_food_items = 0
        recipes_without_food_items = 0
        
        with self.driver.session() as session:
            for batch in tqdm(batches, desc=f"Loading recipes from {source_name}", unit="batch"):
                # Prepare batch data
                batch_results = self._prepare_batch(batch, source_name)
                recipes = batch_results["recipes"]
                recipes_with_food_items += batch_results["with_food_items"]
                recipes_without_food_items += batch_results["without_food_items"]
                errors.extend(batch_results["errors"])
                
                # Execute batch
                if recipes:
                    try:
                        session.run(query, recipes=recipes)
                        total_processed += len(recipes)
                    except Exception as e:
                        error_msg = f"Batch processing error for {source_name}: {str(e)}"
                        logger.error(error_msg)
                        errors.append(error_msg)
        
        # Prepare result
        status = "success" if not errors else "partial_success" if total_processed > 0 else "error"
        result = {
            "status": status,
            "total_processed": total_processed,
            "total_records": len(data),
            "recipes_with_food_items": recipes_with_food_items,
            "recipes_without_food_items": recipes_without_food_items,
            "errors": errors[:10] if len(errors) > 10 else errors
        }
        logger.info(f"Completed loading recipes from {source_name}: {result}")
        return result
    
    def _prepare_batch(self, batch: pd.DataFrame, source_name: str) -> Dict[str, Any]:
        """
        Prepare a batch of recipes for database insertion using vectorized operations.
        
        Args:
            batch: DataFrame batch containing recipe data.
            source_name: Name of the data source.
            
        Returns:
            Dictionary containing prepared recipes and statistics.
        """
        errors = []
        batch = batch.copy()  # Avoid modifying the original DataFrame
        
        try:
            # Generate unique IDs
            batch["id"] = [f"{source_name}_{i}" for i in batch.index]
            
            # Define column mappings for different datasets
            column_mapping = {
                "name": ["title", "Name"],
                "description": ["desc", "Description"],
                "preparation": ["directions", "RecipeInstructions"],
                "Calories": ["calories", "Calories"],  # Changed to match expected property
                "fat": ["fat", "FatContent"],
                "protein": ["protein", "ProteinContent"],
                "sodium": ["sodium", "SodiumContent"],
                "food_items": ["RecipeIngredientParts", "ingredients"]  # Changed to FoodItem
            }
            
            # Create a clean DataFrame with standardized columns
            clean_batch = pd.DataFrame(index=batch.index)
            clean_batch["id"] = batch["id"]
            for target_col, possible_cols in column_mapping.items():
                for col in possible_cols:
                    if col in batch.columns:
                        clean_batch[target_col] = batch[col]
                        break
                else:
                    clean_batch[target_col] = np.nan
            
            # Clean text fields
            text_columns = ["name", "description", "preparation"]
            for col in text_columns:
                clean_batch[col] = clean_batch[col].apply(self.clean_text).fillna("")
            
            # Extract numeric fields
            numeric_columns = ["Calories", "fat", "protein", "sodium"]
            for col in numeric_columns:
                clean_batch[col] = pd.to_numeric(clean_batch[col], errors="coerce")
            
            # Extract food items (formerly ingredients)
            clean_batch["food_items"] = clean_batch["food_items"].apply(self._extract_food_items)
            
            # Count food item statistics
            with_food_items = clean_batch["food_items"].apply(
                lambda x: len(x) > 1 or (len(x) == 1 and x[0] != "Unknown food item")
            ).sum()
            without_food_items = len(clean_batch) - with_food_items
            
            # Add source and price_range
            clean_batch["source"] = source_name
            clean_batch["price_range"] = None  # Calculate if needed
            
            # Convert to list of dictionaries for Neo4j
            recipes = clean_batch.to_dict("records")
            
        except Exception as e:
            error_msg = f"Batch preparation error for {source_name}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)
            recipes = []
            with_food_items = 0
            without_food_items = len(batch)
        
        return {
            "recipes": recipes,
            "with_food_items": with_food_items,
            "without_food_items": without_food_items,
            "errors": errors
        }
    
    def _extract_food_items(self, value: Any) -> List[str]:
        """
        Extract food items from various field formats.
        
        Args:
            value: The value to extract food items from.
            
        Returns:
            List of cleaned food item strings.
        """
        food_items = []
        
        # Handle different input types
        if isinstance(value, (list, np.ndarray)):
            food_items = [self.clean_text(str(item)) for item in value if isinstance(item, str) and item.strip()]
        elif isinstance(value, str) and value.strip():
            food_items = [self.clean_text(value)]
        
        # Return placeholder if no valid food items found
        return food_items if food_items else ["Unknown food item"]