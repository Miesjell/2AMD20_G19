"""
Recipe loader for the knowledge graph.
"""
from typing import Dict, Any, List, Optional

import pandas as pd
import numpy as np
from neo4j import Driver

from .base import DataLoader


class RecipeLoader(DataLoader):
    """Loader for recipes and their ingredient relationships."""
    
    def __init__(self, driver: Optional[Driver] = None):
        """Initialize the recipe loader."""
        super().__init__(driver)
    
    def load_data(self, data: pd.DataFrame, source_name: str, 
                  sample_size: Optional[int] = None, 
                  batch_size: int = 25) -> Dict[str, Any]:
        """
        Load recipes into the Neo4j database.
        
        Args:
            data: DataFrame with recipe data
            source_name: Name of the data source
            sample_size: Optional sample size to limit processing
            batch_size: Number of records to process in each batch
            
        Returns:
            Dict with load results
        """
        if not self.driver:
            return {"status": "error", "error": "Neo4j driver not set. Call set_driver() first."}
        
        # Sample if needed to avoid processing too much data at once
        if sample_size and len(data) > sample_size:
            data = data.sample(sample_size, random_state=42)
        
        # Create batches for efficient loading
        batches = self.batch_data(data, batch_size)
        
        # Cypher query for batch loading recipes
        query = """
        UNWIND $recipes AS recipe
        MERGE (r:Recipe {id: recipe.id})
        SET r.name = recipe.name,
            r.source = recipe.source,
            r.description = recipe.description,
            r.calories = recipe.calories,
            r.fat = recipe.fat,
            r.protein = recipe.protein,
            r.sodium = recipe.sodium,
            r.preparation_description = recipe.preparation
        
        WITH r, recipe
        UNWIND recipe.ingredients AS ingredient
        WHERE ingredient IS NOT NULL AND ingredient <> ''
        MERGE (i:Ingredient {name: ingredient})
        MERGE (r)-[:CONTAINS]->(i)
        
        WITH r, recipe
        WHERE recipe.price_range IS NOT NULL
        SET r.price_range = recipe.price_range
        """
        
        total_processed = 0
        errors = []
        
        with self.driver.session() as session:
            for batch_idx, batch in enumerate(batches):
                # Prepare data for this batch
                recipes = []
                for idx, row in batch.iterrows():
                    try:
                        # Handle different column names in different dataframes
                        recipe = {
                            "id": f"{source_name}_{idx}",
                            "source": source_name,
                            "name": self.clean_text(row.get("title", row.get("Name", f"Recipe-{idx}"))),
                            "description": self.clean_text(row.get("desc", row.get("Description", ""))),
                            "preparation": self.clean_text(row.get("directions", row.get("RecipeInstructions", ""))),
                            "calories": self._extract_numeric(row, ["calories", "Calories"]),
                            "fat": self._extract_numeric(row, ["fat", "FatContent"]),
                            "protein": self._extract_numeric(row, ["protein", "ProteinContent"]),
                            "sodium": self._extract_numeric(row, ["sodium", "SodiumContent"]),
                            "price_range": None,  # Will be calculated later if needed
                            "ingredients": self._extract_ingredients(row)
                        }
                        
                        # If no ingredients were found, skip this recipe
                        if not recipe["ingredients"]:
                            continue
                            
                        recipes.append(recipe)
                    except Exception as e:
                        errors.append(f"Error processing recipe {idx}: {str(e)}")
                
                # Execute the batch
                try:
                    if recipes:  # Only run if we have valid recipes
                        session.run(query, recipes=recipes)
                        total_processed += len(recipes)
                except Exception as e:
                    errors.append(f"Error in batch {batch_idx}: {str(e)}")
        
        return {
            "status": "success" if not errors else "partial_success" if total_processed > 0 else "error",
            "total_processed": total_processed,
            "total_records": len(data),
            "errors": errors[:10] if len(errors) > 10 else errors  # Limit error output
        }
    
    def _extract_numeric(self, row: pd.Series, possible_columns: List[str]) -> Optional[float]:
        """
        Extract numeric value from one of several possible columns.
        
        Args:
            row: DataFrame row
            possible_columns: List of possible column names
            
        Returns:
            Float value or None
        """
        for col in possible_columns:
            if col in row and pd.notna(row[col]):
                try:
                    return float(row[col])
                except (ValueError, TypeError):
                    pass
        return None
    
    def _extract_ingredients(self, row: pd.Series) -> List[str]:
        """
        Extract ingredients from different possible formats.
        
        Args:
            row: DataFrame row
            
        Returns:
            List of ingredient strings
        """
        ingredients = []
        
        # Try different possible ingredient columns
        if "ingredients" in row and not pd.isna(row["ingredients"]).all():
            ingredients_data = row["ingredients"]
            if isinstance(ingredients_data, list):
                for ingr in ingredients_data:
                    if isinstance(ingr, str) and ingr.strip():
                        ingredients.append(self.clean_text(ingr))
            elif isinstance(ingredients_data, str) and ingredients_data.strip():
                ingredients.append(self.clean_text(ingredients_data))
                
        elif "RecipeIngredientParts" in row and not pd.isna(row["RecipeIngredientParts"]).all():
            ingredients_data = row["RecipeIngredientParts"]
            if isinstance(ingredients_data, list):
                for ingr in ingredients_data:
                    if isinstance(ingr, str) and ingr.strip():
                        ingredients.append(self.clean_text(ingr))
            elif isinstance(ingredients_data, str) and ingredients_data.strip():
                ingredients.append(self.clean_text(ingredients_data))
        
        # If no ingredients were found, add a placeholder
        if not ingredients:
            ingredients = ["Unknown ingredient"]
            
        return ingredients