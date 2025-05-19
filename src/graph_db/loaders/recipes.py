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
        
        # Cypher query for batch loading recipes - proper list filtering
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
        UNWIND [ingredient IN recipe.ingredients WHERE ingredient IS NOT NULL AND ingredient <> ''] AS ingredient
        MERGE (i:Ingredient {name: ingredient})
        MERGE (r)-[:CONTAINS]->(i)
        
        WITH r, recipe
        WHERE recipe.price_range IS NOT NULL
        SET r.price_range = recipe.price_range
        """
        
        total_processed = 0
        errors = []
        recipes_with_ingredients = 0
        recipes_without_ingredients = 0
        
        with self.driver.session() as session:
            for batch_idx, batch in enumerate(batches):
                # Prepare data for this batch
                recipes = []
                batch_recipes_with_ingredients = 0
                batch_recipes_without_ingredients = 0
                
                for idx, row in batch.iterrows():
                    try:
                        # If row is Series, convert to dict to make handling more consistent
                        row_dict = row.to_dict() if isinstance(row, pd.Series) else row
                        
                        # Extract ingredients first since this is the most problematic part
                        extracted_ingredients = []
                        try:
                            extracted_ingredients = self._extract_ingredients(row)
                        except Exception as ingr_error:
                            # Continue with unknown ingredient
                            extracted_ingredients = ["Unknown ingredient"]
                        
                        # Handle different column names in different dataframes
                        recipe = {
                            "id": f"{source_name}_{idx}",
                            "source": source_name,
                            "name": self.clean_text(row_dict.get("title", row_dict.get("Name", f"Recipe-{idx}"))),
                            "description": self.clean_text(row_dict.get("desc", row_dict.get("Description", ""))),
                            "preparation": self.clean_text(row_dict.get("directions", row_dict.get("RecipeInstructions", ""))),
                            "calories": self._extract_numeric(row, ["calories", "Calories"]),
                            "fat": self._extract_numeric(row, ["fat", "FatContent"]),
                            "protein": self._extract_numeric(row, ["protein", "ProteinContent"]),
                            "sodium": self._extract_numeric(row, ["sodium", "SodiumContent"]),
                            "price_range": None,  # Will be calculated later if needed
                            "ingredients": extracted_ingredients
                        }
                        
                        # Track ingredients stats
                        if len(recipe["ingredients"]) <= 1 and recipe["ingredients"][0] == "Unknown ingredient":
                            batch_recipes_without_ingredients += 1
                        else:
                            batch_recipes_with_ingredients += 1
                            
                        recipes.append(recipe)
                    except Exception as e:
                        error_msg = f"Error processing recipe {idx}: {str(e)}"
                        errors.append(error_msg)
                
                recipes_with_ingredients += batch_recipes_with_ingredients
                recipes_without_ingredients += batch_recipes_without_ingredients
                
                # Execute the batch
                try:
                    if recipes:  # Only run if we have valid recipes
                        session.run(query, recipes=recipes)
                        total_processed += len(recipes)
                except Exception as e:
                    error_msg = f"Error in batch {batch_idx}: {str(e)}"
                    errors.append(error_msg)
        
        return {
            "status": "success" if not errors else "partial_success" if total_processed > 0 else "error",
            "total_processed": total_processed,
            "total_records": len(data),
            "recipes_with_ingredients": recipes_with_ingredients,
            "recipes_without_ingredients": recipes_without_ingredients,
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
        
        # Get the recipe name for better error logging
        recipe_name = row.get("title", row.get("Name", "Unknown Recipe"))
        if isinstance(recipe_name, np.ndarray) and recipe_name.size > 0:
            recipe_name = recipe_name[0]
        recipe_name = str(recipe_name)
        
        # Try RecipeIngredientParts first (most common in this dataset)
        if "RecipeIngredientParts" in row:
            try:
                parts = row["RecipeIngredientParts"]
                # Handle NumPy arrays properly
                if isinstance(parts, np.ndarray):
                    if parts.size > 0:
                        for item in parts:
                            if isinstance(item, str) and item.strip():
                                ingredients.append(self.clean_text(item))
                # Handle regular Python lists
                elif isinstance(parts, list) and parts:
                    for item in parts:
                        if isinstance(item, str) and item.strip():
                            ingredients.append(self.clean_text(item))
                # Handle string values
                elif isinstance(parts, str) and parts.strip():
                    ingredients.append(self.clean_text(parts))
            except Exception as e:
                pass
        
        # If no ingredients found, try standard 'ingredients' column
        if not ingredients and "ingredients" in row:
            try:
                ingr = row["ingredients"]
                # Handle NumPy arrays properly
                if isinstance(ingr, np.ndarray):
                    if ingr.size > 0:
                        for item in ingr:
                            if isinstance(item, str) and item.strip():
                                ingredients.append(self.clean_text(item))
                # Handle regular Python lists
                elif isinstance(ingr, list) and ingr:
                    for item in ingr:
                        if isinstance(item, str) and item.strip():
                            ingredients.append(self.clean_text(item))
                # Handle string values
                elif isinstance(ingr, str) and ingr.strip():
                    ingredients.append(self.clean_text(ingr))
            except Exception as e:
                pass
        
        # If still no ingredients, try any column that might contain ingredients
        if not ingredients:
            try:
                for col in row.index:
                    if 'ingredient' in str(col).lower() and col not in ["RecipeIngredientParts", "ingredients"]:
                        try:
                            col_val = row[col]
                            # Skip null values
                            if pd.isna(col_val).all() if hasattr(pd.isna(col_val), 'all') else pd.isna(col_val):
                                continue
                                
                            # Handle NumPy arrays
                            if isinstance(col_val, np.ndarray):
                                if col_val.size > 0:
                                    for item in col_val:
                                        if isinstance(item, str) and item.strip():
                                            ingredients.append(self.clean_text(item))
                            # Handle lists
                            elif isinstance(col_val, list) and col_val:
                                for item in col_val:
                                    if isinstance(item, str) and item.strip():
                                        ingredients.append(self.clean_text(item))
                            # Handle strings
                            elif isinstance(col_val, str) and col_val.strip():
                                ingredients.append(self.clean_text(col_val))
                        except Exception as col_err:
                            continue
            except Exception as e:
                pass
        
        # If still no ingredients found, use a placeholder
        if not ingredients:
            ingredients = ["Unknown ingredient"]
            
        return ingredients