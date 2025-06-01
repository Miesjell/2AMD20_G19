"""
Recipe loader for the knowledge graph.

This module provides functionality for loading recipe data into the Neo4j database.
It handles different recipe data formats and extracts ingredients, nutritional 
information, and other relevant properties.
"""
from pathlib import Path
from typing import Dict, Any, List, Optional
from src.utils.ingredient_parser import split_ingredients, parse_ingredient, parse_ingredient_only_ingredient
from src.utils.ingredient_embedder import IngredientNormalizer

# alter threshold
normalizer = IngredientNormalizer(threshold=0.75)

import pandas as pd
import numpy as np
from neo4j import Driver
from tqdm import tqdm

from .base import DataLoader


class RecipeLoader(DataLoader):
    """
    Loader for recipes and their ingredient relationships.
    
    This class is responsible for loading recipes from different data sources
    and creating the appropriate nodes and relationships in the Neo4j database.
    """
    
    def __init__(self, driver: Optional[Driver] = None):
        """Initialize the recipe loader with an optional Neo4j driver."""
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
            Dict with load results including status and statistics
        """
        if not self.driver:
            return {"status": "error", "error": "Neo4j driver not set. Call set_driver() first."}
        
        # Sample if needed to avoid processing too much data at once
        if sample_size and len(data) > sample_size:
            data = data.sample(sample_size, random_state=42)
        
        # Create batches for efficient loading
        batches = self.batch_data(data, batch_size)
        
        # Cypher query for batch loading recipes with proper list filtering
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
        
        # Initialize counters and result lists
        total_processed = 0
        errors = []
        recipes_with_ingredients = 0
        recipes_without_ingredients = 0
        
        with self.driver.session() as session:
            # Process each batch with progress bar
            for batch_idx, batch in enumerate(tqdm(batches, 
                                               desc=f"Loading recipes from {source_name}", 
                                               unit="batch")):
                # Prepare batch data
                batch_results = self._prepare_batch(batch, source_name)
                recipes = batch_results["recipes"]
                recipes_with_ingredients += batch_results["with_ingredients"]
                recipes_without_ingredients += batch_results["without_ingredients"]
                errors.extend(batch_results["errors"])
                
                # Execute the batch
                try:
                    if recipes:  # Only run if we have valid recipes
                        session.run(query, recipes=recipes)
                        total_processed += len(recipes)
                except Exception as e:
                    error_msg = f"Error in batch {batch_idx}: {str(e)}"
                    errors.append(error_msg)
        
        # Prepare result dictionary
        return {
            "status": "success" if not errors else "partial_success" if total_processed > 0 else "error",
            "total_processed": total_processed,
            "total_records": len(data),
            "recipes_with_ingredients": recipes_with_ingredients,
            "recipes_without_ingredients": recipes_without_ingredients,
            "errors": errors[:10] if len(errors) > 10 else errors  # Limit error output
        }
    
    def _prepare_batch(self, batch: pd.DataFrame, source_name: str) -> Dict[str, Any]:
        """
        Prepare a batch of recipes for database insertion.
        
        Args:
            batch: DataFrame batch containing recipe data
            source_name: Name of the data source
            
        Returns:
            Dictionary containing prepared recipes and statistics
        """
        recipes = []
        errors = []
        with_ingredients = 0
        without_ingredients = 0
        
        for idx, row in batch.iterrows():
            try:
                # Convert row to dictionary for consistent handling
                row_dict = row.to_dict() if isinstance(row, pd.Series) else row
                
                # Extract ingredients
                extracted_ingredients = []
                try:
                    extracted_ingredients = self._extract_ingredients(row)
                except Exception as ingr_error:
                    # Continue with unknown ingredient
                    extracted_ingredients = ["Unknown ingredient"]
                
                # Create recipe dictionary with standardized properties
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
                
                # Track ingredient statistics
                if len(recipe["ingredients"]) <= 1 and recipe["ingredients"][0] == "Unknown ingredient":
                    without_ingredients += 1
                else:
                    with_ingredients += 1
                    
                recipes.append(recipe)
            except Exception as e:
                error_msg = f"Error processing recipe {idx}: {str(e)}"
                errors.append(error_msg)
        
        # Return a dictionary with all results
        return {
            "recipes": recipes,
            "with_ingredients": with_ingredients,
            "without_ingredients": without_ingredients,
            "errors": errors
        }
    
    def _extract_numeric(self, row: pd.Series, possible_columns: List[str]) -> Optional[float]:
        """
        Extract numeric value from one of several possible columns.
        
        Args:
            row: DataFrame row
            possible_columns: List of possible column names
            
        Returns:
            Float value or None if not found
        """
        for col in possible_columns:
            if col in row and pd.notna(row[col]):
                try:
                    return float(row[col])
                except (ValueError, TypeError):
                    pass
        return None
    
    # def _extract_ingredients(self, row: pd.Series) -> List[str]:
    #     """
    #     Extract ingredients from different possible formats.
        
    #     Args:
    #         row: DataFrame row
            
    #     Returns:
    #         List of ingredient strings
    #     """
    #     ingredients = []
        
    #     # Get the recipe name for better error logging
    #     recipe_name = row.get("title", row.get("Name", "Unknown Recipe"))
    #     if isinstance(recipe_name, np.ndarray) and recipe_name.size > 0:
    #         recipe_name = recipe_name[0]
    #     recipe_name = str(recipe_name)
        
    #     # Try RecipeIngredientParts first (most common in this dataset)
    #     if "RecipeIngredientParts" in row:
    #         ingredients = self._extract_from_field(row["RecipeIngredientParts"])
                
    #     # If no ingredients found, try standard 'ingredients' column
    #     if not ingredients and "ingredients" in row:
    #         ingredients = self._extract_from_field(row["ingredients"])
        
    #     # If still no ingredients, try any column that might contain ingredients
    #     if not ingredients:
    #         for col in row.index:
    #             if 'ingredient' in str(col).lower() and col not in ["RecipeIngredientParts", "ingredients"]:
    #                 col_val = row[col]
    #                 # Skip null values
    #                 if pd.isna(col_val).all() if hasattr(pd.isna(col_val), 'all') else pd.isna(col_val):
    #                     continue
    #                 ingredients = self._extract_from_field(col_val)
    #                 if ingredients:
    #                     break
        
    #     # If still no ingredients found, use a placeholder
    #     if not ingredients:
    #         ingredients = ["Unknown ingredient"]
        
    #     # Ingredient parser
    #     parsed = []
    #     for item in ingredients:
    #         # Smart split if needed
    #         parts = split_ingredients(item) if ',' in item else [item]
    #         for part in parts:
    #             #amt, unit, name = parse_ingredient(part)
    #             #parsed.append(f"{amt} {unit} {name}".strip()) 
    #             name = parse_ingredient_only_ingredient(part)
    #             parsed.append(name)   

    #     return parsed or ["Unknown ingredient"]
    def _extract_ingredients(self, row: pd.Series) -> List[str]:
        """
        Extract ingredients from different possible formats.
        
        If the 'ingredients' column is present, it applies the ingredient parser.
        Otherwise, it uses fallback fields without parsing.

        Args:
            row: DataFrame row

        Returns:
            List of cleaned ingredient names
        """
        # Handle bad or null recipe names gracefully
        recipe_name = row.get("title", row.get("Name", "Unknown Recipe"))
        if isinstance(recipe_name, np.ndarray) and recipe_name.size > 0:
            recipe_name = recipe_name[0]
        recipe_name = str(recipe_name)

        # === Case 1: 'ingredients' column — apply parser; old ===

        # if "ingredients" in row and isinstance(row["ingredients"], (str, list)):
        #     raw_ingredients = self._extract_from_field(row["ingredients"])
        #     parsed = []
        #     for item in raw_ingredients:
        #         parts = split_ingredients(item) if ',' in item else [item]
        #         for part in parts:
        #             name = parse_ingredient(part)
        #             parsed.append(name)
        #     return parsed or ["Unknown ingredient"]

        # === Case 1: 'ingredients' column — apply parser; including embedding, i.e. new =====
        if "ingredients" in row and isinstance(row["ingredients"], (str, list)):
            raw_ingredients = self._extract_from_field(row["ingredients"])
            parsed = []
            for item in raw_ingredients:
                parts = split_ingredients(item) if ',' in item else [item]
                for part in parts:
                    _, _, name = parse_ingredient(part)
                    canonical = normalizer.normalize(name)
                    parsed.append(canonical)
            return parsed or ["Unknown ingredient"]        

        # === Case 2: Try RecipeIngredientParts or other fallback fields ===
        ingredients = []

        if "RecipeIngredientParts" in row:
            ingredients = self._extract_from_field(row["RecipeIngredientParts"])

        # Fallback: any column with 'ingredient' in its name
        if not ingredients:
            for col in row.index:
                if "ingredient" in str(col).lower() and col not in ["RecipeIngredientParts", "ingredients"]:
                    col_val = row[col]
                    if pd.isna(col_val).all() if hasattr(pd.isna(col_val), 'all') else pd.isna(col_val):
                        continue
                    ingredients = self._extract_from_field(col_val)
                    if ingredients:
                        break

        return ingredients or ["Unknown ingredient"]


    def _extract_from_field(self, field_value: Any) -> List[str]:
        """
        Extract ingredients from a field value of various types.
        
        Args:
            field_value: The value to extract ingredients from
            
        Returns:
            List of ingredient strings
        """
        ingredients = []
        
        # Handle NumPy arrays 
        if isinstance(field_value, np.ndarray):
            if field_value.size > 0:
                for item in field_value:
                    if isinstance(item, str) and item.strip():
                        ingredients.append(self.clean_text(item))
                        
        # Handle regular Python lists
        elif isinstance(field_value, list):
            for item in field_value:
                if isinstance(item, str) and item.strip():
                    ingredients.append(self.clean_text(item))
                    
        # Handle string values
        elif isinstance(field_value, str) and field_value.strip():
            ingredients.append(self.clean_text(field_value))
            
        return ingredients


#= == test
def load_full_format_recipes(data_dir: str = "data"):
    path = Path(data_dir) / "full_format_recipes.json"
    df = pd.read_json(path)
    return df

if __name__ == "__main__":
    import pandas as pd

    # Load the dataset
    df = load_full_format_recipes()

    # Select a specific row by title
    row = df.iloc[0]
    # Create a RecipeLoader instance (no DB needed)
    loader = RecipeLoader()

    # Run the ingredient extractor
    ingredients = loader._extract_ingredients(row)

    # Print results
    print(f"\nParsed ingredients for: {row['title']}")
    for ing in ingredients:
        print("-", ing)