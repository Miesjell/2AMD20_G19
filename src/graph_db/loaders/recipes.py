"""
Recipe loader for the knowledge graph.

This module provides functionality for loading recipe data into the Neo4j database.
It handles different recipe data formats and extracts ingredients, nutritional 
information, and other relevant properties.
"""
from typing import Dict, Any, Optional, List
import pandas as pd
import numpy as np
import json
import logging
from neo4j import Driver
from tqdm import tqdm
from .base import DataLoader

class RecipeLoader(DataLoader):
    """Loader for recipe data into the Neo4j knowledge graph."""

    def __init__(self, driver: Optional[Driver] = None):
        super().__init__(driver)
        self.logger = logging.getLogger(__name__)

    def load_data(self, data: pd.DataFrame, source_name: str,
                  sample_size: Optional[int] = None,
                  batch_size: int = 25) -> Dict[str, Any]:
        """
        Load recipe data into the Neo4j knowledge graph.
        
        This method handles data cleaning, extraction of ingredients, and batch processing
        to efficiently create Recipe nodes and their relationships in the database.
        
        Args:
            data: DataFrame with recipe data
            source_name: Name of the data source (for tracking)
            sample_size: If provided, only load this many recipes (random sample)
            batch_size: Number of recipes to process in each batch
            
        Returns:
            Dictionary with loading results
        """
        if not self.driver:
            return {"status": "error", "error": "Neo4j driver not set. Call set_driver() first."}

        if sample_size and len(data) > sample_size:
            data = data.sample(sample_size, random_state=42)
        try:
            df = (
                self._prepare_basic_info(data, source_name)
                    .pipe(self._clean_text_fields)
                    .pipe(self._extract_preparation)
                    .pipe(self._extract_nutrition)
                    .pipe(self._assign_meal_type)
                    .pipe(self._extract_recipe_ingredients)
            )
            

            # Calculate statistics (don't log debug output)
            ingredient_counts = df["ingredients"].apply(len)
            recipes_with_ingredients = df["ingredients"].apply(lambda x: len(x) > 0 and (len(x) > 1 or x[0] != "Unknown ingredient")).sum()
            recipes_without_ingredients = len(df) - recipes_with_ingredients
            # Create batches for processing
            batches = self.batch_data(df, batch_size)
            # Create constraints and indexes if needed
            setup_query = self._setup_constraints()
            # Query for creating recipes and ingredients
            query = """
            UNWIND $recipes AS recipe
            MERGE (r:Recipe {id: recipe.id})
            SET r.name = recipe.name,
                r.source = recipe.source,
                r.description = recipe.description,
                r.calories = CASE WHEN recipe.calories IS NULL THEN 0 ELSE recipe.calories END,
                r.fat = CASE WHEN recipe.fat IS NULL THEN 0 ELSE recipe.fat END,
                r.protein = CASE WHEN recipe.protein IS NULL THEN 0 ELSE recipe.protein END,
                r.sodium = CASE WHEN recipe.sodium IS NULL THEN 0 ELSE recipe.sodium END,
                r.preparation_description = recipe.preparation,
                r.price_range = recipe.price_range

            WITH r, recipe
            WHERE recipe.meal_type IS NOT NULL
            MERGE (mt:MealType {name: recipe.meal_type})
            MERGE (r)-[:IS_TYPE]->(mt)

            WITH r, recipe
            UNWIND recipe.ingredients AS ingredient
            WITH r, ingredient
            WHERE ingredient IS NOT NULL AND ingredient <> '' AND ingredient <> 'Unknown ingredient'
            MERGE (i:Ingredient {name: ingredient})
            MERGE (r)-[:CONTAINS]->(i)
            """

            total_processed = 0
            errors = []

            # Process batches
            with self.driver.session() as session:
                # First run setup query to ensure constraints and indexes
                try:
                    for stmt in self._setup_constraints():
                        session.run(stmt)
                    self.logger.info("Created necessary constraints and indexes")
                except Exception as e:
                    self.logger.warning(f"Could not create constraints: {str(e)}")
                
                # Process each batch
                for batch_idx, batch in enumerate(tqdm(batches, desc=f"Loading recipes from {source_name}", unit="batch")):
                    try:
                        # Extract relevant columns
                        records = []
                        for _, row in batch.iterrows():
                            try:
                                record = {
                                    "id": row["id"],
                                    "name": row["name"],
                                    "source": row["source"],
                                    "description": row["description"],
                                    "preparation": row["preparation"],
                                    "calories": float(row["calories"]) if pd.notna(row["calories"]) else None,
                                    "fat": float(row["fat"]) if pd.notna(row["fat"]) else None,
                                    "protein": float(row["protein"]) if pd.notna(row["protein"]) else None,
                                    "sodium": float(row["sodium"]) if pd.notna(row["sodium"]) else None,
                                    "price_range": row["price_range"],
                                    "meal_type": row["meal_type"],
                                    "ingredients": [ing for ing in row["ingredients"] if ing and ing != "Unknown ingredient"]
                                }
                                records.append(record)
                            except Exception as e:
                                self.logger.debug(f"Error processing row: {str(e)}")
                        
                        if records:
                            # Run the query with the batch of records
                            result = session.run(query, recipes=records)
                            result.consume()  # Ensure query execution completes
                            total_processed += len(records)
                            self.logger.debug(f"Batch {batch_idx}: Added {len(records)} recipes")
                        else:
                            self.logger.debug(f"Batch {batch_idx}: No valid records to process")
                            
                    except Exception as e:
                        self.logger.error(f"Error in batch {batch_idx}: {str(e)}")
                        errors.append(f"Error in batch {batch_idx}: {str(e)}")

            self.logger.info(f"Total recipes processed: {total_processed} out of {len(df)}")
            
            # Return detailed status information
            return {
                "status": "success" if not errors else "partial_success" if total_processed > 0 else "error",
                "total_processed": total_processed,
                "total_records": len(df),
                "recipes_with_ingredients": recipes_with_ingredients,
                "recipes_without_ingredients": recipes_without_ingredients,
                "data": df[['ingredients']],
                "errors": errors[:10] if errors else [],
                "df": df
            }
        except Exception as e:
            self.logger.error(f"Critical error in recipe loading: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "total_processed": 0,
                "total_records": len(data)
            }

    def _prepare_basic_info(self, data: pd.DataFrame, source_name: str) -> pd.DataFrame:
        df = data.copy().dropna(how='all')
        df.columns = df.columns.str.lower().str.strip()
        df["id"] = [f"{source_name}_{i}" for i in df.index]
        df["source"] = source_name
        if "price_range" not in df.columns:
            df["price_range"] = None
        return df

    def _clean_text_fields(self, df: pd.DataFrame) -> pd.DataFrame: 
        df["name"] = df.get("title", df.get("Name", pd.Series([f"Recipe-{i}" for i in df.index])))
        df["name"] = df["name"].apply(lambda x: self.clean_text(str(x)) if pd.notna(x) else f"Recipe-{np.random.randint(10000)}")
        df["description"] = df.get("desc", "")
        df["description"] = df["description"].apply(
            lambda x: self.clean_text(str(x)) if pd.notna(x) else ""
        ) 
        return df

    def _extract_preparation(self, df: pd.DataFrame) -> pd.DataFrame:
        column = None
        # should always be list form
        for candidate in ["recipeinstructions", "directions"]:
            if candidate in df.columns:
                column = candidate
                break

        if column:
            df["preparation"] = df[column].apply(
                lambda x: " ".join(self.clean_text(str(item)) for item in x)
                if isinstance(x, (list, np.ndarray)) else ""
            )
        else:
            df["preparation"] = ""
        return df

    def _extract_nutrition(self, df: pd.DataFrame) -> pd.DataFrame:
        attributes = ["calories", "fat", "protein", "sodium"]
        for attr in attributes:
            # Find first matching column (can sum different types of fats later)
            matching_col = next((col for col in df.columns if col.startswith(attr)), None)
            df[attr] = pd.to_numeric(df[matching_col], errors="coerce")
        return df

    def _setup_constraints(self) -> List[str]:
        return [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (r:Recipe) REQUIRE r.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (i:Ingredient) REQUIRE i.name IS UNIQUE",
            "CREATE INDEX IF NOT EXISTS FOR (r:Recipe) ON (r.name)",
            "CREATE INDEX IF NOT EXISTS FOR (i:Ingredient) ON (i.name)",
        ]
    
    
    def _assign_meal_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Assign a meal type to each recipe based on keywords in the name.
        Categories: Breakfast, Lunch, Dinner, Drink, Other
        """
        def categorize(name: str) -> str:
            name = name.lower()
            if any(k in name for k in ["breakfast", "pancake", "waffle", "cereal", "oatmeal", "omelet", "omelette", "bacon", "toast"]):
                return "Breakfast"
            if any(k in name for k in ["lunch", "sandwich", "salad", "wrap", "soup"]):
                return "Lunch"
            if any(k in name for k in ["dinner", "roast", "steak", "pasta", "casserole"]):
                return "Dinner"
            if any(k in name for k in ["cocktail", "drink", "smoothie", "juice", "coffee", "tea", "wine", "beer", "liquor", "whiskey", "vodka"]):
                return "Drink"
            return "Other"

        df["meal_type"] = df["name"].apply(lambda x: categorize(str(x)))
        return df
    
    
    
    def _extract_recipe_ingredients(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Pipeable function that extracts ingredients from each recipe row 
        and adds an 'ingredients' column to the DataFrame.

        Args:
            df: DataFrame with recipe data.

        Returns:
            DataFrame with a new 'ingredients' column.
        """
        def extract(row: pd.Series) -> List[str]:
            ingredients = []

            # Try RecipeIngredientParts
            if "recipeingredientparts" in row and isinstance(row["recipeingredientparts"], np.ndarray):
                try:
                    ingredients += [
                        self.clean_text(item)
                        for item in row["recipeingredientparts"]
                        if isinstance(item, str) and item.strip()
                    ]
                except Exception as e:
                    self.logger.debug(f"Error processing RecipeIngredientParts: {e}")

            # Fallback to 'ingredients' field
            if not ingredients and "ingredients" in row and isinstance(row["ingredients"], list):
                try:
                    for item in row["ingredients"]:
                        if isinstance(item, str) and item.strip():
                            ingredients.append(self.clean_text(item))
                        elif isinstance(item, (int, float)):
                            ingredients.append(self.clean_text(str(item)))
                except Exception as e:
                    self.logger.debug(f"Error processing ingredients field: {e}")

            return ingredients or ["Unknown ingredient"]

        df["ingredients"] = df.apply(extract, axis=1)
        return df
    