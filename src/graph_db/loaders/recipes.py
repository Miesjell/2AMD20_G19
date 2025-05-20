from typing import Dict, Any, Optional, List
import pandas as pd
import numpy as np
from neo4j import Driver
from tqdm import tqdm
from .base import DataLoader


class RecipeLoader(DataLoader):
    """Loader for recipe data into the Neo4j knowledge graph."""

    def __init__(self, driver: Optional[Driver] = None):
        super().__init__(driver)

    def load_data(self, data: pd.DataFrame, source_name: str,
                  sample_size: Optional[int] = None,
                  batch_size: int = 25) -> Dict[str, Any]:
        if not self.driver:
            return {"status": "error", "error": "Neo4j driver not set. Call set_driver() first."}

        if sample_size and len(data) > sample_size:
            data = data.sample(sample_size, random_state=42)

        df = data.copy()
        df["id"] = [f"{source_name}_{i}" for i in df.index]
        df["source"] = source_name

        df["name"] = df.get("title", df.get("Name", pd.Series([f"Recipe-{i}" for i in df.index]))).map(self.clean_text)
        df["description"] = df.get("desc", df.get("Description", "")).map(self.clean_text)
        df["preparation"] = df.get("directions", df.get("RecipeInstructions", "")).map(self.clean_text)

        for col, new in [("calories", "calories"), ("fat", "fat"), ("protein", "protein"), ("sodium", "sodium")]:
            df[new] = pd.to_numeric(df[col], errors="coerce") if col in df else None

        df["ingredients"] = df.apply(self._vectorized_extract_ingredients, axis=1)
        df["price_range"] = None

        recipes_with_ingredients = df["ingredients"].apply(lambda x: len(x) > 1 or x[0] != "Unknown ingredient").sum()
        recipes_without_ingredients = len(df) - recipes_with_ingredients

        batches = self.batch_data(df, batch_size)

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

        with self.driver.session() as session:
            for batch_idx, batch in enumerate(tqdm(batches, desc=f"Loading recipes from {source_name}", unit="batch")):
                try:
                    records = batch[["id", "name", "source", "description", "preparation",
                                     "calories", "fat", "protein", "sodium", "price_range", "ingredients"]]
                    session.run(query, recipes=records.to_dict(orient="records"))
                    total_processed += len(batch)
                except Exception as e:
                    errors.append(f"Error in batch {batch_idx}: {str(e)}")

        return {
            "status": "success" if not errors else "partial_success" if total_processed > 0 else "error",
            "total_processed": total_processed,
            "total_records": len(df),
            "recipes_with_ingredients": recipes_with_ingredients,
            "recipes_without_ingredients": recipes_without_ingredients,
            "errors": errors[:10]
        }

    def _vectorized_extract_ingredients(self, row: pd.Series) -> List[str]:
        for key in ["RecipeIngredientParts", "ingredients"]:
            if key in row and pd.notna(row[key]):
                return self._extract_from_field(row[key])

        for col in row.index:
            if "ingredient" in str(col).lower():
                return self._extract_from_field(row[col])

        return ["Unknown ingredient"]

    def _extract_from_field(self, field_value: Any) -> List[str]:
        ingredients = []

        if isinstance(field_value, np.ndarray):
            for item in field_value:
                if isinstance(item, str) and item.strip():
                    ingredients.append(self.clean_text(item))

        elif isinstance(field_value, list):
            for item in field_value:
                if isinstance(item, str) and item.strip():
                    ingredients.append(self.clean_text(item))

        elif isinstance(field_value, str) and field_value.strip():
            ingredients.append(self.clean_text(field_value))

        return ingredients
