import re
import pandas as pd
import logging
from typing import Dict, Any, Optional
from neo4j import Driver
from .base import DataLoader


class PriceLoader(DataLoader):
    def __init__(self, driver: Optional[Driver] = None):
        super().__init__(driver)
        self.logger = logging.getLogger(__name__)

        # Precompile regex patterns for speed
        self.quantity_unit_pattern = re.compile(
            r"^[\d\s/.,()\-]+(?:cups?|tablespoons?|tbsp|teaspoons?|tsp|pounds?|lbs?|ounces?|oz|grams?|g|ml|liters?|l|inch|inches)?\b"
        )
        self.descriptor_pattern = re.compile(
            r"\b(chopped|minced|sliced|diced|crushed|hulled|peeled|ground|shredded|mashed|fresh|frozen|cooked|raw|boiled|grated|"
            r"coarsely|finely|optional|packed|stemmed|trimmed|beaten|halved|pitted|cut|into|removed|loosely|torn|quartered|ribs|"
            r"leaves|stems|wedges|slices|strips|whole|large|small|medium|dry|dried|sweetened|unsalted|low-salt|no-salt-added|"
            r"store-bought|preferably|coarsely chopped)\b"
        )
        self.number_pattern = re.compile(r"\b\d+([\/.-]\d+)?\b|\b\d+\w*\b")
        self.punct_pattern = re.compile(r"[^\w\s]")
        self.space_pattern = re.compile(r"\s+")

    def extract_core_ingredient(self, text: str) -> Optional[str]:
        """Clean and normalize ingredient text to extract the core term."""
        text = text.lower().strip()
        text = self.quantity_unit_pattern.sub("", text)
        text = self.descriptor_pattern.sub("", text)
        text = self.number_pattern.sub("", text)
        text = self.punct_pattern.sub("", text)
        text = self.space_pattern.sub(" ", text).strip()
        tokens = text.split()
        return " ".join(tokens[-2:]) if len(tokens) >= 2 else (tokens[-1] if tokens else None)

    def load_data(self, df: pd.DataFrame, batch_size: int = 100) -> Dict[str, Any]:
        if not self.driver:
            return {"status": "error", "error": "Neo4j driver not set."}

        # Clean column names
        df.columns = df.columns.str.lower().str.strip()

        if "product_name" not in df.columns or "price_current" not in df.columns:
            return {"status": "error", "error": "Missing required 'product_name' or 'price_current' column."}

        df["product_name"] = df["product_name"].astype(str).str.lower().str.strip()

        # Step 1: Get known ingredients from Neo4j
        with self.driver.session() as session:
            result = session.run("MATCH (i:Ingredient) RETURN i.name AS name")
            raw_ingredient_names = [record["name"] for record in result if record["name"]]

        # Step 2: Process known ingredients and build reverse mapping
        processed_to_original = {}
        for name in raw_ingredient_names:
            processed = self.extract_core_ingredient(name)
            if processed:
                processed_to_original[processed] = name

        processed_ingredient_names = set(processed_to_original.keys())
        self.logger.info(f"Processed {len(processed_ingredient_names)} ingredients from Neo4j.")

        # Step 3: Extract matched ingredient from product names using regex
        pattern = r"\b(" + "|".join(re.escape(ingr) for ingr in processed_ingredient_names) + r")\b"
        df["matched_ingredient"] = df["product_name"].str.extract(pattern, expand=False)

        # Step 4: Map to original Neo4j node name
        df["original_ingredient"] = df["matched_ingredient"].map(processed_to_original)
        filtered_df = df[df["original_ingredient"].notnull()].copy()

        if filtered_df.empty:
            self.logger.warning("No matching ingredients found in Neo4j.")
            return {"status": "success", "loaded": 0, "filtered_out": len(df)}

        # Step 5: Batch insert prices using UNWIND
        batch_data = filtered_df[["original_ingredient", "product_name", "price_current"]].to_dict("records")

        with self.driver.session() as session:
            session.run(
                """
                UNWIND $batch AS row
                MATCH (i:Ingredient {name: row.original_ingredient})
                MERGE (i)-[:HAS_PRICE]->(:Price {
                    source: row.product_name,
                    price: row.price_current
                })
                """,
                batch=batch_data
            )

        return {
            "status": "success",
            "loaded": len(batch_data),
            "filtered_out": len(df) - len(batch_data)
        }
