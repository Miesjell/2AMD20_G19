import re
import pandas as pd
import logging
from tqdm import tqdm
from typing import Dict, Any, Optional
from neo4j import Driver
from .base import DataLoader


class PriceLoader(DataLoader):
    def __init__(self, driver: Optional[Driver] = None):
        super().__init__(driver)
        self.logger = logging.getLogger(__name__)

    def extract_core_ingredient(self, text: str) -> Optional[str]:
        """
        Extract the most likely core ingredient term(s) from a product description string.
        Returns a cleaned string that may match a known ingredient node.
        """
        text = text.lower().strip()

        # Remove quantities and units
        text = re.sub(
            r"^[\d\s/.,()\-]+(?:cups?|tablespoons?|tbsp|teaspoons?|tsp|pounds?|lbs?|ounces?|oz|grams?|g|ml|liters?|l|inch|inches)?\b", 
            "", 
            text
        )

        # Remove preparation/prefix descriptors
        text = re.sub(
            r"\b(chopped|minced|sliced|diced|crushed|hulled|peeled|ground|shredded|mashed|fresh|frozen|cooked|raw|boiled|grated|coarsely|finely|optional|packed|stemmed|trimmed|beaten|halved|pitted|cut|into|removed|loosely|torn|quartered|ribs|leaves|stems|wedges|slices|strips|whole|large|small|medium|dry|dried|sweetened|unsalted|low-salt|no-salt-added|store-bought|preferably|coarsely chopped)\b",
            "", 
            text
        )

        # Remove punctuation
        text = re.sub(r"[^\w\s]", "", text)

        # Get candidate tokens
        tokens = text.strip().split()
        if not tokens:
            return None

        # Heuristic: last two tokens form best guess
        return " ".join(tokens[-2:]) if len(tokens) >= 2 else tokens[-1]

    def load_data(self, df: pd.DataFrame, batch_size: int = 100) -> Dict[str, Any]:
        if not self.driver:
            return {"status": "error", "error": "Neo4j driver not set."}

        # Clean column names
        df.columns = df.columns.str.lower().str.strip()

        if "product_name" not in df.columns:
            return {"status": "error", "error": "Missing required 'product_name' column."}

        df["product_name"] = df["product_name"].astype(str).str.lower().str.strip()

        # Step 1: Get raw ingredient names from Neo4j
        with self.driver.session() as session:
            result = session.run("MATCH (i:Ingredient) RETURN i.name AS name")
            raw_ingredient_names = [record["name"] for record in result if record["name"]]

        # Step 2: Process known ingredients using same logic
        processed_ingredient_names = {
            self.extract_core_ingredient(name) for name in raw_ingredient_names if name
        }
        processed_ingredient_names = {name for name in processed_ingredient_names if name}
        
        print(processed_ingredient_names)

        self.logger.info(f"Processed {len(processed_ingredient_names)} ingredients from Neo4j.")

        # Step 3: Extract core ingredient terms from product_name
        df["core_ingredient"] = df["product_name"].apply(self.extract_core_ingredient)

        # Step 4: Filter rows
        filtered_df = df[df["core_ingredient"].isin(processed_ingredient_names)].copy()

        if filtered_df.empty:
            self.logger.warning("No matching core ingredients found in Neo4j.")
            return {"status": "success", "loaded": 0, "filtered_out": len(df)}

        # Step 5: Insert into Neo4j
        total_processed = 0
        with self.driver.session() as session:
            for _, row in tqdm(filtered_df.iterrows(), total=len(filtered_df), desc="Loading prices"):
                try:
                    # process
                    total_processed += 1
                except Exception as e:
                    self.logger.warning(f"Error inserting row: {e}")

        return {
            "status": "success",
            "loaded": total_processed,
            "filtered_out": len(df) - len(filtered_df)
        }

