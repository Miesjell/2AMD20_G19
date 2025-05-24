# from typing import Dict, Any, Optional
# import pandas as pd
# from neo4j import Driver
# from tqdm import tqdm

# from .base import DataLoader

# class PriceLoader(DataLoader):
#     """
#     Loader for product prices filtered by known ingredient names in the graph.
#     """

#     def __init__(self, driver: Optional[Driver] = None):
#         super().__init__(driver)

#     def load_data(self, df: pd.DataFrame, batch_size: int = 100) -> Dict[str, Any]:
#         if not self.driver:
#             return {"status": "error", "error": "Neo4j driver not set. Call set_driver() first."}

#         # Clean column names: lowercase, strip, replace spaces/characters
#         df.columns = (
#             df.columns.str.lower()
#                       .str.strip()
#                     #   .str.replace(r"[^a-zA-Z0-9_]", "_", regex=True)
#         )

#         if "product_name" not in df.columns:
#             return {"status": "error", "error": "Missing required 'product_name' column."}

#         # Clean product names
#         df["product_name"] = df["product_name"].astype(str).str.lower().str.strip()

#         # Step 1: Get all ingredient names from Neo4j
#         with self.driver.session() as session:
#             result = session.run("MATCH (i:Ingredient) RETURN i.name AS name")
#             ingredient_names = {
#                 record["name"].lower().strip()
#                 for record in result if record["name"]
#             }

#         self.logger.info(f"Retrieved {len(ingredient_names)} ingredients from Neo4j.")

#         # Step 2: Filter price data to only known ingredients
#         filtered_df = df[df["product_name"].isin(ingredient_names)].copy()

#         if filtered_df.empty:
#             self.logger.warning("No matching products found in ingredient list.")
#             return {"status": "success", "loaded": 0, "filtered_out": len(df)}

#         # Step 3: Optional: Load into graph (example only)
#         total_processed = 0
#         with self.driver.session() as session:
#             for _, row in tqdm(filtered_df.iterrows(), total=len(filtered_df), desc="Loading prices"):
#                 try:
#                     session.run(
#                         """
#                         MATCH (i:Ingredient {name: $name})
#                         MERGE (i)-[:HAS_PRICE]->(:Price {
#                             source: $source,
#                             price: $price
#                         })
#                         """,
#                         name=row["product_name"],
#                         source=row.get("source", "unknown"),
#                         price=row.get("price", None),
#                     )
#                     total_processed += 1
#                 except Exception as e:
#                     self.logger.warning(f"Error inserting row: {e}")

#         return {"status": "success", "loaded": total_processed, "filtered_out": len(df) - len(filtered_df)}
