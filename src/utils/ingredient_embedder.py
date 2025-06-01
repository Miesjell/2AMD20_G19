import pandas as pd
import re
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from pathlib import Path

# ------------ Regex-Based Ingredient Parser ------------
PATTERN = re.compile(
    r'^\s*(?P<amount>\d+\s\d+/\d+|\d+/\d+|\d+\.\d+|\d+)?\s*'
    r'(?P<unit>[a-zA-Z]+)?\s+'
    r'(?P<ingredient>.+)$'
)

def parse_ingredient(ingredient_str: str):
    match = PATTERN.match(ingredient_str.strip())
    if match:
        return match.group("amount") or "", match.group("unit") or "", match.group("ingredient").strip()
    return "", "", ingredient_str.strip()

def split_ingredients(text: str):
    marked = re.sub(r'(?<=,)\s*(?=\d+[\d\s\/\.]*)', '|', text.strip())
    parts = [part.strip(" ,") for part in marked.split('|') if part.strip()]
    return parts

# ------------ Embedding Logic ------------
model = SentenceTransformer("paraphrase-MiniLM-L6-v2")

def get_embedding(text):
    return model.encode(text.lower().strip(), convert_to_numpy=True)

class IngredientNormalizer:
    def __init__(self, threshold=0.9):
        self.canonical = {}
        self.embeddings = []
        self.names = []
        self.threshold = threshold

    def normalize(self, ingredient: str) -> str:
        ingredient = ingredient.lower().strip()
        vector = get_embedding(ingredient)

        if not self.embeddings:
            self.embeddings.append(vector)
            self.names.append(ingredient)
            self.canonical[ingredient] = ingredient
            return ingredient

        sims = cosine_similarity([vector], self.embeddings)[0]
        best_idx = int(np.argmax(sims))

        if sims[best_idx] > self.threshold:
            canon = self.names[best_idx]
        else:
            canon = ingredient
            self.embeddings.append(vector)
            self.names.append(canon)

        self.canonical[ingredient] = canon
        return canon

# ------------ Run Normalization on First 20 Recipes ------------
# Load original dataset
def load_full_format_recipes(data_dir: str = "data"):
    path = Path(data_dir) / "full_format_recipes.json"
    df = pd.read_json(path)
    return df

if __name__ == "__main__":
    df = load_full_format_recipes()
    normalizer = IngredientNormalizer(threshold=0.9)

    print("\n=== Ingredient Normalization from First 20 Recipes ===")

    all_normalized = set()

    for idx, entry in enumerate(df["ingredients"].head(20)):
        print(f"\n--- Recipe {idx+1} ---")

        if isinstance(entry, str):
            ingredient_list = split_ingredients(entry)
        elif isinstance(entry, list):
            ingredient_list = entry
        else:
            print("Invalid format.")
            continue

        for raw in ingredient_list:
            _, _, name = parse_ingredient(raw)
            canonical = normalizer.normalize(name)
            all_normalized.add(canonical)
            print(f"{raw:50s} → {canonical}")

    print(f"\nTotal unique canonical ingredients found: {len(all_normalized)}")
