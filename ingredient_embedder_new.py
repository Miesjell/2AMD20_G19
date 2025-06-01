import pandas as pd
import re
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from pathlib import Path
from sklearn.preprocessing import normalize
from collections import defaultdict

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
# First ingredient ever seen is automaticallu set as canonical
# order matters
# thats why we continuesly look for shorter form
model = SentenceTransformer("paraphrase-MiniLM-L6-v2")

def get_embedding(text):
    return model.encode(text.lower().strip(), convert_to_numpy=True)

class IngredientNormalizer:
    def __init__(self, threshold=0.75):
        self.canonical = {}
        self.embeddings = []
        self.names = []
        self.threshold = threshold

    def normalize(self, ingredient: str) -> str:
        ingredient = ingredient.lower().strip()
        vector = get_embedding(ingredient)
        # sort inputs alphabesticallu (shorterst first)
        # increases e.g. water comes before 'warm water'
        #normalize before comparing
        vector = normalize([vector])[0]

        if not self.embeddings:
            self.embeddings.append(vector)
            self.names.append(ingredient)
            self.canonical[ingredient] = ingredient
            return ingredient

        sims = cosine_similarity([vector], self.embeddings)[0]
        best_idx = int(np.argmax(sims))
        best_score = sims[best_idx]

        if best_score > self.threshold:
            previous = self.names[best_idx]
            # Prefer shorter name as canonical
            canon = previous if len(previous) <= len(ingredient) else ingredient
        else:
            canon = ingredient
            self.embeddings.append(vector)
            self.names.append(canon)

        self.canonical[ingredient] = canon
        return canon
    

def print_canonical_groups(normalizer):
    grouped = defaultdict(list)
    for original, canonical in normalizer.canonical.items():
        grouped[canonical].append(original)

    print("\n=== Canonical Ingredient Groups ===")
    for canon, variants in grouped.items():
        if len(variants) > 1:
            print(f"\nCanonical: {canon}")
            for v in sorted(variants):
                print(f"  - {v}")

# ------------ Run Normalization on First 20 Recipes ------------
# Load original dataset
def load_full_format_recipes(data_dir: str = "data"):
    path = Path(data_dir) / "full_format_recipes.json"
    df = pd.read_json(path)
    return df

#Trial on DS
if __name__ == "__main__":
    df = load_full_format_recipes()
    normalizer = IngredientNormalizer(threshold=0.5)

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

    # This shows which raw ingredients mapped to the same canonical name
    print_canonical_groups(normalizer)

    
# Test the class

# if __name__ == "__main__":
#     test_ingredients = [
#         "butter",
#         "unsalted butter",
#         "slice of butter",
#         "olive oil",
#         "extra virgin olive oil",
#         "margarine",
#         "garlic",
#         "chopped garlic",
#         "minced garlic",
#         "sugar",
#         "white sugar"
#     ]

#     normalizer = IngredientNormalizer(threshold=0.80)

#     print("\n--- Ingredient Normalization Test ---")
#     for ing in test_ingredients:
#         canonical = normalizer.normalize(ing)
#         print(f"{ing:30s} → {canonical}")

#     print("\n--- Canonical Mapping ---")
#     for original, mapped in normalizer.canonical.items():
#         print(f"{original:30s} → {mapped}")