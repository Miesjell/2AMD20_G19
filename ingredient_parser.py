import re
from typing import Tuple
import pandas as pd
import os
import json
from pathlib import Path

# Import ds
def load_full_format_recipes(data_dir: str = "data"):
    path = Path(data_dir) / "full_format_recipes.json"
    df = pd.read_json(path)
    return df
    

# col entries = list seperated by comma
# sometimes like this though: 1 large garlic clove, crushed,
# so full ingredient always starts with an integer and should end when theres another integer found
# shape:
# - amount, unit (cup, tablespoon, etc), ingredient 
# - amount, ingredient

# Metric is monstly 1 word
# ingredient can consist of multiple words:
# - 'water'
# - 'diced drained oil-packed sun-dried tomatoes'


def split_ingredients(text: str):
    """
    Splits a long comma-separated string of ingredients into individual phrases.
    It assumes a new ingredient starts with a number (e.g., '1', '1/2', '1 1/2').
    """
    # Add a marker before new ingredients
    marked = re.sub(r'(?<=,)\s*(?=\d+[\d\s\/\.]*)', '|', text.strip())
    # Split on the marker
    parts = [part.strip(" ,") for part in marked.split('|') if part.strip()]
    return parts

PATTERN = re.compile(
    r'^\s*(?P<amount>\d+\s\d+/\d+|\d+/\d+|\d+\.\d+|\d+)?\s*'
    r'(?P<unit>[a-zA-Z]+)?\s+'
    r'(?P<ingredient>.+)$'
)

def parse_ingredient(ingredient_str: str) -> Tuple[str, str, str]:
    """
    Parse a single ingredient string into (amount, unit, ingredient).
    If unit is missing, it's assumed to be part of the ingredient.
    """
    match = PATTERN.match(ingredient_str.strip())
    if match:
        amount = match.group("amount") or ""
        unit = match.group("unit") or ""
        ingredient = match.group("ingredient").strip()
        #return amount, unit, ingredient
        return ingredient
    return "", "", ingredient_str.strip()

def parse_ingredient_only_ingredient(ingredient_str: str) -> str:
    """
    Parse a single ingredient string and return only the ingredient name.
    """
    match = PATTERN.match(ingredient_str.strip())
    if match:
        return match.group("ingredient").strip()
    return ingredient_str.strip()

# ================ TEST ON DS =======================

if __name__ == "__main__":
    df = load_full_format_recipes()
    ingredients_series = df["ingredients"]

    print(f"Total recipes: {len(ingredients_series)}\n")

    # Test on first 5 entries
    for i, entry in enumerate(ingredients_series.head(20)):
        print(f"\n--- Recipe {i+1} ---")

        if isinstance(entry, str):
            ingredient_list = split_ingredients(entry)
        elif isinstance(entry, list):
            # Already split
            ingredient_list = entry
        else:
            print("Unknown format, skipping.")
            continue

        for raw in ingredient_list:
            raw = raw.strip()
            print(f"Raw: {raw}")
            print("Parsed:", parse_ingredient(raw))


# ================ TEST =============================

# test_inputs = [
#     "1 cup sugar",
#     "2 eggs",
#     "3 tablespoons olive oil",
#     "1.5 diced onions",
#     "1/2 teaspoon cinnamon",
#     "2 diced drained oil-packed sun-dried tomatoes"
# ]

# for i in test_inputs:
#     print(parse_ingredient(i))
