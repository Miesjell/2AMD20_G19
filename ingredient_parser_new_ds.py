import re
from typing import Tuple
import pandas as pd
import os
import json
from pathlib import Path
from tqdm import tqdm

# Load original dataset
def load_full_format_recipes(data_dir: str = "data"):
    path = Path(data_dir) / "full_format_recipes.json"
    df = pd.read_json(path)
    return df

# Split long strings on new ingredient markers
def split_ingredients(text: str):
    marked = re.sub(r'(?<=,)\s*(?=\d+[\d\s\/\.]*)', '|', text.strip())
    parts = [part.strip(" ,") for part in marked.split('|') if part.strip()]
    return parts

# Pattern to extract amount, unit, and ingredient
PATTERN = re.compile(
    r'^\s*(?P<amount>\d+\s\d+/\d+|\d+/\d+|\d+\.\d+|\d+)?\s*'
    r'(?P<unit>[a-zA-Z]+)?\s+'
    r'(?P<ingredient>.+)$'
)

def parse_ingredient(ingredient_str: str) -> Tuple[str, str, str]:
    match = PATTERN.match(ingredient_str.strip())
    if match:
        amount = match.group("amount") or ""
        unit = match.group("unit") or ""
        ingredient = match.group("ingredient").strip()
        return amount, unit, ingredient
    return "", "", ingredient_str.strip()

# ================ MAIN EXECUTION =======================

if __name__ == "__main__":
    df = load_full_format_recipes()
    print(f"Loaded {len(df)} recipes")

    parsed_ingredient_column = []

    for entry in tqdm(df["ingredients"], desc="Parsing ingredients"):
        parsed = []

        if isinstance(entry, str):
            items = split_ingredients(entry)
        elif isinstance(entry, list):
            items = entry
        else:
            items = []

        for raw in items:
            raw = raw.strip()
            amt, unit, name = parse_ingredient(raw)
            parsed.append({
                "amount": amt,
                "unit": unit,
                "ingredient": name
            })

        parsed_ingredient_column.append(parsed)

    # Add parsed column and save to file
    df["parsed_ingredients"] = parsed_ingredient_column

    output_path = Path("data/parsed_recipes.json")
    df.to_json(output_path, orient="records", indent=2)

    print(f"\n✅ Parsed dataset saved to: {output_path}")