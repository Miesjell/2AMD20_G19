import re
from typing import Tuple
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd
import os
import json
from pathlib import Path
from sklearn.preprocessing import normalize
from collections import defaultdict

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

# ------------ Embedding Logic ------------
# First ingredient ever seen is automaticallu set as canonical
# order matters
# thats why we continuesly look for shorter form

model = SentenceTransformer("paraphrase-MiniLM-L6-v2")

def get_embedding(text):
    return model.encode(text.lower().strip(), convert_to_numpy=True)

class IngredientNormalizer:
    def __init__(self, threshold=0.75):
        self.to_embed = set()
        self.threshold = threshold
        self.embeddings = []
        self.names = []
        self.canonical = {}

    def stage_ingredient(self, ingredient: str):
        ing = ingredient.lower().strip()
        if ing not in self.canonical:
            self.to_embed.add(ing)
        return ing

    def build_embeddings(self):
        if not self.to_embed:
            return
        print(f"[Embedding] Encoding {len(self.to_embed)} new ingredients...")
        texts = sorted(list(self.to_embed))
        vectors = model.encode(texts, convert_to_numpy=True, normalize_embeddings=True)
        for name, vec in zip(texts, vectors):
            if not self.embeddings:
                self.embeddings.append(vec)
                self.names.append(name)
                self.canonical[name] = name
            else:
                sims = cosine_similarity([vec], self.embeddings)[0]
                best_idx = int(np.argmax(sims))
                best_score = sims[best_idx]
                if best_score > self.threshold:
                    previous = self.names[best_idx]
                    canon = previous if len(previous) <= len(name) else name
                else:
                    canon = name
                    self.embeddings.append(vec)
                    self.names.append(name)
                self.canonical[name] = canon
        self.to_embed.clear()

    def normalize(self, ingredient: str):
        ing = ingredient.lower().strip()
        return self.canonical.get(ing, ing)