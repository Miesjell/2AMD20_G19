from utils.ingredient_embedder import IngredientNormalizer, split_ingredients, parse_ingredient

# Sample fake data
raw_ingredients = [
    "1 cup warm water, 2 eggs",
    "1.5 cups flour, 1 tsp sugar",
    "2 tbsp olive oil",
    "1 cup water",  # Should group with "warm water"
    "3 eggs",       # Should group with "2 eggs"
    "1 tablespoon sugar",  # Should group with "1 tsp sugar"
]

normalizer = IngredientNormalizer(threshold=0.7
)
normalizer.canonical.clear()
normalizer.embeddings.clear()
normalizer.names.clear()
#normalizer.cache.clear()

# Stage all ingredients
for line in raw_ingredients:
    for item in split_ingredients(line):
        ing = parse_ingredient(item)
        if isinstance(ing, tuple):
            ing = ing[-1]
        if ing:
            normalizer.stage_ingredient(ing)

# Build embeddings in bulk
normalizer.build_embeddings()

# Print the canonical mapping
print("\n--- Canonical Ingredient Map ---")
for ing, canon in normalizer.canonical.items():
    print(f"{ing} -> {canon}")
