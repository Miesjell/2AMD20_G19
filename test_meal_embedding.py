from utils.meal_type_embedder import MealTypeEmbedder

# Sample recipe titles
sample_titles = [
    "Classic fluffy pancakes with maple syrup",
    "Turkey sandwich with avocado",
    "Steak with garlic mashed potatoes",
    "Banana smoothie with almond milk",
    "Chocolate cake",
    "Scrambled eggs and toast",
    "Beef ramen",
    "Pumpkin spice latte",
    "Grilled chicken salad",
    "Lasagna",
    "Coffee",
    "Rice pudding",
]

embedder = MealTypeEmbedder(threshold=0.3) #0.35

print("\n--- Meal Type Classification ---")
for title in sample_titles:
    category = embedder.classify(title)
    print(f"{title:<45} → {category}")
