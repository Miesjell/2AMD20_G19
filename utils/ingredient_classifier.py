"""
Ingredient classification system for dietary filtering.

This module classifies ingredients into various categories to support
efficient recipe filtering for dietary preferences and allergens.
"""
import re
from typing import Dict, Set, List, Tuple
from dataclasses import dataclass
from enum import Enum


@dataclass
class IngredientProperties:
    """Properties of an ingredient for dietary filtering."""
    is_meat: bool = False
    is_poultry: bool = False
    is_fish: bool = False
    is_seafood: bool = False
    is_dairy: bool = False
    is_egg: bool = False
    is_gluten_containing: bool = False
    is_nut: bool = False
    is_soy: bool = False
    is_vegetarian: bool = True
    is_vegan: bool = True
    is_kosher: bool = True
    is_halal: bool = True
    allergens: List[str] = None
    
    def __post_init__(self):
        if self.allergens is None:
            self.allergens = []
        
        # Auto-update vegetarian/vegan based on other properties
        if self.is_meat or self.is_poultry or self.is_fish or self.is_seafood:
            self.is_vegetarian = False
            self.is_vegan = False
        
        if self.is_dairy or self.is_egg:
            self.is_vegan = False
            
        # Auto-populate allergens
        if self.is_dairy and 'dairy' not in self.allergens:
            self.allergens.append('dairy')
        if self.is_egg and 'egg' not in self.allergens:
            self.allergens.append('egg')
        if self.is_gluten_containing and 'gluten' not in self.allergens:
            self.allergens.append('gluten')
        if self.is_nut and 'nuts' not in self.allergens:
            self.allergens.append('nuts')
        if self.is_soy and 'soy' not in self.allergens:
            self.allergens.append('soy')
        if self.is_fish and 'fish' not in self.allergens:
            self.allergens.append('fish')
        if self.is_seafood and 'shellfish' not in self.allergens:
            self.allergens.append('shellfish')


class IngredientClassifier:
    """
    Classifies ingredients based on dietary properties and allergens.
    
    This classifier uses keyword matching and pattern recognition to identify
    ingredient properties for efficient recipe filtering.
    """
    
    def __init__(self):
        self._setup_classification_data()
    
    def _setup_classification_data(self):
        """Initialize classification dictionaries with ingredient keywords."""
        
        # Meat and animal proteins
        self.meat_keywords = {
            'beef', 'steak', 'ground beef', 'chuck', 'sirloin', 'ribeye', 'brisket',
            'pork', 'bacon', 'ham', 'sausage', 'pepperoni', 'prosciutto', 'chorizo',
            'lamb', 'mutton', 'veal', 'venison', 'duck', 'goose', 'turkey', 'chicken',
            'meat', 'meatball', 'meatloaf', 'salami', 'bologna', 'pastrami',
            'andouille', 'bratwurst', 'kielbasa', 'frankfurter', 'hot dog',
            'pancetta', 'guanciale', 'lard', 'tallow', 'suet'
        }
        
        self.poultry_keywords = {
            'chicken', 'turkey', 'duck', 'goose', 'quail', 'pheasant', 'cornish hen',
            'chicken breast', 'chicken thigh', 'chicken wing', 'turkey breast',
            'ground turkey', 'ground chicken', 'chicken stock', 'turkey stock',
            'poultry', 'fowl'
        }
        
        self.fish_keywords = {
            'salmon', 'tuna', 'cod', 'halibut', 'tilapia', 'bass', 'trout', 'catfish',
            'mackerel', 'sardine', 'anchovies', 'herring', 'sole', 'flounder',
            'mahi mahi', 'snapper', 'grouper', 'swordfish', 'monkfish', 'haddock',
            'fish', 'fish sauce', 'fish stock', 'fish fillet', 'smoked fish'
        }
        
        self.seafood_keywords = {
            'shrimp', 'lobster', 'crab', 'scallops', 'oysters', 'mussels', 'clams',
            'squid', 'octopus', 'calamari', 'prawns', 'crayfish', 'langostino',
            'sea urchin', 'abalone', 'conch', 'whelk', 'cockles', 'barnacles',
            'seafood', 'shellfish', 'crustacean', 'mollusk'
        }
        
        # Dairy products
        self.dairy_keywords = {
            'milk', 'cream', 'butter', 'cheese', 'yogurt', 'sour cream', 'creme fraiche',
            'heavy cream', 'whipping cream', 'half and half', 'buttermilk',
            'cottage cheese', 'ricotta', 'mozzarella', 'cheddar', 'swiss', 'brie',
            'camembert', 'gouda', 'parmesan', 'romano', 'feta', 'goat cheese',
            'cream cheese', 'mascarpone', 'whey', 'casein', 'lactose',
            'ice cream', 'sherbet', 'frozen yogurt', 'condensed milk', 'evaporated milk',
            'dairy', 'milk powder', 'dry milk', 'ghee'
        }
        
        # Eggs
        self.egg_keywords = {
            'egg', 'eggs', 'egg white', 'egg yolk', 'whole egg', 'beaten egg',
            'scrambled egg', 'hard boiled egg', 'mayonnaise', 'aioli', 'hollandaise',
            'egg wash', 'egg substitute', 'quail egg', 'duck egg'
        }
        
        # Gluten-containing grains
        self.gluten_keywords = {
            'wheat', 'flour', 'all-purpose flour', 'bread flour', 'cake flour',
            'whole wheat flour', 'wheat germ', 'wheat bran', 'semolina',
            'barley', 'rye', 'spelt', 'kamut', 'bulgur', 'couscous', 'farro',
            'bread', 'pasta', 'noodles', 'spaghetti', 'macaroni', 'linguine',
            'penne', 'rigatoni', 'fettuccine', 'lasagna', 'ravioli', 'gnocchi',
            'breadcrumbs', 'panko', 'croutons', 'crackers', 'pretzels',
            'beer', 'ale', 'lager', 'stout', 'wheat beer', 'malt', 'brewer\'s yeast',
            'soy sauce', 'teriyaki sauce', 'worcestershire sauce', 'seitan',
            'vital wheat gluten', 'gluten'
        }
        
        # Nuts and seeds
        self.nut_keywords = {
            'almonds', 'walnuts', 'pecans', 'hazelnuts', 'cashews', 'pistachios',
            'macadamia', 'brazil nuts', 'pine nuts', 'chestnuts', 'peanuts',
            'peanut butter', 'almond butter', 'cashew butter', 'tahini',
            'sesame seeds', 'sunflower seeds', 'pumpkin seeds', 'flax seeds',
            'chia seeds', 'hemp seeds', 'poppy seeds', 'sesame oil',
            'nut', 'nuts', 'tree nuts', 'seed', 'seeds'
        }
        
        # Soy products
        self.soy_keywords = {
            'soy', 'soya', 'soybeans', 'soy sauce', 'tofu', 'tempeh', 'miso',
            'soy milk', 'soy flour', 'soy protein', 'edamame', 'soybean oil',
            'lecithin', 'soy lecithin', 'tamari', 'shoyu', 'natto'
        }
        
        # Non-kosher/non-halal specific
        self.pork_keywords = {
            'pork', 'bacon', 'ham', 'sausage', 'pepperoni', 'prosciutto', 'chorizo',
            'pancetta', 'guanciale', 'lard', 'pork chop', 'pork shoulder', 'pork belly',
            'pork tenderloin', 'ground pork', 'pork ribs', 'pork butt'
        }
        
        self.shellfish_keywords = {
            'shrimp', 'lobster', 'crab', 'scallops', 'oysters', 'mussels', 'clams',
            'squid', 'octopus', 'calamari', 'prawns', 'crayfish', 'langostino'
        }
    
    def classify_ingredient(self, ingredient: str) -> IngredientProperties:
        """
        Classify an ingredient and return its dietary properties.
        
        Args:
            ingredient: The ingredient name to classify
            
        Returns:
            IngredientProperties object with dietary flags set
        """
        ingredient_lower = ingredient.lower().strip()
        
        # Initialize properties
        props = IngredientProperties()
        
        # Check for meat
        if any(keyword in ingredient_lower for keyword in self.meat_keywords):
            props.is_meat = True
        
        # Check for poultry
        if any(keyword in ingredient_lower for keyword in self.poultry_keywords):
            props.is_poultry = True
            props.is_meat = True  # Poultry is also meat
        
        # Check for fish
        if any(keyword in ingredient_lower for keyword in self.fish_keywords):
            props.is_fish = True
        
        # Check for seafood
        if any(keyword in ingredient_lower for keyword in self.seafood_keywords):
            props.is_seafood = True
        
        # Check for dairy
        if any(keyword in ingredient_lower for keyword in self.dairy_keywords):
            props.is_dairy = True
        
        # Check for eggs
        if any(keyword in ingredient_lower for keyword in self.egg_keywords):
            props.is_egg = True
        
        # Check for gluten
        if any(keyword in ingredient_lower for keyword in self.gluten_keywords):
            props.is_gluten_containing = True
        
        # Check for nuts
        if any(keyword in ingredient_lower for keyword in self.nut_keywords):
            props.is_nut = True
        
        # Check for soy
        if any(keyword in ingredient_lower for keyword in self.soy_keywords):
            props.is_soy = True
        
        # Check kosher/halal restrictions
        if any(keyword in ingredient_lower for keyword in self.pork_keywords):
            props.is_kosher = False
            props.is_halal = False
        
        if any(keyword in ingredient_lower for keyword in self.shellfish_keywords):
            props.is_kosher = False
        
        # Auto-update vegetarian/vegan status (done in __post_init__)
        
        return props
    
    def get_dietary_cypher_properties(self, properties: IngredientProperties) -> Dict[str, any]:
        """
        Convert IngredientProperties to a dictionary suitable for Cypher queries.
        
        Args:
            properties: IngredientProperties object
            
        Returns:
            Dictionary with property names and values for Neo4j
        """
        return {
            'is_meat': properties.is_meat,
            'is_poultry': properties.is_poultry,
            'is_fish': properties.is_fish,
            'is_seafood': properties.is_seafood,
            'is_dairy': properties.is_dairy,
            'is_egg': properties.is_egg,
            'is_gluten_containing': properties.is_gluten_containing,
            'is_nut': properties.is_nut,
            'is_soy': properties.is_soy,
            'is_vegetarian': properties.is_vegetarian,
            'is_vegan': properties.is_vegan,
            'is_kosher': properties.is_kosher,
            'is_halal': properties.is_halal,
            'allergens': properties.allergens
        }
    
    def classify_batch(self, ingredients: List[str]) -> Dict[str, IngredientProperties]:
        """
        Classify a batch of ingredients efficiently.
        
        Args:
            ingredients: List of ingredient names to classify
            
        Returns:
            Dictionary mapping ingredient names to their properties
        """
        return {ingredient: self.classify_ingredient(ingredient) for ingredient in ingredients}


# Global classifier instance
_classifier = None

def get_classifier() -> IngredientClassifier:
    """Get the global ingredient classifier instance."""
    global _classifier
    if _classifier is None:
        _classifier = IngredientClassifier()
    return _classifier


def classify_ingredient(ingredient: str) -> IngredientProperties:
    """Convenience function to classify a single ingredient."""
    return get_classifier().classify_ingredient(ingredient)


def classify_ingredients(ingredients: List[str]) -> Dict[str, IngredientProperties]:
    """Convenience function to classify multiple ingredients."""
    return get_classifier().classify_batch(ingredients)
