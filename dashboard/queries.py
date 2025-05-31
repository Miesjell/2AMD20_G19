import streamlit as st
import pandas as pd
from typing import List, Dict, Any


def find_recipe_ingredients(recipe_name: str) -> List[str]:
    """Get ingredients for a specific recipe."""
    if not st.session_state.connected:
        return []

    query = """
    MATCH (r:Recipe {name: $recipe_name})-[:CONTAINS]->(i:Ingredient)
    RETURN i.name AS Ingredient
    """

    df = st.session_state.connection.execute_query_to_df(query, {"recipe_name": recipe_name})
    return df["Ingredient"].tolist() if not df.empty else []

def get_recipe_nutrition_profile(recipe_name: str) -> Dict[str, Any]:
    """Get detailed nutritional profile for a recipe."""
    connection = st.session_state.connection
    if not st.session_state.connected:
        return {}

    try:
        query = """
        MATCH (r:Recipe {name: $recipe_name})
        RETURN r.calories AS calories, 
               r.protein AS protein,
               r.fat AS fat, 
               r.carbohydrates AS carbs,
               r.fiber AS fiber,
               r.sugar AS sugar,
               r.sodium AS sodium
        """
        df = connection.execute_query_to_df(query, {"recipe_name": recipe_name})
        return df.iloc[0].to_dict() if not df.empty else {}
    except Exception as e:
        st.error(f"Error getting nutrition profile: {e}")
        return {}

def get_recipe_recommendations_by_similarity(base_recipe: str, limit: int = 5) -> pd.DataFrame:
    """Find recipes similar to a given recipe based on shared ingredients."""
    connection = st.session_state.connection
    
    if not st.session_state.connected:
        return pd.DataFrame()

    try:
        query = f"""
        MATCH (base:Recipe {{name: $base_recipe}})-[:CONTAINS]->(shared:Ingredient)<-[:CONTAINS]-(similar:Recipe)
        WHERE base <> similar
        WITH similar, count(shared) AS shared_ingredients
        ORDER BY shared_ingredients DESC
        RETURN similar.name AS Recipe, 
               similar.calories AS Calories,
               shared_ingredients AS Shared_Ingredients
        LIMIT {limit}
        """
        return connection.execute_query_to_df(query, {"base_recipe": base_recipe})
    except Exception as e:
        st.error(f"Error finding similar recipes: {e}")
        return pd.DataFrame()
    

    
def find_recipes_with_ingredient(ingredient_name: str, limit: int = 10) -> pd.DataFrame:
    """Find recipes containing a specific ingredient."""
    connection = st.session_state.connection
    
    if not st.session_state.connected:
        return pd.DataFrame()

    query = """
    MATCH (i:Ingredient {name: $ingredient_name})<-[:CONTAINS]-(r:Recipe)
    RETURN r.name AS Recipe, 
           r.calories AS Calories, 
           r.preparation_description AS Preparation
    ORDER BY r.calories ASC
    LIMIT $limit
    """

    return connection.execute_query_to_df(
        query,
        {"ingredient_name": ingredient_name, "limit": limit}
    )

def search_recipes_by_name(search_term: str, limit: int = 10) -> pd.DataFrame:
    """Search recipes by name with fuzzy matching."""
    if not st.session_state.connected or not search_term:
        return pd.DataFrame()
    
    query = f"""
    MATCH (r:Recipe)
    WHERE toLower(r.name) CONTAINS toLower($search_term)
    RETURN r.name AS Recipe, r.calories AS Calories, 
            r.preparation_description AS Preparation
    ORDER BY r.name
    LIMIT {limit}
    """
    
    # Add to search history
    if search_term not in st.session_state.search_history:
        st.session_state.search_history.append(search_term)
        # Keep only last 10 searches
        st.session_state.search_history = st.session_state.search_history[-10:]
    
    return st.session_state.connection.execute_query_to_df(query, {"search_term": search_term})

##### These ones are not used?

# def find_recipes_for_diet(diet_preference: str, limit: int = 10) -> pd.DataFrame:
#     """Find recipes suitable for a specific diet preference."""
#     if not st.session_state.connected:
#         return pd.DataFrame()

#     query = """
#     MATCH (d:DietPreference {name: $diet_name})-[:INCLUDES]->(r:Recipe)
#     RETURN r.name AS Recipe, r.calories AS Calories, 
#            r.preparation_description AS Preparation
#     LIMIT $limit
#     """

#     return st.session_state.connection.execute_query_to_df(
#         query, {"diet_name": diet_preference, "limit": limit}
#     )


# def find_recipes_without_allergens(allergens: List[str], limit: int = 10) -> pd.DataFrame:
#     """Find recipes that don't contain specific allergens."""
#     if not st.session_state.connected or not allergens:
#         return pd.DataFrame()

#     allergen_params = {f"allergen{i}": allergen for i, allergen in enumerate(allergens)}
#     allergen_where_conditions = [
#         f"NOT EXISTS {{ MATCH (r)-[:MAY_CONTAIN_ALLERGEN]->(:Allergy {{name: $allergen{i}}}) }}"
#         for i in range(len(allergens))
#     ]

#     query = f"""
#     MATCH (r:Recipe)
#     WHERE {" AND ".join(allergen_where_conditions)}
#     RETURN r.name AS Recipe, r.calories AS Calories,
#            r.preparation_description AS Preparation
#     LIMIT $limit
#     """

#     allergen_params["limit"] = limit
#     return st.session_state.connection.execute_query_to_df(query, allergen_params)

# def find_recipes_by_meal_type(meal_type: str, limit: int = 10) -> pd.DataFrame:
#     """Find recipes based on meal type."""
#     if not st.session_state.connected:
#         return pd.DataFrame()

#     query = """
#     MATCH (r:Recipe)-[:IS_TYPE]->(:MealType {name: $meal_type})
#     RETURN r.name AS Recipe, r.calories AS Calories, 
#            r.preparation_description AS Preparation
#     LIMIT $limit
#     """

#     return st.session_state.connection.execute_query_to_df(query, {"meal_type": meal_type, "limit": limit})