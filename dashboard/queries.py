import streamlit as st
import pandas as pd
from typing import List


def find_recipes_for_diet(diet_preference: str, limit: int = 10) -> pd.DataFrame:
    """Find recipes suitable for a specific diet preference."""
    if not st.session_state.connected:
        return pd.DataFrame()

    query = """
    MATCH (d:DietPreference {name: $diet_name})-[:INCLUDES]->(r:Recipe)
    RETURN r.name AS Recipe, r.calories AS Calories, 
           r.preparation_description AS Preparation
    LIMIT $limit
    """

    return st.session_state.connection.execute_query_to_df(
        query, {"diet_name": diet_preference, "limit": limit}
    )


def find_recipes_without_allergens(allergens: List[str], limit: int = 10) -> pd.DataFrame:
    """Find recipes that don't contain specific allergens."""
    if not st.session_state.connected or not allergens:
        return pd.DataFrame()

    allergen_params = {f"allergen{i}": allergen for i, allergen in enumerate(allergens)}
    allergen_where_conditions = [
        f"NOT EXISTS {{ MATCH (r)-[:MAY_CONTAIN_ALLERGEN]->(:Allergy {{name: $allergen{i}}}) }}"
        for i in range(len(allergens))
    ]

    query = f"""
    MATCH (r:Recipe)
    WHERE {" AND ".join(allergen_where_conditions)}
    RETURN r.name AS Recipe, r.calories AS Calories,
           r.preparation_description AS Preparation
    LIMIT $limit
    """

    allergen_params["limit"] = limit
    return st.session_state.connection.execute_query_to_df(query, allergen_params)


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


def find_recipes_by_meal_type(meal_type: str, limit: int = 10) -> pd.DataFrame:
    """Find recipes based on meal type."""
    if not st.session_state.connected:
        return pd.DataFrame()

    query = """
    MATCH (r:Recipe)-[:IS_TYPE]->(:MealType {name: $meal_type})
    RETURN r.name AS Recipe, r.calories AS Calories, 
           r.preparation_description AS Preparation
    LIMIT $limit
    """

    return st.session_state.connection.execute_query_to_df(query, {"meal_type": meal_type, "limit": limit})