import streamlit as st
import pandas as pd
import time
from typing import Optional, List
from dashboard.dashboard_helpers import render_recipe_card


def render_recommendations_tab():
    """Render the smart recommendations tab with enhanced UI."""
    st.header("🎯 Personalized Recipe Recommendations")

    diet_preferences = st.session_state.diet_preferences
    meal_types = st.session_state.meal_types
    allergies = st.session_state.allergies

    # User input
    with st.container():
        st.markdown("### 👤 Your Preferences")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### 🥗 Dietary Preferences")
            selected_diets = st.multiselect(
                "What dietary preferences do you follow?",
                options=diet_preferences,
                help="Select all that apply to get the most relevant recommendations"
            )

            st.markdown("#### 🍽️ Meal Type")
            selected_meal_type = st.selectbox(
                "What type of meal are you looking for?",
                options=["All Types"] + meal_types,
                help="Filter recipes by meal category"
            )

        with col2:
            st.markdown("#### ⚠️ Allergies & Restrictions")
            selected_allergies = st.multiselect(
                "What allergies or ingredients should we avoid?",
                options=allergies,
                help="We'll make sure to exclude these from your recommendations"
            )

            st.markdown("#### 🔥 Calorie Range")
            col_a, col_b = st.columns(2)
            with col_a:
                min_calories = st.number_input("Min Calories", min_value=0, value=0)
            with col_b:
                max_calories = st.number_input("Max Calories", min_value=0, value=2000)

    # Advanced filters
    with st.expander("🔧 Advanced Filters", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            num_recommendations = st.slider(
                "Number of recommendations", min_value=1, max_value=20, value=8
            )
        with col2:
            sort_by = st.selectbox(
                "Sort recommendations by:",
                ["Calories (Low to High)", "Calories (High to Low)", "Recipe Name"]
            )

    if st.button("🔍 Find My Perfect Recipes", use_container_width=True, type="primary"):
        with st.spinner("🤖 AI is analyzing your preferences..."):
            progress_bar = st.progress(0)
            for i in range(100):
                time.sleep(0.01)
                progress_bar.progress(i + 1)

            min_cal = min_calories if min_calories > 0 else None
            max_cal = max_calories if max_calories > 0 else None

            recipes_df = find_personalized_recipes(
                diet_preferences=selected_diets,
                allergies=selected_allergies,
                meal_type=selected_meal_type if selected_meal_type != "All Types" else None,
                min_calories=min_cal,
                max_calories=max_cal,
                limit=num_recommendations
            )

            progress_bar.empty()

            if recipes_df.empty:
                st.markdown("""
                <div class="warning-box">
                    <h3>😕 No Perfect Matches Found</h3>
                    <p>Try adjusting your criteria:</p>
                    <ul>
                        <li>Remove some dietary restrictions</li>
                        <li>Increase calorie range</li>
                        <li>Try different meal types</li>
                    </ul>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div class="success-box">
                    <h3>🎉 Found {len(recipes_df)} Perfect Recipes for You!</h3>
                    <p>Based on your preferences, here are our top recommendations:</p>
                </div>
                """, unsafe_allow_html=True)

                st.session_state.current_recommendations = recipes_df

                for i, (_, row) in enumerate(recipes_df.iterrows()):
                    render_recipe_card(row, i, "recommendation")

def find_personalized_recipes(
    diet_preferences: List[str],
    allergies: List[str],
    meal_type: Optional[str] = None,
    min_calories: Optional[int] = None,
    max_calories: Optional[int] = None,
    limit: int = 10
) -> pd.DataFrame:
    if not st.session_state.connected:
        return pd.DataFrame()

    params = {}
    diet_condition, meal_type_condition, allergy_condition, calorie_condition = "", "", "", ""

    if diet_preferences:
        diet_params = {f"diet{i}": dp for i, dp in enumerate(diet_preferences)}
        params.update(diet_params)
        include = [f"EXISTS {{ MATCH (r)<-[:INCLUDES]-(dp:DietPreference {{name: $diet{i}}}) }}" for i in range(len(diet_preferences))]
        exclude = [f"NOT EXISTS {{ MATCH (r)<-[:EXCLUDES]-(dp:DietPreference {{name: $diet{i}}}) }}" for i in range(len(diet_preferences))]
        diet_condition = f"\nAND {' AND '.join(include)}\nAND {' AND '.join(exclude)}"

    if meal_type and meal_type != "All Types":
        params["meal_type"] = meal_type
        meal_type_condition = "\nAND EXISTS {\nMATCH (r)-[:IS_TYPE]->(:MealType {name: $meal_type})\n}"

    if allergies:
        allergy_params = {f"allergen{i}": a for i, a in enumerate(allergies)}
        params.update(allergy_params)
        direct = [f"NOT EXISTS {{ MATCH (r)-[:MAY_CONTAIN_ALLERGEN]->(:Allergy {{name: $allergen{i}}}) }}" for i in range(len(allergies))]
        via_ingredient = [f"NOT EXISTS {{ MATCH (r)-[:CONTAINS]->(:Ingredient)<-[:CAUSES]-(:Allergy {{name: $allergen{i}}}) }}" for i in range(len(allergies))]
        allergy_condition = "\nAND " + " AND ".join([f"({d} AND {v})" for d, v in zip(direct, via_ingredient)])

    if min_calories is not None:
        params["min_calories"] = min_calories
        calorie_condition += " AND r.calories >= $min_calories"
    if max_calories is not None:
        params["max_calories"] = max_calories
        calorie_condition += " AND r.calories <= $max_calories"

    query = f"""
    MATCH (r:Recipe)
    WHERE true
    {diet_condition}
    {meal_type_condition}
    {allergy_condition}
    {calorie_condition}
    RETURN r.name AS Recipe, r.calories AS Calories,
           r.preparation_description AS Preparation
    ORDER BY r.calories ASC
    LIMIT {limit}
    """

    return st.session_state.connection.execute_query_to_df(query, params)