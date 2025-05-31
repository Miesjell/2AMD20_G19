import streamlit as st
import pandas as pd
from typing import Dict, Any

from dashboard.dashboard_helpers import render_recipe_card, get_recipe_analytics
from dashboard.queries import find_recipes_with_ingredient

def render_ingredient_insights_tab():
    """Render the ingredient insights and exploration tab."""
    st.header("🧪 Ingredient Intelligence")
    # Ingredient search
    col1, col2 = st.columns([3, 1])
    with col1:
        ingredient_name = st.text_input(
            "🔍 Explore an ingredient:",
            value=st.session_state.get("selected_ingredient", ""),
            placeholder="Try 'tomato', 'chicken', 'flour'...",
            help="Get detailed insights about any ingredient"
        )

    if ingredient_name:
        with st.spinner(f"Analyzing '{ingredient_name}'..."):
            insights = get_ingredient_insights(ingredient_name)

            if insights.get('recipe_count', 0) > 0:
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown(f"""
                    <div class="metric-card">
                        <h3 style="color: #667eea;">📊 Recipe Usage</h3>
                        <h1 style="color: #333;">{insights['recipe_count']}</h1>
                        <p>recipes contain {ingredient_name}</p>
                    </div>
                    """, unsafe_allow_html=True)

                with col2:
                    allergies = insights.get('related_allergies', [])
                    if allergies:
                        allergy_list = ", ".join(allergies)
                        st.markdown(f"""
                        <div class="metric-card">
                            <h3 style="color: #e74c3c;">⚠️ Related Allergies</h3>
                            <p>{allergy_list}</p>
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.markdown(f"""
                        <div class="metric-card">
                            <h3 style="color: #27ae60;">✅ Allergy Status</h3>
                            <p>No known allergies associated</p>
                        </div>
                        """, unsafe_allow_html=True)

                # Recipes with this ingredient
                ingredient_recipes = find_recipes_with_ingredient(ingredient_name)
                if not ingredient_recipes.empty:
                    st.markdown(f"### 🍽️ Recipes containing {ingredient_name}")
                    for i, (_, row) in enumerate(ingredient_recipes.head(5).iterrows()):
                        render_recipe_card(row, i, source="ingredient")
                else:
                    st.info("No recipes found with this ingredient.")
            else:
                st.warning(f"No data found for ingredient '{ingredient_name}'. Try a different ingredient!")

    # Popular ingredients section
    st.markdown("### 🏆 Most Popular Ingredients")
    analytics = get_recipe_analytics()
    popular_ingredients = analytics.get('popular_ingredients', pd.DataFrame())

    if not popular_ingredients.empty:
        cols = st.columns(4)
        for i, (_, row) in enumerate(popular_ingredients.head(12).iterrows()):
            with cols[i % 4]:
                if st.button(f"🥕 {row['Ingredient']} ({row['RecipeCount']})",
                            key=f"popular_ingredient_{i}",
                            help=f"Used in {row['RecipeCount']} recipes"):
                    st.session_state.selected_ingredient = row['Ingredient']
                    st.rerun()
                    
def get_ingredient_insights(ingredient_name: str) -> Dict[str, Any]:
    """Get detailed insights about a specific ingredient."""
    if not st.session_state.connected:
        return {}
    
    try:
        # Recipes containing this ingredient
        recipes_query = """
        MATCH (i:Ingredient {name: $ingredient_name})<-[:CONTAINS]-(r:Recipe)
        RETURN count(r) AS recipe_count
        """
        
        # Allergies related to this ingredient
        allergies_query = """
        MATCH (i:Ingredient {name: $ingredient_name})-[:CAUSES]->(a:Allergy)
        RETURN collect(a.name) AS related_allergies
        """
        
        recipe_count = st.session_state.connection.execute_query_to_df(
            recipes_query, {"ingredient_name": ingredient_name}
        )
        allergies = st.session_state.connection.execute_query_to_df(
            allergies_query, {"ingredient_name": ingredient_name}
        )
        
        return {
            'recipe_count': recipe_count.iloc[0]['recipe_count'] if not recipe_count.empty else 0,
            'related_allergies': allergies.iloc[0]['related_allergies'] if not allergies.empty else []
        }
    except Exception as e:
        st.error(f"Error getting ingredient insights: {e}")
        return {}
