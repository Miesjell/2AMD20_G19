import streamlit as st
import pandas as pd
import time
from typing import Dict, Any

import plotly.graph_objects as go
import plotly.express as px

from dashboard.queries import (
    find_recipe_ingredients,
    get_recipe_nutrition_profile,
    get_recipe_recommendations_by_similarity,
)

#### STYILING
def apply_custom_css():
    

    """Apply custom CSS for a modern, clean look."""
    st.markdown("""
    <style>
    .main {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        padding: 2rem;
    }
    
    .stApp {
        background: #f8f9fa;
    }
    
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin: 0.5rem 0;
        border: 1px solid #e9ecef;
    }
    
    .recipe-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 15px;
        margin: 1rem 0;
        box-shadow: 0 8px 16px rgba(0, 0, 0, 0.2);
    }
    
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding-left: 20px;
        padding-right: 20px;
        background-color: #f0f2f6;
        border-radius: 10px;
        border: none;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
    }
    
    .success-box {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    
    .warning-box {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    
    .info-box {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    
    .sidebar .stSelectbox, .sidebar .stMultiSelect {
        background-color: white;
        border-radius: 5px;
    }
    
    .stSelectbox > div > div {
        background-color: white;
        border-radius: 5px;
    }
    
    .stMultiSelect > div > div {
        background-color: white;
        border-radius: 5px;
    }
    
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.5rem 1rem;
        font-weight: bold;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 16px rgba(0, 0, 0, 0.2);
    }
    
    .stExpander {
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }
    
    .recipe-ingredient {
        background: rgba(255, 255, 255, 0.1);
        padding: 0.5rem;
        border-radius: 5px;
        margin: 0.2rem;
        display: inline-block;
    }
    
    .dashboard-card {
        background: white;
        border: 1px solid #e9ecef;
        border-radius: 15px;
        padding: 1.5rem;
        margin: 1rem 0;
        transition: all 0.3s ease;
        box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
    }
    
    .dashboard-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 25px rgba(0, 0, 0, 0.15);
    }
    
    .nutrition-badge {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        color: white;
        padding: 0.3rem 0.8rem;
        border-radius: 20px;
        font-size: 0.8rem;
        margin: 0.2rem;
        display: inline-block;
        font-weight: bold;
    }
    
    .complexity-simple {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
    }
    
    .complexity-moderate {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
    }
    
    .complexity-complex {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
    }
    
    .meal-plan-card {
        background: linear-gradient(135deg, rgba(255,255,255,0.15) 0%, rgba(255,255,255,0.05) 100%);
        border-radius: 12px;
        padding: 1rem;
        margin: 0.5rem;
        border: 1px solid rgba(255,255,255,0.1);
        backdrop-filter: blur(5px);
    }
    
    .feature-highlight {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 2rem;
        border-radius: 15px;
        margin: 1rem 0;
        text-align: center;
        box-shadow: 0 10px 30px rgba(102, 126, 234, 0.3);
    }
    
    .animated-metric {
        animation: pulse 2s infinite;
    }
    
    @keyframes pulse {
        0% { transform: scale(1); }
        50% { transform: scale(1.05); }
        100% { transform: scale(1); }
    }
    
    .gradient-text {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        font-weight: bold;
        color: #667eea; /* Fallback for unsupported browsers */
    }
    
    .floating-card {
        position: relative;
        overflow: hidden;
    }
    
    .floating-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: -100%;
        width: 100%;
        height: 100%;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,0.1), transparent);
        transition: left 0.5s;
    }
    
    .floating-card:hover::before {
        left: 100%;
    }
    </style>
    """, unsafe_allow_html=True)
      
              
#### Utils #####
def get_recipe_complexity_score(recipe_name: str) -> Dict[str, Any]:
    """
    Calculate a complexity score for a recipe based on number of ingredients.

    Args:
        recipe_name: Name of the recipe
        connection: Neo4jConnection object with `execute_query_to_df` method

    Returns:
        Dict with ingredient count, complexity level, and numeric score
    """
    connection = st.session_state.connection
    if not st.session_state.connected:
        return {}

    try:
        ingredients_query = """
        MATCH (r:Recipe {name: $recipe_name})-[:CONTAINS]->(i:Ingredient)
        RETURN count(i) AS ingredient_count
        """
        ingredients_df = connection.execute_query_to_df(
            ingredients_query, {"recipe_name": recipe_name}
        )

        ingredient_count = ingredients_df.iloc[0]['ingredient_count'] if not ingredients_df.empty else 0

        if ingredient_count <= 5:
            complexity = "Simple"
            score = 1
        elif ingredient_count <= 10:
            complexity = "Moderate"
            score = 2
        else:
            complexity = "Complex"
            score = 3

        return {
            'ingredient_count': ingredient_count,
            'complexity': complexity,
            'score': score
        }
    except Exception as e:
        st.error(f"Error calculating complexity: {e}")
        return {}

def save_recipe_preference(person_id: str, recipe_name: str, rating: int = 5) -> bool:
    if not st.session_state.connected:
        return False

    try:
        create_person_query = """
        MERGE (p:Person {id: $person_id})
        RETURN p
        """

        save_pref_query = """
        MATCH (p:Person {id: $person_id})
        MATCH (r:Recipe {name: $recipe_name})
        CREATE (p)-[l:LIKES {rating: $rating, timestamp: timestamp()}]->(r)
        RETURN l
        """

        with st.session_state.connection.get_driver().session() as session:
            session.run(create_person_query, {"person_id": person_id})
            session.run(save_pref_query, {
                "person_id": person_id,
                "recipe_name": recipe_name,
                "rating": rating
            })

        if recipe_name not in st.session_state.favorite_recipes:
            st.session_state.favorite_recipes.append(recipe_name)

        return True

    except Exception as e:
        st.error(f"Error saving recipe preference: {e}")
        if recipe_name not in st.session_state.favorite_recipes:
            st.session_state.favorite_recipes.append(recipe_name)
        return True


def get_recipe_analytics() -> Dict[str, Any]:
        """Get comprehensive analytics about recipes in the database."""
        if not st.session_state.connected:
            return {}
        
        try:
            # Get total counts
            total_recipes_query = "MATCH (r:Recipe) RETURN count(r) AS total_recipes"
            total_ingredients_query = "MATCH (i:Ingredient) RETURN count(i) AS total_ingredients"
            total_allergies_query = "MATCH (a:Allergy) RETURN count(a) AS total_allergies"
            
            total_recipes = st.session_state.connection.execute_query_to_df(total_recipes_query, {})
            total_ingredients = st.session_state.connection.execute_query_to_df(total_ingredients_query, {})
            total_allergies = st.session_state.connection.execute_query_to_df(total_allergies_query, {})
            
            # Get calorie statistics
            calorie_stats_query = """
            MATCH (r:Recipe) 
            WHERE r.calories IS NOT NULL 
            RETURN 
                avg(r.calories) AS avg_calories,
                min(r.calories) AS min_calories,
                max(r.calories) AS max_calories,
                count(r) AS recipes_with_calories
            """
            calorie_stats = st.session_state.connection.execute_query_to_df(calorie_stats_query, {})
            
            # Get popular ingredients
            popular_ingredients = st.session_state.query_manager.find_popular_ingredients(10)
            
            # Get meal type distribution
            meal_type_query = """
            MATCH (r:Recipe)-[:IS_TYPE]->(m:MealType)
            RETURN m.name AS meal_type, count(r) AS recipe_count
            ORDER BY recipe_count DESC
            """
            meal_distribution = st.session_state.connection.execute_query_to_df(meal_type_query, {})
            
            return {
                'total_recipes': total_recipes.iloc[0]['total_recipes'] if not total_recipes.empty else 0,
                'total_ingredients': total_ingredients.iloc[0]['total_ingredients'] if not total_ingredients.empty else 0,
                'total_allergies': total_allergies.iloc[0]['total_allergies'] if not total_allergies.empty else 0,
                'calorie_stats': calorie_stats.to_dict('records')[0] if not calorie_stats.empty else {},
                'popular_ingredients': popular_ingredients,
                'meal_distribution': meal_distribution
            }
        except Exception as e:
            st.error(f"Error getting analytics: {e}")
            return {}
        
##### rendering
def render_recipe_card(recipe_row: Dict[str, Any], index: int, source: str, show_rating: bool = False):
    """Render an enhanced recipe card with modern styling and detailed information."""
    recipe_name = recipe_row['Recipe']
    calories = recipe_row.get('Calories', 'N/A')
    
    ingredients = find_recipe_ingredients(recipe_name)
    ingredient_count = len(ingredients)
    complexity = get_recipe_complexity_score(recipe_name)
    nutrition = get_recipe_nutrition_profile(recipe_name)

    with st.expander(
        f"🍽️ {recipe_name} • {calories} cal • {ingredient_count} ingredients • {complexity.get('complexity', 'Unknown')} complexity",
        expanded=False
    ):
        col1, col2, col3 = st.columns([2, 1, 1])

        with col1:
            if ingredients:
                st.markdown("**🥗 Ingredients:**")
                ingredient_html = "".join([
                    f"""
                    <span style="background: rgba(102, 126, 234, 0.2); color: #667eea; 
                               padding: 0.3rem 0.6rem; border-radius: 15px; margin: 0.2rem; 
                               display: inline-block; font-size: 0.85rem;">
                        {ingredient}
                    </span>
                    """ for ingredient in ingredients[:12]
                ])
                st.markdown(ingredient_html, unsafe_allow_html=True)
                if len(ingredients) > 12:
                    st.markdown(f"*...and {len(ingredients) - 12} more ingredients*")
            else:
                st.markdown("*No ingredient information available*")

            if 'Preparation' in recipe_row and pd.notna(recipe_row['Preparation']):
                st.markdown("**📝 Preparation:**")
                prep_text = recipe_row['Preparation']
                st.markdown(f"_{prep_text[:300]}..._" if len(prep_text) > 300 else f"_{prep_text}_")

        with col2:
            st.markdown("**🔥 Nutrition:**")
            if nutrition:
                nutrition_html = f"""
                <div style="background: rgba(255,255,255,0.1); padding: 0.8rem; border-radius: 8px;">
                    <div><strong>Calories:</strong> {nutrition.get('calories', 'N/A')}</div>
                    <div><strong>Protein:</strong> {nutrition.get('protein', 'N/A')}g</div>
                    <div><strong>Carbs:</strong> {nutrition.get('carbs', 'N/A')}g</div>
                    <div><strong>Fat:</strong> {nutrition.get('fat', 'N/A')}g</div>
                </div>
                """
                st.markdown(nutrition_html, unsafe_allow_html=True)
            elif calories != 'N/A':
                calorie_color = "#27ae60" if calories < 300 else "#f39c12" if calories < 600 else "#e74c3c"
                st.markdown(f"""
                <div style="background: {calorie_color}; color: white; padding: 0.8rem; 
                           border-radius: 8px; text-align: center;">
                    <strong>🔥 {calories} calories</strong>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown("*Nutrition data not available*")

            if complexity:
                complexity_color = {
                    'Simple': '#27ae60',
                    'Moderate': '#f39c12',
                    'Complex': '#e74c3c'
                }.get(complexity.get('complexity', 'Unknown'), '#95a5a6')
                st.markdown(f"""
                <div style="background: {complexity_color}; color: white; padding: 0.5rem; 
                           border-radius: 5px; text-align: center; margin-top: 0.5rem;">
                    <strong>{complexity.get('complexity', 'Unknown')}</strong><br>
                    <small>{complexity.get('ingredient_count', 0)} ingredients</small>
                </div>
                """, unsafe_allow_html=True)

        with col3:
            if show_rating and 'Rating' in recipe_row:
                st.markdown("**⭐ Your Rating:**")
                stars = "⭐" * int(recipe_row['Rating'])
                st.markdown(f"<h3 style='margin: 0; color: #f39c12;'>{stars}</h3>", unsafe_allow_html=True)

            if source != "favorite":
                st.markdown("**💖 Save Recipe:**")
                rating = st.selectbox(
                    "Rating",
                    options=[1, 2, 3, 4, 5],
                    index=4,
                    key=f"rating_{source}_{index}"
                )
                if st.button(f"💖 Save", key=f"save_{source}_{index}", use_container_width=True):
                    if save_recipe_preference(st.session_state.user_id, recipe_name, rating):
                        st.success(f"✅ Saved {recipe_name}!")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("Failed to save recipe.")

            if st.button(f"🔍 Similar", key=f"similar_{source}_{index}", use_container_width=True):
                st.session_state.similarity_search_recipe = recipe_name
                st.info(f"🔍 Finding recipes similar to {recipe_name}...")
                similar_recipes = get_recipe_recommendations_by_similarity(recipe_name, 3)
                if not similar_recipes.empty:
                    st.markdown("**Similar recipes:**")
                    for _, similar in similar_recipes.iterrows():
                        st.markdown(f"• {similar['Recipe']} ({similar['Shared_Ingredients']} shared)")
                else:
                    st.markdown("*No similar recipes found*")

            if nutrition and source != "comparison":
                if st.button(f"📊 Analyze", key=f"analyze_{source}_{index}", use_container_width=True):
                    st.markdown("**📊 Detailed Nutrition:**")
                    for nutrient, value in nutrition.items():
                        if value is not None and nutrient != 'Recipe':
                            st.markdown(f"• **{nutrient.title()}:** {value}")

    st.markdown("<br>", unsafe_allow_html=True)
    

