import streamlit as st
from dashboard.dashboard_helpers import render_recipe_card
import pandas as pd

def render_favorites_tab():
    """Render the favorites tab with enhanced features."""
    st.header("❤️ Your Recipe Collection")

    # Load saved recipes from the session's user ID
    saved_recipes_df = get_saved_recipes(st.session_state.user_id)

    if saved_recipes_df.empty:
        st.markdown("""
        <div class="info-box">
            <h3>📚 Start Building Your Recipe Collection!</h3>
            <p>You haven't saved any recipes yet. Here's how to get started:</p>
            <ol>
                <li>Go to the "Smart Recommendations" tab</li>
                <li>Find recipes you love</li>
                <li>Click "Save Recipe" to add them here</li>
            </ol>
            <p>Your saved recipes will appear here for easy access!</p>
        </div>
        """, unsafe_allow_html=True)
    else:
        # Collection stats
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Saved", len(saved_recipes_df))
        with col2:
            avg_rating = saved_recipes_df['Rating'].mean() if 'Rating' in saved_recipes_df.columns else 0
            st.metric("Avg Rating", f"{avg_rating:.1f} ⭐")
        with col3:
            if 'Calories' in saved_recipes_df.columns:
                avg_calories = saved_recipes_df['Calories'].mean()
                st.metric("Avg Calories", f"{avg_calories:.0f}")

        st.markdown("### 🍽️ Your Saved Recipes")

        # Render each saved recipe card
        for i, (_, row) in enumerate(saved_recipes_df.iterrows()):
            render_recipe_card(row, i, source="favorite", show_rating=True)
            
            
def get_saved_recipes(person_id: str) -> pd.DataFrame:
    """
    Retrieve saved recipes for a user. Falls back to session state if database query fails or returns nothing.
    
    Args:
        person_id: ID of the user
    
    Returns:
        DataFrame of saved recipes with calories, rating, and timestamp
    """
    if not st.session_state.connected:
        return pd.DataFrame()

    try:
        # Try to fetch from the database
        query = """
        MATCH (p:Person {id: $person_id})-[l:LIKES]->(r:Recipe)
        RETURN r.name AS Recipe, r.calories AS Calories,
               l.rating AS Rating, l.timestamp AS SavedOn
        ORDER BY l.timestamp DESC
        """
        df = st.session_state.connection.execute_query_to_df(query, {"person_id": person_id})
        if not df.empty:
            return df

    except Exception as e:
        st.error(f"Error retrieving saved recipes: {e}")

    # Fallback to session state
    if 'favorite_recipes' not in st.session_state or not st.session_state.favorite_recipes:
        return pd.DataFrame()

    recipe_data = []
    for recipe in st.session_state.favorite_recipes:
        details_query = """
        MATCH (r:Recipe {name: $recipe_name})
        RETURN r.name AS Recipe, r.calories AS Calories
        """
        try:
            df = st.session_state.connection.execute_query_to_df(details_query, {"recipe_name": recipe})
            if not df.empty:
                data = df.iloc[0].to_dict()
                data["Rating"] = 5
                data["SavedOn"] = pd.Timestamp.now()
                recipe_data.append(data)
            else:
                recipe_data.append({
                    "Recipe": recipe,
                    "Calories": None,
                    "Rating": 5,
                    "SavedOn": pd.Timestamp.now()
                })
        except Exception:
            continue

    return pd.DataFrame(recipe_data)
