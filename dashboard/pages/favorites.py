import streamlit as st
from dashboard.dashboard_helpers import get_saved_recipes, render_recipe_card


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
            render_recipe_card(row, i, source="favorite", connection=st.session_state.connection, show_rating=True)
