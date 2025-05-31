import streamlit as st
from dashboard.dashboard_helpers import get_recipe_analytics

from dashboard.visualization import create_calorie_distribution_chart
from dashboard.visualization import create_meal_type_chart
from dashboard.visualization import create_ingredient_popularity_chart

def render_analytics_tab():
    """Render the analytics and insights tab."""
    st.header("📊 Recipe & Nutrition Analytics")

    # Get analytics data
    analytics = get_recipe_analytics()

    if not analytics:
        st.error("Unable to load analytics data. Please check your database connection.")
        return

    # Detailed metrics
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 📈 Database Overview")
        calorie_stats = analytics.get('calorie_stats', {})

        if calorie_stats:
            st.write(f"**Average Calories per Recipe:** {calorie_stats.get('avg_calories', 0):.0f}")
            st.write(f"**Lowest Calorie Recipe:** {calorie_stats.get('min_calories', 0):.0f} calories")
            st.write(f"**Highest Calorie Recipe:** {calorie_stats.get('max_calories', 0):.0f} calories")
            st.write(f"**Recipes with Calorie Data:** {calorie_stats.get('recipes_with_calories', 0)}")

    with col2:
        st.markdown("### 🎯 Your Activity")
        st.write(f"**Favorite Recipes:** {len(st.session_state.favorite_recipes)}")
        st.write(f"**Recent Searches:** {len(st.session_state.search_history)}")
        if st.session_state.search_history:
            st.write(f"**Last Search:** {st.session_state.search_history[-1]}")

    # Visualizations
    st.markdown("### 📊 Data Visualizations")

    viz_col1, viz_col2 = st.columns(2)

    with viz_col1:
        calorie_chart = create_calorie_distribution_chart()
        if calorie_chart.data:
            st.plotly_chart(calorie_chart, use_container_width=True)

    with viz_col2:
        meal_chart = create_meal_type_chart()
        if meal_chart.data:
            st.plotly_chart(meal_chart, use_container_width=True)

    st.markdown("### 🥕 Ingredient Popularity")
    ingredient_chart = create_ingredient_popularity_chart()
    if ingredient_chart.data:
        st.plotly_chart(ingredient_chart, use_container_width=True)
