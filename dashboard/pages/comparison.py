import streamlit as st
import pandas as pd
import plotly.express as px
import time

from dashboard.dashboard_helpers import (
    search_recipes_by_name,
    compare_recipes,
    get_recipe_complexity_score,
    create_nutrition_radar_chart,
    get_recipe_recommendations_by_similarity,
)


def render_recipe_comparison_tab():
    """Render the recipe comparison tab."""
    st.header("⚖️ Recipe Comparison & Analysis")

    st.markdown("### 🔍 Select Recipes to Compare")
    col1, col2 = st.columns(2)

    # --- Left: Search box ---
    with col1:
        search_term = st.text_input(
            "🔍 Search recipes to compare:",
            placeholder="Search for recipes...",
            key="comparison_search"
        )
        if search_term:
            search_results = search_recipes_by_name(search_term, 5)
            if not search_results.empty:
                st.markdown("**Search Results:**")
                for _, recipe in search_results.iterrows():
                    if st.button(f"➕ Add {recipe['Recipe']}", key=f"add_comparison_{recipe['Recipe']}"):
                        if 'comparison_recipes' not in st.session_state:
                            st.session_state.comparison_recipes = []
                        if recipe['Recipe'] not in st.session_state.comparison_recipes:
                            st.session_state.comparison_recipes.append(recipe['Recipe'])
                            st.rerun()

    # --- Right: Current comparison list ---
    with col2:
        if 'comparison_recipes' in st.session_state and st.session_state.comparison_recipes:
            st.markdown("**Selected for Comparison:**")
            for i, recipe in enumerate(st.session_state.comparison_recipes):
                col_a, col_b = st.columns([3, 1])
                with col_a:
                    st.write(f"• {recipe}")
                with col_b:
                    if st.button("❌", key=f"remove_comparison_{i}"):
                        st.session_state.comparison_recipes.remove(recipe)
                        st.rerun()

            # Comparison trigger
            if len(st.session_state.comparison_recipes) >= 2:
                if st.button("📊 Compare Recipes", use_container_width=True, type="primary"):
                    comparison_df = compare_recipes(st.session_state.comparison_recipes)
                    if not comparison_df.empty:
                        st.markdown("### 📊 Nutritional Comparison")
                        st.dataframe(comparison_df.set_index('Recipe'), use_container_width=True)

                        if len(st.session_state.comparison_recipes) <= 3:
                            st.markdown("### 🕸️ Nutritional Profile Radar")
                            radar_chart = create_nutrition_radar_chart(st.session_state.comparison_recipes)
                            if radar_chart.data:
                                st.plotly_chart(radar_chart, use_container_width=True)

                        st.markdown("### 🎯 Recipe Complexity Analysis")
                        complexity_data = []
                        for recipe in st.session_state.comparison_recipes:
                            complexity = get_recipe_complexity_score(recipe)
                            complexity["Recipe"] = recipe
                            complexity_data.append(complexity)

                        if complexity_data:
                            complexity_df = pd.DataFrame(complexity_data)
                            fig = px.bar(
                                complexity_df,
                                x="Recipe",
                                y="ingredient_count",
                                color="complexity",
                                title="Recipe Complexity by Ingredient Count",
                                labels={"ingredient_count": "Number of Ingredients"},
                                color_discrete_map={
                                    "Simple": "#27ae60",
                                    "Moderate": "#f39c12",
                                    "Complex": "#e74c3c"
                                }
                            )
                            fig.update_layout(
                                plot_bgcolor='white',
                                paper_bgcolor='white',
                                font=dict(color='black'),
                                title_font_color='black'
                            )
                            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Search and select at least 2 recipes to compare their nutritional profiles.")

    # --- Similar recipe suggestions ---
    st.markdown("### 🔍 Find Similar Recipes")
    base_recipe = st.text_input(
        "Enter a recipe name to find similar recipes:",
        placeholder="e.g., 'Chicken Parmesan'",
        key="similarity_search"
    )

    if base_recipe:
        similar_recipes = get_recipe_recommendations_by_similarity(base_recipe, 5)
        if not similar_recipes.empty:
            st.markdown(f"**Recipes similar to '{base_recipe}':**")
            for _, recipe in similar_recipes.iterrows():
                with st.expander(f"🍽️ {recipe['Recipe']} - {recipe['Shared_Ingredients']} shared ingredients"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Calories:** {recipe['Calories']}")
                        st.write(f"**Shared Ingredients:** {recipe['Shared_Ingredients']}")
                    with col2:
                        if st.button(f"🔗 Add to Comparison", key=f"add_similar_{recipe['Recipe']}"):
                            if 'comparison_recipes' not in st.session_state:
                                st.session_state.comparison_recipes = []
                            if recipe['Recipe'] not in st.session_state.comparison_recipes:
                                st.session_state.comparison_recipes.append(recipe['Recipe'])
                                st.success(f"Added {recipe['Recipe']} to comparison!")
                                time.sleep(1)
                                st.rerun()
        else:
            st.warning(f"No similar recipes found for '{base_recipe}'. Try a different recipe name.")
