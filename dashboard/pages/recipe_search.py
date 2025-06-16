import streamlit as st
from dashboard.dashboard_helpers import render_recipe_card
from dashboard.queries import search_recipes_by_name, search_recipes_with_dietary_filter

def render_search_tab():
    """Render the enhanced recipe search tab with dietary filtering."""
    st.header("🔍 Advanced Recipe Search")

    # Search input
    col1, col2 = st.columns([3, 1])
    with col1:
        search_term = st.text_input(
            "🔍 Search for recipes by name:",
            placeholder="Try 'chocolate cake', 'chicken soup', or 'vegan pasta'...",
            help="Enter any recipe name or keyword"
        )
    with col2:
        search_limit = st.number_input("Results", min_value=1, max_value=50, value=10)

    # Advanced filters in expander
    with st.expander("🎯 Advanced Filters", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 🥗 Dietary Preferences")
            selected_diets = st.multiselect(
                "Filter by diet:",
                options=st.session_state.get('diet_preferences', []),
                help="Only show recipes that match these dietary preferences"
            )
            
            st.markdown("#### 🍽️ Meal Type")
            meal_types = st.session_state.get('meal_types', ['Breakfast', 'Lunch', 'Dinner', 'Drink', 'Other'])
            selected_meal_type = st.selectbox(
                "Meal category:",
                options=["All Types"] + meal_types,
                help="Filter by meal category"
            )
        
        with col2:
            st.markdown("#### ⚠️ Avoid Allergens")
            selected_allergies = st.multiselect(
                "Exclude these allergens:",
                options=st.session_state.get('allergies', []),
                help="Recipes containing these will be excluded"
            )
            
            # Performance indicator
            if selected_diets or selected_allergies:
                st.success("⚡ Using efficient pre-classified filtering!")

    # Use advanced search if filters are applied
    use_advanced = selected_diets or selected_allergies or (selected_meal_type != "All Types")

    # Search results
    if search_term or use_advanced:
        with st.spinner(f"Searching for recipes..."):
            if use_advanced:
                search_results = search_recipes_with_dietary_filter(
                    search_term=search_term,
                    dietary_preferences=selected_diets,
                    allergies=selected_allergies,
                    meal_type=selected_meal_type if selected_meal_type != "All Types" else None,
                    limit=search_limit
                )
            else:
                search_results = search_recipes_by_name(search_term, search_limit)

            if search_results.empty:
                if use_advanced:
                    st.warning("No recipes found matching your criteria. Try adjusting your filters!")
                else:
                    st.warning(f"No recipes found containing '{search_term}'. Try different keywords!")
            else:
                filter_info = []
                if selected_diets:
                    filter_info.append(f"{', '.join(selected_diets)}")
                if selected_allergies:
                    filter_info.append(f"avoiding {', '.join(selected_allergies)}")
                if selected_meal_type != "All Types":
                    filter_info.append(f"{selected_meal_type} meals")
                
                filter_text = f" ({' | '.join(filter_info)})" if filter_info else ""
                st.success(f"Found {len(search_results)} recipes{filter_text}")

                for i, (_, row) in enumerate(search_results.iterrows()):
                    render_recipe_card(row, i, source="search")

    # Quick search suggestions
    if st.session_state.get('search_history'):
        st.markdown("### 📝 Recent Searches")
        cols = st.columns(min(len(st.session_state.search_history), 4))
        for i, search in enumerate(st.session_state.search_history[-4:]):
            with cols[i]:
                if st.button(f"🔍 {search}", key=f"quick_search_{i}"):
                    st.session_state["search_term"] = search
                    st.rerun()
