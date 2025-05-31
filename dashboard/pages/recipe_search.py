import streamlit as st
from dashboard.dashboard_helpers import render_recipe_card
from dashboard.queries import search_recipes_by_name

def render_search_tab():
    """Render the recipe search tab."""
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

    # Search results
    if search_term:
        with st.spinner(f"Searching for '{search_term}'..."):
            search_results = search_recipes_by_name(search_term, search_limit)

            if search_results.empty:
                st.warning(f"No recipes found containing '{search_term}'. Try different keywords!")
            else:
                st.success(f"Found {len(search_results)} recipes matching '{search_term}'")

                for i, (_, row) in enumerate(search_results.iterrows()):
                    render_recipe_card(row, i, source="search")

    # Quick search suggestions
    if st.session_state.search_history:
        st.markdown("### 📝 Recent Searches")
        cols = st.columns(min(len(st.session_state.search_history), 4))
        for i, search in enumerate(st.session_state.search_history[-4:]):
            with cols[i]:
                if st.button(f"🔍 {search}", key=f"quick_search_{i}"):
                    st.session_state["search_term"] = search
                    st.rerun()
