import streamlit as st
import pandas as pd
from food_kg.kg import FoodKnowledgeGraph

st.set_page_config(page_title="📖 Recipe Data Viewer", layout="wide")
st.title("🍲 Recipe DataFrame Viewer")

@st.cache_data(show_spinner="Loading data...")
def load_recipe_df(uri, user, password, data_dir, sample_recipes):
    # Initialize and connect
    kg = FoodKnowledgeGraph(uri=uri, user=user, password=password)
    kg.connect()

    results = kg.load_data(
        data_dir=data_dir,
        sample_recipes=sample_recipes,
        sample_persons=0  # skip person loading
    )

    kg.close()

    # Merge both sources (JSON + Parquet) if available
    dfs = []

    for key in ["recipes_json", "recipes_parquet"]:
        if key in results and "df" in results[key]:
            dfs.append(results[key]["df"])

    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()

# Sidebar settings
st.sidebar.header("Neo4j Configuration")
uri = st.sidebar.text_input("Bolt URI", value="bolt://localhost:7687")
user = st.sidebar.text_input("Username", value="neo4j")
password = st.sidebar.text_input("Password", type="password", value="password")
data_dir = st.sidebar.text_input("Path to data directory", value="data/")
sample_recipes = st.sidebar.slider("Number of recipes to load", 10, 1000, 100)

if st.sidebar.button("Load and Display Recipes"):
    with st.spinner("Loading recipes from Neo4j and preprocessing..."):
        df = load_recipe_df(uri, user, password, data_dir, sample_recipes)

        if df.empty:
            st.warning("⚠️ No recipes were loaded or found in the data files.")
        else:
            st.success(f"✅ Loaded {len(df)} recipes!")
            st.dataframe(df.sample(n=50), use_container_width=True)
