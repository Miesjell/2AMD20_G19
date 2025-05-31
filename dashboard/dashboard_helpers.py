import streamlit as st
from typing import Dict, Any, List, Optional
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import time

# Most of these are not used?
from dashboard.queries import (
    find_recipes_for_diet,
    find_recipes_without_allergens,
    find_recipe_ingredients,
    find_recipes_by_meal_type
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
    

def display_metrics_dashboard():
    """Display key metrics in an attractive dashboard format with enhanced styling."""
    analytics = get_recipe_analytics()
    
    # Main metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_recipes = analytics.get('total_recipes', 0)
        st.markdown(f"""
        <div class="dashboard-card floating-card animated-metric">
            <h3 class="gradient-text">Total Recipes</h3>
            <h1 style="color: #333; margin: 0; font-size: 2.5rem;">{total_recipes:,}</h1>
            <p style="color: #666; margin: 0;">Available in database</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        total_ingredients = analytics.get('total_ingredients', 0)
        st.markdown(f"""
        <div class="dashboard-card floating-card animated-metric">
            <h3 class="gradient-text">Unique Ingredients</h3>
            <h1 style="color: #333; margin: 0; font-size: 2.5rem;">{total_ingredients:,}</h1>
            <p style="color: #666; margin: 0;">Ingredient variety</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        avg_calories = analytics.get('calorie_stats', {}).get('avg_calories', 0)
        calorie_color = "#27ae60" if avg_calories < 400 else "#f39c12" if avg_calories < 600 else "#e74c3c"
        st.markdown(f"""
        <div class="dashboard-card floating-card animated-metric">
            <h3 class="gradient-text">Avg Calories</h3>
            <h1 style="color: {calorie_color}; margin: 0; font-size: 2.5rem;">{avg_calories:.0f}</h1>
            <p style="color: #666; margin: 0;">Per recipe</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        fav_count = len(st.session_state.favorite_recipes)
        st.markdown(f"""
        <div class="dashboard-card floating-card animated-metric">
            <h3 class="gradient-text">Your Favorites</h3>
            <h1 style="color: #e74c3c; margin: 0; font-size: 2.5rem;">{fav_count}</h1>
            <p style="color: #666; margin: 0;">Saved recipes</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Additional insights row
    if analytics.get('calorie_stats'):
        insight_col1, insight_col2 = st.columns(2)
        
        with insight_col1:
            min_cal = analytics['calorie_stats'].get('min_calories', 0)
            max_cal = analytics['calorie_stats'].get('max_calories', 0)
            st.markdown(f"""
            <div class="dashboard-card">
                <h4 style="color: #667eea;">📊 Calorie Range</h4>
                <p style="margin: 0.5rem 0;">
                    <strong>Lowest:</strong> {min_cal:.0f} cal<br>
                    <strong>Highest:</strong> {max_cal:.0f} cal<br>
                    <strong>Range:</strong> {max_cal - min_cal:.0f} cal
                </p>
            </div>
            """, unsafe_allow_html=True)
        
        with insight_col2:
            recipes_with_cal = analytics['calorie_stats'].get('recipes_with_calories', 0)
            coverage = (recipes_with_cal / analytics.get('total_recipes', 1)) * 100 if analytics.get('total_recipes', 0) > 0 else 0
            st.markdown(f"""
            <div class="dashboard-card">
                <h4 style="color: #667eea;">🔍 Data Coverage</h4>
                <p style="margin: 0.5rem 0;">
                    <strong>With Calories:</strong> {recipes_with_cal:,} recipes<br>
                    <strong>Coverage:</strong> {coverage:.1f}%<br>
                    <strong>Quality:</strong> {'Excellent' if coverage > 80 else 'Good' if coverage > 60 else 'Fair'}
                </p>
            </div>
            """, unsafe_allow_html=True)
            
            
            
### charts
def create_calorie_distribution_chart() -> go.Figure:
    """Create a histogram of calorie distribution."""
    if not st.session_state.connected:
        return go.Figure()
    
    try:
        query = """
        MATCH (r:Recipe)
        WHERE r.calories IS NOT NULL AND r.calories > 0
        RETURN r.calories AS calories
        """
        
        df = st.session_state.connection.execute_query_to_df(query, {})
        
        if df.empty:
            return go.Figure()
        
        fig = px.histogram(
            df, 
            x='calories', 
            bins=30,
            title='Recipe Calorie Distribution',
            labels={'calories': 'Calories per Recipe', 'count': 'Number of Recipes'},
            color_discrete_sequence=['#667eea']
        )
        
        fig.update_layout(
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(color='black'),
            title_font_color='black'
        )
        
        return fig
    except Exception as e:
        st.error(f"Error creating chart: {e}")
        return go.Figure()

def create_meal_type_chart() -> go.Figure:
    """Create a pie chart of meal type distribution."""
    analytics = get_recipe_analytics()
    meal_distribution = analytics.get('meal_distribution', pd.DataFrame())
    
    if meal_distribution.empty:
        return go.Figure()
    
    fig = px.pie(
        meal_distribution, 
        values='recipe_count', 
        names='meal_type',
        title='Recipe Distribution by Meal Type',
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='black'),
        title_font_color='black'
    )
    
    return fig

def create_ingredient_popularity_chart() -> go.Figure:
    """Create a bar chart of most popular ingredients."""
    analytics = get_recipe_analytics()
    popular_ingredients = analytics.get('popular_ingredients', pd.DataFrame())
    
    if popular_ingredients.empty:
        return go.Figure()
    
    fig = px.bar(
        popular_ingredients.head(10), 
        x='RecipeCount', 
        y='Ingredient',
        orientation='h',
        title='Top 10 Most Popular Ingredients',
        labels={'RecipeCount': 'Number of Recipes', 'Ingredient': 'Ingredient'},
        color='RecipeCount',
        color_continuous_scale='Viridis'
    )
    
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='black'),
        title_font_color='black',
        yaxis={'categoryorder': 'total ascending'}
    )
    
    return fig

def create_nutrition_radar_chart(recipes: List[str]) -> go.Figure:
    """Create a radar chart comparing nutritional profiles of recipes."""
    if not recipes or not st.session_state.connected:
        return go.Figure()
    
    try:
        fig = go.Figure()
        nutrition_categories = ['calories', 'protein', 'fat', 'carbs', 'fiber']

        for recipe in recipes[:3]:  # Limit to 3 recipes for clarity
            nutrition = get_recipe_nutrition_profile(recipe)

            if nutrition:
                values = []
                for category in nutrition_categories:
                    value = nutrition.get(category, 0)
                    if category == 'calories':
                        values.append(min(value / 10, 100))
                    else:
                        values.append(min(value or 0, 100))

                fig.add_trace(go.Scatterpolar(
                    r=values,
                    theta=nutrition_categories,
                    fill='toself',
                    name=recipe[:20] + "..." if len(recipe) > 20 else recipe
                ))

        fig.update_layout(
            polar=dict(
                radialaxis=dict(visible=True, range=[0, 100])
            ),
            showlegend=True,
            title="Nutritional Profile Comparison",
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(color='black'),
            title_font_color='black'
        )

        return fig
    except Exception as e:
        st.error(f"Error creating radar chart: {e}")
        return go.Figure()
            
#### QUERIES #####
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

def search_recipes_by_name(search_term: str, limit: int = 10) -> pd.DataFrame:
    """Search recipes by name with fuzzy matching."""
    if not st.session_state.connected or not search_term:
        return pd.DataFrame()
    
    query = f"""
    MATCH (r:Recipe)
    WHERE toLower(r.name) CONTAINS toLower($search_term)
    RETURN r.name AS Recipe, r.calories AS Calories, 
            r.preparation_description AS Preparation
    ORDER BY r.name
    LIMIT {limit}
    """
    
    # Add to search history
    if search_term not in st.session_state.search_history:
        st.session_state.search_history.append(search_term)
        # Keep only last 10 searches
        st.session_state.search_history = st.session_state.search_history[-10:]
    
    return st.session_state.connection.execute_query_to_df(query, {"search_term": search_term})

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


def get_saved_recipes(person_id: str) -> pd.DataFrame:
    if not st.session_state.connected:
        return pd.DataFrame()

    try:
        query = """
        MATCH (p:Person {id: $person_id})-[l:LIKES]->(r:Recipe)
        RETURN r.name AS Recipe, r.calories AS Calories,
               l.rating AS Rating, l.timestamp AS SavedOn
        ORDER BY l.timestamp DESC
        """

        df = st.session_state.connection.execute_query_to_df(query, {"person_id": person_id})
        if not df.empty:
            return df

        # fallback to session state
        return _build_fallback_favorites()

    except Exception as e:
        st.error(f"Error retrieving saved recipes: {e}")
        return _build_fallback_favorites()


def _build_fallback_favorites() -> pd.DataFrame:
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
    
    
def get_recipe_nutrition_profile(recipe_name: str) -> Dict[str, Any]:
    """Get detailed nutritional profile for a recipe."""
    connection = st.session_state.connection
    if not st.session_state.connected:
        return {}

    try:
        query = """
        MATCH (r:Recipe {name: $recipe_name})
        RETURN r.calories AS calories, 
               r.protein AS protein,
               r.fat AS fat, 
               r.carbohydrates AS carbs,
               r.fiber AS fiber,
               r.sugar AS sugar,
               r.sodium AS sodium
        """
        df = connection.execute_query_to_df(query, {"recipe_name": recipe_name})
        return df.iloc[0].to_dict() if not df.empty else {}
    except Exception as e:
        st.error(f"Error getting nutrition profile: {e}")
        return {}

def compare_recipes(recipe_names: List[str]) -> pd.DataFrame:
    """Compare nutritional profiles of multiple recipes."""
    if not st.session_state.connected or len(recipe_names) < 2:
        return pd.DataFrame()

    try:
        comparison_data = []

        for recipe_name in recipe_names:
            nutrition = get_recipe_nutrition_profile(recipe_name)
            if nutrition:
                nutrition['Recipe'] = recipe_name
                comparison_data.append(nutrition)

        return pd.DataFrame(comparison_data)
    except Exception as e:
        st.error(f"Error comparing recipes: {e}")
        return pd.DataFrame()
    
    
def generate_meal_plan(days: int, meals_per_day: int) -> pd.DataFrame:
    """Generate a balanced meal plan.

    Args:
        days: Number of days in the meal plan
        meals_per_day: Number of meals per day (max 3: Breakfast, Lunch, Dinner)
        connection: Neo4j connection object

    Returns:
        DataFrame containing the meal plan
    """
    connection = st.session_state.connection
    
    if not st.session_state.connected:
        return pd.DataFrame()

    try:
        meal_types = ["Breakfast", "Lunch", "Dinner"][:meals_per_day]
        meal_plan = []

        for day in range(1, days + 1):
            for meal_type in meal_types:
                query = """
                MATCH (r:Recipe)-[:IS_TYPE]->(m:MealType {name: $meal_type})
                WITH r, rand() AS random
                ORDER BY random
                RETURN r.name AS Recipe, r.calories AS Calories
                LIMIT 1
                """
                recipe_df = connection.execute_query_to_df(query, {"meal_type": meal_type})

                if not recipe_df.empty:
                    meal_plan.append({
                        'Day': f"Day {day}",
                        'Meal_Type': meal_type,
                        'Recipe': recipe_df.iloc[0]['Recipe'],
                        'Calories': recipe_df.iloc[0]['Calories']
                    })

        return pd.DataFrame(meal_plan)
    except Exception as e:
        st.error(f"Error generating meal plan: {e}")
        return pd.DataFrame()
    
    
def get_recipe_recommendations_by_similarity(base_recipe: str, limit: int = 5) -> pd.DataFrame:
    """Find recipes similar to a given recipe based on shared ingredients."""
    connection = st.session_state.connection
    
    if not st.session_state.connected:
        return pd.DataFrame()

    try:
        query = f"""
        MATCH (base:Recipe {{name: $base_recipe}})-[:CONTAINS]->(shared:Ingredient)<-[:CONTAINS]-(similar:Recipe)
        WHERE base <> similar
        WITH similar, count(shared) AS shared_ingredients
        ORDER BY shared_ingredients DESC
        RETURN similar.name AS Recipe, 
               similar.calories AS Calories,
               shared_ingredients AS Shared_Ingredients
        LIMIT {limit}
        """
        return connection.execute_query_to_df(query, {"base_recipe": base_recipe})
    except Exception as e:
        st.error(f"Error finding similar recipes: {e}")
        return pd.DataFrame()

def find_recipes_with_ingredient(ingredient_name: str, limit: int = 10) -> pd.DataFrame:
    """Find recipes containing a specific ingredient."""
    connection = st.session_state.connection
    
    if not st.session_state.connected:
        return pd.DataFrame()

    query = """
    MATCH (i:Ingredient {name: $ingredient_name})<-[:CONTAINS]-(r:Recipe)
    RETURN r.name AS Recipe, 
           r.calories AS Calories, 
           r.preparation_description AS Preparation
    ORDER BY r.calories ASC
    LIMIT $limit
    """

    return connection.execute_query_to_df(
        query,
        {"ingredient_name": ingredient_name, "limit": limit}
    )

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