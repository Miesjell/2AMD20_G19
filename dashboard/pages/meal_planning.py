import streamlit as st
import pandas as pd


def render_meal_planning_tab():
    """Render the meal planning tab with weekly meal plans."""
    st.header("📅 Smart Meal Planning")
    # Meal planning configuration
    col1, col2, col3 = st.columns(3)

    with col1:
        planning_days = st.slider(
            "📆 Planning Period (days)",
            min_value=1,
            max_value=14,
            value=7,
            help="How many days do you want to plan for?"
        )

    with col2:
        meals_per_day = st.selectbox(
            "🍽️ Meals per Day",
            options=[1, 2, 3, 4],
            index=2,
            help="How many meals do you want planned each day?"
        )

    with col3:
        target_calories = st.number_input(
            "🎯 Daily Calorie Target",
            min_value=1000,
            max_value=4000,
            value=2000,
            step=100,
            help="Your daily calorie goal"
        )

    # Generate meal plan button
    if st.button("🤖 Generate Smart Meal Plan", use_container_width=True, type="primary"):
        with st.spinner("Creating your personalized meal plan..."):
            meal_plan_df = generate_meal_plan(planning_days, meals_per_day)

            if meal_plan_df.empty:
                st.error("Unable to generate meal plan. Please check your database connection.")
                return

            st.session_state.current_meal_plan = meal_plan_df
            st.success(f"✅ Generated {planning_days}-day meal plan with {len(meal_plan_df)} meals!")

    # Display current meal plan
    if 'current_meal_plan' in st.session_state and not st.session_state.current_meal_plan.empty:
        meal_plan_df = st.session_state.current_meal_plan

        # Calculate daily calories
        daily_calories = meal_plan_df.groupby('Day')['Calories'].sum().reset_index()
        avg_daily_calories = daily_calories['Calories'].mean()

        # Meal plan overview
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📊 Avg Daily Calories", f"{avg_daily_calories:.0f}")
        with col2:
            st.metric("🍽️ Total Meals", len(meal_plan_df))
        with col3:
            calories_diff = avg_daily_calories - target_calories
            st.metric("🎯 vs Target", f"{calories_diff:+.0f} cal")

        # Display meal plan in an organized way
        st.markdown("### 📋 Your Meal Plan")

        for day in meal_plan_df['Day'].unique():
            with st.expander(f"📅 {day} - {meal_plan_df[meal_plan_df['Day'] == day]['Calories'].sum():.0f} calories", expanded=False):
                day_meals = meal_plan_df[meal_plan_df['Day'] == day]
                cols = st.columns(len(day_meals))
                for i, (_, meal) in enumerate(day_meals.iterrows()):
                    with cols[i]:
                        st.markdown(f"""
                        <div class="recipe-card" style="margin: 0.5rem; padding: 1rem;">
                            <h4>{meal['Meal_Type']}</h4>
                            <h5>{meal['Recipe']}</h5>
                            <p>🔥 {meal['Calories']} calories</p>
                        </div>
                        """, unsafe_allow_html=True)

        # Download meal plan option
        if st.button("📥 Export Meal Plan", help="Download your meal plan as CSV"):
            csv = meal_plan_df.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"meal_plan_{planning_days}_days.csv",
                mime="text/csv"
            )
            
            
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
