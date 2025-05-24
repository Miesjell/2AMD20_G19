"""
Person loader for the knowledge graph.
"""

from typing import Dict, Any, Optional
from faker import Faker
import pandas as pd
from neo4j import Driver
from tqdm import tqdm

from .base import DataLoader

faker = Faker('nl_NL')

class PersonLoader(DataLoader):
    """Loader for persons and their diet/allergy relationships."""

    def __init__(self, driver: Optional[Driver] = None):
        """Initialize the person loader."""
        super().__init__(driver)

    def load_data(
        self,
        data: pd.DataFrame,
        sample_size: Optional[int] = None,
        batch_size: int = 25,
    ) -> Dict[str, Any]:
        """
        Load person data into the Neo4j database.

        Args:
            data: DataFrame with person data
            sample_size: Optional sample size to limit processing
            batch_size: Number of records to process in each batch

        Returns:
            Dict with load results
        """
        if not self.driver:
            return {
                "status": "error",
                "error": "Neo4j driver not set. Call set_driver() first.",
            }

        # Sample if needed to avoid processing too much data at once
        if sample_size and len(data) > sample_size:
            data = data.sample(sample_size, random_state=42)

        # Create batches for efficient loading
        batches = self.batch_data(data, batch_size)

        # Cypher query for batch loading persons
        query = """
        UNWIND $persons AS person
        MERGE (p:Person {id: person.id})
        SET p.name = person.name,
            p.recommended_calories = person.recommended_calories,
            p.recommended_protein = person.recommended_protein,
            p.recommended_carbs = person.recommended_carbs,
            p.recommended_fats = person.recommended_fats,
            p.preferred_cuisine = person.preferred_cuisine,
            p.food_aversions = person.food_aversions,
            p.budget = person.budget
        
        WITH p, person
        WHERE person.diet_preference IS NOT NULL
        MERGE (d:DietPreference {name: person.diet_preference})
        MERGE (p)-[:HAS_DIETARY_PREFERENCE]->(d)
        
        WITH p, person
        WHERE person.allergy IS NOT NULL
        MERGE (a:Allergy {name: person.allergy})
        MERGE (p)-[:HAS_ALLERGY]->(a)
        """

        total_processed = 0
        errors = []

        with self.driver.session() as session:
            # Add tqdm progress bar for batches
            for batch_idx, batch in enumerate(
                tqdm(batches, desc="Loading persons", unit="batch")
            ):
                # Prepare data for this batch
                persons = []
                for idx, row in batch.iterrows():
                    try:
                        person = {
                            "name": faker.name(),
                            "id": f"person_{idx}",
                            "diet_preference": self.clean_text(
                                row.get("Dietary_Habits", "")
                            ),
                            "allergy": self.clean_text(row.get("Allergies", "")),
                            "recommended_calories": self._extract_numeric(
                                row, "Recommended_Calories"
                            ),
                            "recommended_protein": self._extract_numeric(
                                row, "Recommended_Protein"
                            ),
                            "recommended_carbs": self._extract_numeric(
                                row, "Recommended_Carbs"
                            ),
                            "recommended_fats": self._extract_numeric(
                                row, "Recommended_Fats"
                            ),
                            "preferred_cuisine": self.clean_text(
                                row.get("Preferred_Cuisine", "")
                            ),
                            "food_aversions": self.clean_text(
                                row.get("Food_Aversions", "")
                            ),
                            "budget": "medium",  # Default budget level
                        }
                        persons.append(person)
                    except Exception as e:
                        errors.append(f"Error processing person {idx}: {str(e)}")

                # Execute the batch
                try:
                    if persons:  # Only run if we have valid persons
                        session.run(query, persons=persons)
                        total_processed += len(persons)
                except Exception as e:
                    errors.append(f"Error in batch {batch_idx}: {str(e)}")

        return {
            "status": "success"
            if not errors
            else "partial_success"
            if total_processed > 0
            else "error",
            "total_processed": total_processed,
            "total_records": len(data),
            "errors": errors[:10] if len(errors) > 10 else errors,  # Limit error output
        }

    def _extract_numeric(self, row: pd.Series, column: str) -> Optional[float]:
        """
        Extract numeric value safely.

        Args:
            row: DataFrame row
            column: Column name

        Returns:
            Float value or None
        """
        if column in row and pd.notna(row[column]):
            try:
                return float(row[column])
            except (ValueError, TypeError):
                pass
        return None
