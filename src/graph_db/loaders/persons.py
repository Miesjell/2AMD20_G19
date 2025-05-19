"""
Person loader for the knowledge graph.
"""
from typing import Dict, Any, List, Optional

import pandas as pd
from neo4j import Driver

from .base import DataLoader


class PersonLoader(DataLoader):
    """Loader for persons and their diet/allergy relationships."""
    
    def __init__(self, driver: Optional[Driver] = None):
        """Initialize the person loader."""
        super().__init__(driver)
    
    def load_data(self, data: pd.DataFrame, 
                  sample_size: Optional[int] = None, 
                  batch_size: int = 50) -> Dict[str, Any]:
        """
        Load persons with their diet preferences and allergies.
        
        Args:
            data: DataFrame with person data
            sample_size: Optional sample size to limit processing
            batch_size: Number of records to process in each batch
            
        Returns:
            Dict with load results
        """
        if not self.driver:
            return {"status": "error", "error": "Neo4j driver not set. Call set_driver() first."}
        
        # Sample if needed
        if sample_size and len(data) > sample_size:
            data = data.sample(sample_size, random_state=42)
        
        # Create batches for efficient loading
        batches = self.batch_data(data, batch_size)
        
        # Cypher query for batch loading
        query = """
        UNWIND $persons AS person
        MERGE (p:Person {id: person.id})
        SET p.recommended_calories = person.calories,
            p.recommended_protein = person.protein,
            p.recommended_carbs = person.carbs,
            p.recommended_fats = person.fats,
            p.preferred_cuisine = person.cuisine,
            p.budget = person.budget
        
        WITH p, person
        WHERE person.diet_preference IS NOT NULL
        MERGE (d:DietPreference {name: person.diet_preference})
        MERGE (p)-[:FOLLOWS]->(d)
        
        WITH p, person
        WHERE person.allergy IS NOT NULL
        MERGE (a:Allergy {name: person.allergy})
        MERGE (p)-[:HAS_ALLERGY]->(a)
        """
        
        total_processed = 0
        errors = []
        
        with self.driver.session() as session:
            for batch_idx, batch in enumerate(batches):
                # Prepare data for this batch
                persons = []
                for idx, row in batch.iterrows():
                    try:
                        person = {
                            "id": f"person_{idx}",
                            "diet_preference": self.clean_text(row["Dietary_Habits"]) if pd.notna(row.get("Dietary_Habits", None)) else None,
                            "allergy": self.clean_text(row["Allergies"]) if pd.notna(row.get("Allergies", None)) else None,
                            "calories": self._extract_numeric(row, "Recommended_Calories"),
                            "protein": self._extract_numeric(row, "Recommended_Protein"),
                            "carbs": self._extract_numeric(row, "Recommended_Carbs"),
                            "fats": self._extract_numeric(row, "Recommended_Fats"),
                            "cuisine": self.clean_text(row["Preferred_Cuisine"]) if pd.notna(row.get("Preferred_Cuisine", None)) else None,
                            "budget": "medium"  # Default budget, can be updated based on actual data
                        }
                        persons.append(person)
                    except Exception as e:
                        errors.append(f"Error processing person {idx}: {str(e)}")
                
                # Execute the batch
                try:
                    if persons:
                        session.run(query, persons=persons)
                        total_processed += len(persons)
                except Exception as e:
                    errors.append(f"Error in batch {batch_idx}: {str(e)}")
        
        return {
            "status": "success" if not errors else "partial_success" if total_processed > 0 else "error",
            "total_processed": total_processed,
            "total_records": len(data),
            "errors": errors[:10] if len(errors) > 10 else errors  # Limit error output
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