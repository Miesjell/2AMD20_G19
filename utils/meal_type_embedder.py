import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import normalize

class MealTypeEmbedder:
    def __init__(self, threshold=0.3):
        self.meal_types = ["Breakfast", "Lunch", "Dinner", "Desert", "Drink"]
        self.threshold = threshold
        self.model = SentenceTransformer("paraphrase-MiniLM-L6-v2")
        self.embeddings = self.model.encode(
            self.meal_types, convert_to_numpy=True, normalize_embeddings=True
        )

    def classify_bulk(self, texts):
        """
        Classify a list of recipe names/descriptions.
        Returns a list of meal type labels.
        """
        results = []
        to_encode = [text.lower().strip() if isinstance(text, str) else "" for text in texts]
        vectors = self.model.encode(to_encode, convert_to_numpy=True)
        vectors = normalize(vectors)

        similarities = cosine_similarity(vectors, self.embeddings)
        for sim in similarities:
            best_idx = int(np.argmax(sim))
            best_score = sim[best_idx]
            results.append(self.meal_types[best_idx] if best_score >= self.threshold else "Other")

        return results
    
    def classify(self, text: str) -> str:
        return self.classify_bulk([text])[0]
