# autopilot/intent.py
import re

def detect_dominant_intent(df) -> str:
    intent_scores = {"comparison": 0.0, "price": 0.0, "trust": 0.0}

    for _, row in df.iterrows():
        q = str(row["query"]).lower()
        w = float(row["impressions"])

        if re.search(r"\b(best|top|which)\b", q):
            intent_scores["comparison"] += w
        if re.search(r"\b(price|cost|cheap)\b", q):
            intent_scores["price"] += w
        if re.search(r"\b(review|reviews|worth)\b", q):
            intent_scores["trust"] += w

    return max(intent_scores, key=intent_scores.get)
