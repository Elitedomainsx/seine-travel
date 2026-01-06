# autopilot/templates.py
def build_templates(topic: str, city: str, year: str) -> dict:
    """Template pool intentionally small.

    Reason: low-impression pages need faster convergence and less variance.
    Each intent has 2 variants (A/B). Total pool size per intent: 2.
    """
    TEMPLATES = {
        "comparison": [
            ("Best {topic} in {city} ({year}) – Prices & Reviews",
             "Compare the best {topic} in {city} by price, route and experience. See which option is worth booking."),
            ("Which Is the Best {topic}? Honest {city} Comparison",
             "Not sure which {topic} to choose? We compare top Paris options to help you decide."),
        ],
        "price": [
            ("{topic} Prices in {city} ({year}) – What You’ll Actually Pay",
             "See typical {topic} prices in {city}, what’s included, and which options offer the best value."),
            ("Best Value {topic} in {city} – Prices & What’s Included",
             "Compare {topic} options by price and inclusions to pick the best-value cruise for your trip."),
        ],
        "trust": [
            ("Best {topic} in {city} – Reviews & What Travelers Say ({year})",
             "Compare {topic} options in {city} using real review signals and practical differences to choose confidently."),
            ("Is a {topic} Worth It? {city} Reviews & Comparison",
             "See what travelers love (and dislike) about {topic} in {city} and choose the right option."),
        ],
    }

    out = {}
    for intent, pairs in TEMPLATES.items():
        out[intent] = [
            (t.format(topic=topic, city=city, year=year),
             m.format(topic=topic, city=city, year=year))
            for (t, m) in pairs
        ]
    return out


def pick_template_for_intent(intent_templates: list, idx: int):
    title, meta = intent_templates[idx]
    return title, meta
