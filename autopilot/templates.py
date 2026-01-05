# autopilot/templates.py
def build_templates(topic: str, city: str, year: str) -> dict:
    TEMPLATES = {
        "comparison": [
            ("Best {topic} in {city} ({year}) – Prices & Reviews",
             "Compare the best {topic} in {city} by price, route and experience. See which option is worth booking."),
            ("Which Is the Best {topic}? Honest {city} Comparison",
             "Not sure which {topic} to choose? We compare top Paris options to help you decide."),
            ("Best {topic} – Compare Routes, Dining & Prices",
             "Looking for the best {topic}? Compare Paris cruises by route, dining and value.")
        ],
        "price": [
            ("{topic} Prices in {city} ({year}) – What You’ll Actually Pay",
             "See typical {topic} prices in {city}, what’s included, and which options offer the best value."),
            ("Best Value {topic} in {city} – Prices & What’s Included",
             "Compare {topic} options by price and inclusions to pick the best-value cruise for your trip."),
            ("Cheap vs Premium {topic} in {city} – Price Comparison",
             "A clear price comparison of {topic} options in {city}, from budget to premium experiences.")
        ],
        "trust": [
            ("Best {topic} in {city} – Reviews & What Travelers Say ({year})",
             "Compare {topic} options in {city} using real review signals and practical differences to choose confidently."),
            ("Is a {topic} Worth It? {city} Reviews & Comparison",
             "See what travelers love (and dislike) about {topic} in {city} and choose the right option."),
            ("Top-Rated {topic} in {city} ({year}) – Honest Picks",
             "A curated comparison of top-rated {topic} in {city}, focused on what matters for your booking.")
        ]
    }

    # materialize format now (same behavior as before)
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
