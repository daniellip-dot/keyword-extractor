"""TF-IDF keyword extraction."""

import re
from sklearn.feature_extraction.text import TfidfVectorizer

BOILERPLATE_STOPS = {
    "limited", "ltd", "plc", "llp", "company", "group", "holdings",
    "services", "solutions", "management", "uk", "united", "kingdom",
    "england", "scotland", "wales", "home", "about", "contact",
    "welcome", "team", "news", "blog", "privacy", "cookies", "terms",
    "conditions", "website", "online", "call", "email", "phone",
    "address", "click", "here", "more", "learn", "read", "view",
    "please", "today", "get", "our", "your", "their", "they", "this",
    "that", "these", "those", "which", "who", "what", "when", "where",
    "how", "also", "would", "could", "just", "like", "new", "use",
    "make", "find", "need", "know", "want", "look", "good", "great",
    "best", "right", "well", "way", "work", "offer", "provide",
    "ensure", "help", "can", "will", "may", "including", "based",
    "range", "wide", "fully", "years", "experience", "professional",
    "quality", "high", "free", "quote", "enquiry", "enquiries",
    "site", "page", "copyright", "reserved", "rights", "all",
}


def extract_keywords(weighted_text: str, top_n: int = 20) -> list[str]:
    """Extract top keywords using TF-IDF."""
    if not weighted_text or len(weighted_text.strip()) < 20:
        return []

    # Custom stop words: sklearn english + boilerplate
    from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
    all_stops = list(ENGLISH_STOP_WORDS | BOILERPLATE_STOPS)

    try:
        vectorizer = TfidfVectorizer(
            ngram_range=(1, 3),
            stop_words=all_stops,
            max_features=200,
            min_df=1,
            token_pattern=r"\b[a-zA-Z][a-zA-Z\-]{2,}\b",
        )
        tfidf = vectorizer.fit_transform([weighted_text])
        feature_names = vectorizer.get_feature_names_out()
        scores = tfidf.toarray()[0]

        ranked = sorted(
            zip(feature_names, scores), key=lambda x: -x[1]
        )
        return [term for term, score in ranked[:top_n] if score > 0]
    except ValueError:
        return []
