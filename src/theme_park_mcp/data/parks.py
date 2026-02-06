"""
Park definitions for Disney and Universal Florida.
IDs correspond to Queue-Times.com API.
"""

PARKS = {
    # Walt Disney World
    "magic-kingdom": {
        "id": 6,
        "name": "Magic Kingdom",
        "resort": "Walt Disney World",
        "timezone": "America/New_York",
    },
    "epcot": {
        "id": 5,
        "name": "EPCOT",
        "resort": "Walt Disney World",
        "timezone": "America/New_York",
    },
    "hollywood-studios": {
        "id": 7,
        "name": "Hollywood Studios",
        "resort": "Walt Disney World",
        "timezone": "America/New_York",
    },
    "animal-kingdom": {
        "id": 8,
        "name": "Animal Kingdom",
        "resort": "Walt Disney World",
        "timezone": "America/New_York",
    },
    # Universal Orlando
    "universal-studios": {
        "id": 64,
        "name": "Universal Studios Florida",
        "resort": "Universal Orlando",
        "timezone": "America/New_York",
    },
    "islands-of-adventure": {
        "id": 65,
        "name": "Islands of Adventure",
        "resort": "Universal Orlando",
        "timezone": "America/New_York",
    },
}


def get_park_by_slug(slug: str) -> dict | None:
    """Get park info by its slug (e.g., 'magic-kingdom')."""
    return PARKS.get(slug.lower())


def get_park_by_id(park_id: int) -> dict | None:
    """Get park info by its Queue-Times ID."""
    for slug, park in PARKS.items():
        if park["id"] == park_id:
            return {**park, "slug": slug}
    return None


def list_all_parks() -> list[dict]:
    """Return list of all parks with their slugs."""
    return [
        {**park, "slug": slug}
        for slug, park in PARKS.items()
    ]