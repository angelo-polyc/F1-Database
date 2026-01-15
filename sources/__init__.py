from sources.base import BaseSource
from sources.artemis import ArtemisSource

SOURCES = {
    "artemis": ArtemisSource,
}

def get_source(name):
    if name not in SOURCES:
        raise ValueError(f"Unknown source: {name}. Available: {list(SOURCES.keys())}")
    return SOURCES[name]()
