from sources.base import BaseSource
from sources.artemis import ArtemisSource
from sources.defillama import DefiLlamaSource

SOURCES = {
    "artemis": ArtemisSource,
    "defillama": DefiLlamaSource,
}

def get_source(name):
    if name not in SOURCES:
        raise ValueError(f"Unknown source: {name}. Available: {list(SOURCES.keys())}")
    return SOURCES[name]()
