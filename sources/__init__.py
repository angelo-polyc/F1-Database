from sources.base import BaseSource
from sources.artemis import ArtemisSource
from sources.defillama import DefiLlamaSource
from sources.velo import VeloSource

SOURCES = {
    "artemis": ArtemisSource,
    "defillama": DefiLlamaSource,
    "velo": VeloSource,
}

def get_source(name):
    if name not in SOURCES:
        raise ValueError(f"Unknown source: {name}. Available: {list(SOURCES.keys())}")
    return SOURCES[name]()
