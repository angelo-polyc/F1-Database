from sources.base import BaseSource
from sources.artemis import ArtemisSource
from sources.defillama import DefiLlamaSource
from sources.velo import VeloSource
from sources.alphavantage import AlphaVantageSource
from sources.coingecko import CoinGeckoSource

SOURCES = {
    "artemis": ArtemisSource,
    "defillama": DefiLlamaSource,
    "velo": VeloSource,
    "alphavantage": AlphaVantageSource,
    "coingecko": CoinGeckoSource,
}

def get_source(name):
    if name not in SOURCES:
        raise ValueError(f"Unknown source: {name}. Available: {list(SOURCES.keys())}")
    return SOURCES[name]()
