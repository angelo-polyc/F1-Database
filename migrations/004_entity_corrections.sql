-- Migration 004: Entity Mapping Corrections
-- Based on user review of entity_mappings.csv

-- =============================================================================
-- PART 1: MERGE DUPLICATE ENTITIES
-- Pattern: Move source mappings from duplicate to primary, then delete duplicate
-- =============================================================================

-- ALT + AltLayer -> keep AltLayer (defillama mapping), add velo:ALT
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'altlayer')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'alt') AND source = 'velo';
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'alt');
DELETE FROM entities WHERE canonical_id = 'alt';

-- Arkham + ARKM -> keep Arkham (defillama mapping), add velo:ARKM
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'arkham')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'arkm') AND source = 'velo';
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'arkm');
DELETE FROM entities WHERE canonical_id = 'arkm';

-- Agora Dollar + AUSD -> keep Agora Dollar, add velo:AUSD
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'agora-dollar')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'ausd') AND source = 'velo';
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'ausd');
DELETE FROM entities WHERE canonical_id = 'ausd';

-- banana + banana-gun -> need to check which exists
-- Chiliz (entity 922 defillama) + CHZ (entity 488 velo) -> keep Chiliz, add velo:CHZ
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'chiliz')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'chz') AND source = 'velo';
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'chz');
DELETE FROM entities WHERE canonical_id = 'chz';

-- DBR + deBridge -> keep deBridge (defillama), add velo:DBR
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'debridge')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'dbr') AND source = 'velo';
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'dbr');
DELETE FROM entities WHERE canonical_id = 'dbr';

-- Fantom (defillama) + FTM (velo) -> keep Fantom, add velo:FTM
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'fantom')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'ftm') AND source = 'velo';
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'ftm');
DELETE FROM entities WHERE canonical_id = 'ftm';

-- Gate + Gate.io -> keep Gate.io (defillama), add velo:GATE  
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'gate-io')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'gate') AND source = 'velo';
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'gate');
DELETE FROM entities WHERE canonical_id = 'gate';

-- Gnosis (defillama) + GNO (velo) -> keep Gnosis, add velo:GNO
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'gnosis')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'gno') AND source = 'velo';
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'gno');
DELETE FROM entities WHERE canonical_id = 'gno';

-- Helium Network (defillama) + HNT (velo) -> keep Helium Network, add velo:HNT
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'helium-network')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'hnt') AND source = 'velo';
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'hnt');
DELETE FROM entities WHERE canonical_id = 'hnt';

-- Metaplex (defillama) + MPLX (velo) -> keep Metaplex, add velo:MPLX
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'metaplex')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'mplx') AND source = 'velo';
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'mplx');
DELETE FROM entities WHERE canonical_id = 'mplx';

-- Movement (defillama) + MOVE (velo) -> keep Movement, add velo:MOVE
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'movement')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'move') AND source = 'velo';
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'move');
DELETE FROM entities WHERE canonical_id = 'move';

-- Osmosis (defillama) + OSMO (velo) -> keep Osmosis, add velo:OSMO
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'osmosis')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'osmo') AND source = 'velo';
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'osmo');
DELETE FROM entities WHERE canonical_id = 'osmo';

-- Polygon (defillama) + POL (artemis+velo) -> keep Polygon, merge mappings
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'polygon')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'pol');
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'pol');
DELETE FROM entities WHERE canonical_id = 'pol';

-- pump.fun (defillama) + PUMP (velo) -> keep pump.fun, add velo:PUMP
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'pump.fun')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'pump') AND source = 'velo';
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'pump');
DELETE FROM entities WHERE canonical_id = 'pump';

-- Raydium (defillama) + RAY (artemis) + RAYDIUM (velo) -> keep Raydium
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE name = 'Raydium')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'raydium');
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE name = 'Raydium')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'ray');
DELETE FROM entity_source_ids WHERE entity_id IN (SELECT entity_id FROM entities WHERE canonical_id IN ('raydium', 'ray') AND name != 'Raydium');
DELETE FROM entities WHERE canonical_id = 'raydium' AND name = 'RAYDIUM';
DELETE FROM entities WHERE canonical_id = 'ray';

-- Ronin (defillama/artemis) + RON (velo) -> keep Ronin
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'ronin')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'ron');
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'ron');
DELETE FROM entities WHERE canonical_id = 'ron';

-- Scroll (artemis) + SCR (velo) -> keep Scroll, add velo:SCR
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'scroll')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'scr') AND source = 'velo';
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'scr');
DELETE FROM entities WHERE canonical_id = 'scr';

-- SushiSwap (defillama) + SUSHI (velo) -> keep SushiSwap, add velo:SUSHI
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'sushiswap')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'sushi') AND source = 'velo';
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'sushi');
DELETE FROM entities WHERE canonical_id = 'sushi';

-- Toncoin (artemis+velo) + TON (defillama) -> keep Toncoin
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'toncoin')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'ton' AND name = 'TON');
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'ton' AND name = 'TON');
DELETE FROM entities WHERE canonical_id = 'ton' AND name = 'TON';

-- TrueFi (defillama) + TRU (velo) -> keep TrueFi, add velo:TRU
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'trufi')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'tru') AND source = 'velo';
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'tru');
DELETE FROM entities WHERE canonical_id = 'tru';

-- Tether Gold: keep one with xaut (defillama), merge
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'xaut')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'tether-gold');
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'tether-gold');
DELETE FROM entities WHERE canonical_id = 'tether-gold';

-- LayerZero (defillama) + ZRO (velo) -> keep LayerZero, add velo:ZRO
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'layerzero')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'zro') AND source = 'velo';
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'zro');
DELETE FROM entities WHERE canonical_id = 'zro';

-- Berachain: keep main one, merge bera
UPDATE entity_source_ids SET entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'berachain')
WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'bera');
DELETE FROM entity_source_ids WHERE entity_id = (SELECT entity_id FROM entities WHERE canonical_id = 'bera');
DELETE FROM entities WHERE canonical_id = 'bera';

-- =============================================================================
-- PART 2: ADD NEW ARTEMIS MAPPINGS
-- =============================================================================

-- Across - add artemis mapping (bridge volume)
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'artemis', 'across'
FROM entities WHERE canonical_id = 'across'
ON CONFLICT (source, source_id) DO NOTHING;

-- Plasma - add artemis mapping
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'artemis', 'plasma'
FROM entities WHERE canonical_id = 'plasma'
ON CONFLICT (source, source_id) DO NOTHING;

-- =============================================================================
-- PART 3: ADD NEW DEFILLAMA MAPPINGS
-- =============================================================================

-- aero -> AERO entity (need to find/create the entity first)
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'aerodrome-finance'
FROM entities WHERE canonical_id = 'aero'
ON CONFLICT (source, source_id) DO NOTHING;

-- aftermath -> Aftermath
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'aftermath-finance'
FROM entities WHERE canonical_id = 'aftermath'
ON CONFLICT (source, source_id) DO NOTHING;

-- aevo -> AEVO
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'aevo'
FROM entities WHERE canonical_id = 'aevo'
ON CONFLICT (source, source_id) DO NOTHING;

-- axl -> Axelar
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'axelar'
FROM entities WHERE canonical_id = 'axl'
ON CONFLICT (source, source_id) DO NOTHING;

-- base -> Base (chain)
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'base'
FROM entities WHERE canonical_id = 'base'
ON CONFLICT (source, source_id) DO NOTHING;

-- berachain -> Berachain
-- Already has defillama mapping

-- blur -> BLUR
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'blur'
FROM entities WHERE canonical_id = 'blur'
ON CONFLICT (source, source_id) DO NOTHING;

-- bob -> BOB
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'bob'
FROM entities WHERE canonical_id = 'bob'
ON CONFLICT (source, source_id) DO NOTHING;

-- celestia -> Celestia (check canonical_id)
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'celestia'
FROM entities WHERE name = 'Celestia' OR canonical_id = 'celestia'
ON CONFLICT (source, source_id) DO NOTHING;

-- chainlink -> Chainlink
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'chainlink'
FROM entities WHERE name = 'Chainlink' OR canonical_id = 'chainlink'
ON CONFLICT (source, source_id) DO NOTHING;

-- crvusd -> crvUSD
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'crvusd'
FROM entities WHERE name = 'crvUSD' OR canonical_id = 'crvusd'
ON CONFLICT (source, source_id) DO NOTHING;

-- cronos -> Cronos
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'cronos'
FROM entities WHERE name = 'Cronos' OR canonical_id = 'cronos'
ON CONFLICT (source, source_id) DO NOTHING;

-- convex finance -> Convex
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'convex-finance'
FROM entities WHERE name = 'Convex' OR canonical_id LIKE '%convex%'
ON CONFLICT (source, source_id) DO NOTHING;

-- DFlow -> DFlow
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'dflow'
FROM entities WHERE name = 'DFlow' OR canonical_id = 'dflow'
ON CONFLICT (source, source_id) DO NOTHING;

-- ether.fi -> ETHFI
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'ether.fi'
FROM entities WHERE name = 'ETHFI' OR symbol = 'ETHFI'
ON CONFLICT (source, source_id) DO NOTHING;

-- Euler -> EUL
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'euler'
FROM entities WHERE name = 'EUL' OR symbol = 'EUL'
ON CONFLICT (source, source_id) DO NOTHING;

-- Extended -> Extended
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'extended'
FROM entities WHERE name = 'Extended' OR canonical_id = 'extended'
ON CONFLICT (source, source_id) DO NOTHING;

-- Ink -> Ink
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'ink'
FROM entities WHERE name = 'Ink' OR canonical_id = 'ink'
ON CONFLICT (source, source_id) DO NOTHING;

-- ICP -> Internet Computer
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'internet-computer'
FROM entities WHERE name = 'Internet Computer' OR canonical_id LIKE '%internet%computer%'
ON CONFLICT (source, source_id) DO NOTHING;

-- Infrared Finance -> IR
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'infrared-finance'
FROM entities WHERE name = 'IR' OR symbol = 'IR'
ON CONFLICT (source, source_id) DO NOTHING;

-- Jito -> JTO
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'jito'
FROM entities WHERE name = 'JTO' OR symbol = 'JTO'
ON CONFLICT (source, source_id) DO NOTHING;

-- Kamino -> KMNO
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'kamino'
FROM entities WHERE name = 'KMNO' OR symbol = 'KMNO'
ON CONFLICT (source, source_id) DO NOTHING;

-- Lighter -> Lighter
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'lighter'
FROM entities WHERE name = 'Lighter' OR canonical_id = 'lighter'
ON CONFLICT (source, source_id) DO NOTHING;

-- Liquity -> LQTY
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'liquity'
FROM entities WHERE name = 'LQTY' OR symbol = 'LQTY'
ON CONFLICT (source, source_id) DO NOTHING;

-- M0 -> M0
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'm0'
FROM entities WHERE name = 'M0' OR canonical_id = 'm0'
ON CONFLICT (source, source_id) DO NOTHING;

-- Ostium -> Ostium
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'ostium-labs'
FROM entities WHERE name = 'Ostium' OR canonical_id LIKE '%ostium%'
ON CONFLICT (source, source_id) DO NOTHING;

-- Paradex -> Paradex
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'paradex'
FROM entities WHERE name = 'Paradex' OR canonical_id = 'paradex'
ON CONFLICT (source, source_id) DO NOTHING;

-- Resolv -> RESOLV
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'resolv'
FROM entities WHERE name = 'RESOLV' OR symbol = 'RESOLV'
ON CONFLICT (source, source_id) DO NOTHING;

-- Safe -> SAFE
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'safe'
FROM entities WHERE name = 'SAFE' OR symbol = 'SAFE'
ON CONFLICT (source, source_id) DO NOTHING;

-- Scroll -> Scroll (chain)
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'scroll'
FROM entities WHERE canonical_id = 'scroll'
ON CONFLICT (source, source_id) DO NOTHING;

-- Sky -> SKY
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'sky'
FROM entities WHERE name = 'SKY' OR symbol = 'SKY'
ON CONFLICT (source, source_id) DO NOTHING;

-- Stargate Finance -> STG
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'stargate'
FROM entities WHERE name = 'STG' OR symbol = 'STG'
ON CONFLICT (source, source_id) DO NOTHING;

-- Maple Finance -> SYRUP
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'maple'
FROM entities WHERE name = 'SYRUP' OR symbol = 'SYRUP'
ON CONFLICT (source, source_id) DO NOTHING;

-- The Graph -> The Graph
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'defillama', 'the-graph'
FROM entities WHERE name = 'The Graph' OR canonical_id LIKE '%graph%'
ON CONFLICT (source, source_id) DO NOTHING;

-- =============================================================================
-- PART 4: ADD NEW VELO MAPPINGS
-- =============================================================================

-- AXL -> Axelar
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'velo', 'AXL'
FROM entities WHERE canonical_id = 'axl'
ON CONFLICT (source, source_id) DO NOTHING;

-- BERA -> Berachain (already merged above)
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'velo', 'BERA'
FROM entities WHERE canonical_id = 'berachain'
ON CONFLICT (source, source_id) DO NOTHING;

-- ZRO -> LayerZero (already handled in merge)

-- LIT -> Lighter
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'velo', 'LIT'
FROM entities WHERE name = 'Lighter' OR canonical_id = 'lighter'
ON CONFLICT (source, source_id) DO NOTHING;

-- PAXG -> Paxos Gold
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'velo', 'PAXG'
FROM entities WHERE name = 'Paxos Gold' OR canonical_id LIKE '%paxos%gold%' OR canonical_id = 'paxg'
ON CONFLICT (source, source_id) DO NOTHING;

-- XPL -> Plasma
INSERT INTO entity_source_ids (entity_id, source, source_id)
SELECT entity_id, 'velo', 'XPL'
FROM entities WHERE canonical_id = 'plasma'
ON CONFLICT (source, source_id) DO NOTHING;

-- =============================================================================
-- CLEANUP: Remove any orphaned entity_source_ids
-- =============================================================================
DELETE FROM entity_source_ids 
WHERE entity_id NOT IN (SELECT entity_id FROM entities);
