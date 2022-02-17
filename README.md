# islandora-preservica
A set of functions written in Python to modify exported assets from Islandora as bags using Islandora BagIt into assets ready for Preservica ingest using OPEX incremental ingest and the PAX structure

Islandora version 7.x-1.13
Requires Islandora BagIt for export of bags from Islandora

Preservica Cloud Edition used for ingest of assets
Workflow for ingest is OPEX Incremental using WinSCP
Structure for assets is PAX

Python library used for bag manipulation:
bdbag - https://github.com/fair-research/bdbag
for validation and reversion of bags into directories
