# Data dictionary

Tracks source tables (Redshift) and the medallion tables built from them. Fill in as tables
are confirmed. **Start small.**

## Source schemas (Redshift)

| Schema | Table | Description | Exported? | Notes |
| --- | --- | --- | --- | --- |
| goodrx | TBD | | ☐ | |
| goodrx_raw | TBD | | ☐ | |
| edr | TBD | | ☐ | |
| claim | TBD | | ☐ | |

## Medallion tables

| Layer | Table | Built from | Description |
| --- | --- | --- | --- |
| bronze | TBD | CSV export | 1:1 raw landing |
| silver | TBD | bronze | cleaned/typed/deduped |
| gold | TBD | silver | features + churn label |
