Catalog Search OAI Harvest Process
===

## Summary
The current catalog OAI harvesting process is broken up into two distinct but linked processes and their respective DAGs:
* [The Pre Production Catalog OAI Harvest](../cob_datapipeline/catalog_preproduction_oai_harvest_dag.py)
* [The Production Catalog OAI Harvest](../cob_datapipeline/catalog_production_oai_harvest_dag.py)

Each of these DAGs will harvest independently to a configured collection:
* `catalog_pre_production_oai_harvest`  harvests:
    * from date set in `CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE` variable.
    * until `data_interval_start` by default.
    * into the collection set in `CATALOG_PRE_PRODUCTION_SOLR_COLLECTION` variable.

* `catalog_production_oai_harvest_dag` harvests:
    * from date set in `CATALOG_PROD_HARVEST_FROM_DATE` variable.
    * until `data_interval_start` by default.
    * into the collection set in `CATALOG_PRODUCTION_SOLR_COLLECTION` variable

Additional considerations for the OAI Harvest pocess are detailed in the [Deploy to Production](production-solr-collection-swap-process.md#deploy-to-production) section of the [Production Solr Collection Swap Process](production-solr-collection-swap-process.md).
