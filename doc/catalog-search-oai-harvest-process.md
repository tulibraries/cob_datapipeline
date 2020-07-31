Catalog Search OAI Harvest Process
===

## Summary
The current catalog OAI harvesting process is broken up into two distinct but linked processes and their respective DAGs:
* [The Pre Production Catalog OAI Harvest](../cob_datapipeline/prod_catalog_oai_harvest_dag.py)
* [The Production Catalog OAI Harvest](../cob_datapipeline/catalog_preproduction_oai_harvest_dag.py)

Each of these DAGs will harvest independently to a configured collection:
* `catalog_pre_production_oai_harvest`  harvests:
    * from date set in `CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE` variable.
    * until `execution_date` by default.
    * into the collection set in `CATALOG_PRE_PRODUCTION_SOLR_COLLECTION` variable.

* `catalog_preproduction_oai_harvest_dag` harvests:
    * from date set in `CATALOG_PROD_HARVEST_FROM_DATE` variable.
    * until `execution_date` by default.
    * into the collection set in `CATALOG_PRODUCTION_SOLR_COLLECTION` variable

### Production Collection swap process.
When tul_cob gets deployed to production, the `CATALOG_PRODUCTION_SOLR_COLLECTION` will get
[automatically](https://github.com/tulibraries/tul_cob/blob/main/.circleci/update-airflow.sh)
updated to what is deployed.

The following steps MUST be taken when swaping/updating the production collection on tul_cob.
* Before deploying to production we MUST stop the oai pre production and production dags and kill any running harvest.
* After the deployment is complete we MUST update the
  `CATALOG_PROD_HARVEST_FROM_DATE` variable with the
  `CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE` variable value.
* Then we MUST turn the production oai harvest back on.





