
# Production Solr Collection Swap Process Steps

This document outlines the expected steps needed to complete the catalog full reindex process from exporting data from Alma to deployment on the production Library Search site. This is intended as a checklist for the person responsible for shepherding the process to ensure that the required manual steps are followed.

These steps are broken down into three sections as these parts can be run sequentially, but they may also be run independently.

## Full Reindex

1. Initiate appropriate Alma SFTP exports via pubishing profiles in Alma and wait for completion.
1. Trigger the `catalog_move_alma_sftp_to_s3` Dag and wait for completion.
   1. Skip this step if you want to reuse the s3 alma_sftp files from the last run.
1. Trigger the `boundwith_move_alma_sftp_to_s3` Dag and wait for completion.
   1. Skip this step if you want to reuse the s3 alma_sftp files from the last run.
1. Turn off the catalog_preproduction_oai_harvest
1. Set the Airflow variables required for the full reindex:
   1. Ensure the variable `CATALOG_PRE_PRODUCTION_SOLR_CONFIG` is using the correct version of solr configs. See the [tul_cob-catalog-solr releases](https://github.com/tulibraries/tul_cob-catalog-solr/releases) to confirm latest version, if needed. 
   1. Ensure the variable `PRE_PRODUCTION_COB_INDEX_VERSION` is the correct version of cob_index. See the  [cob_index releases](https://github.com/tulibraries/cob_index/releases) to confirm latest version, if needed. 
   1. Update the `CATALOG_PRE_PRODUCTION_SOLR_COLLECTION` variable to be None.
1. Trigger `catalog_full_reindex` Dag and wait for completion. 
1. Once a new preproduction Solr collection is created via `catalog_full_reindex`, turn on the `catalog_preproduction_oai_harvest` Dag. 

### Caveats and Incidentals
* `cob_datapipeline` deployments to airflow-prod only happen on new releases for that repo (plus verified CI deployment step).
* `ansible-playbook-airflow` prod updates need shepherding from qa to main branch and then a verified deployment step in the CI.

#### If something goes wrong with the full reindex
If something goes wrong with the full reindex and you need to rerun it from the start you can reuse the files that were moved to s3 simply by skipping the `catalog_move_alma_sftp_to_s3` dag run step.

## Testing

1. When testing a preproduction Solr collection, ensure the `catalog_preproduction_oai_harvest` Dag is on and indexing updates to that collection. 
1. Create a PR to `tul_cob` changing CATALOG_COLLECTION in .env to use the updated Airflow variable `CATALOG_PRE_PRODUCTION_SOLR_COLLECTION` and merge/deploy to Library Search QA. 
1. Test changes on Library Search QA and get approval from any relevant stakeholders/managers for a release.

## Deploy to Production

1. Before proceeding, turn off the Pre Production and Production OAI Harvest Dags
1. Check that the gitlab HELM_VERSION_PROD variable is up-to-date (consult with colleagues if needed). 
1. Create release of `tul_cob` and wait for deployment to complete to Library Search production.
   1. The deployment process will automatically update the Airflow variable `CATALOG_PRODUCTION_SOLR_COLLECTION` to the same value as `CATALOG_PRE_PRODUCTION_SOLR_COLLECTION`
1. After the deployment is complete, update the Airflow variable `CATALOG_PROD_HARVEST_FROM_DATE` to the same value as `CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE` and update the Airflow variable `PROD_COB_INDEX_VERSION` to the same value as `PRE_PRODUCTION_COB_INDEX_VERSION`.
1. Turn the Production OAI Harvest back on.
