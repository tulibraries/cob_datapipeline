
# Production Solr Collection Swap Process Steps

This document outlines the expected steps needed to complete the Full Reindex Process to go from exporting data from Alma to having it live in the Production Library Search. This is intended as a checklist for the person responsible for shepherding the process to ensure that the steps that the required manual steps are followed.

These steps are broken down into three sections as these parts can be run sequentially, but they may also be run independently.

## Full Reindex

1. Initiate Alma FTP Export and wait for completion.
1. Trigger the `catalog_move_alma_sftp_to_s3` Dag and wait for completion.
1. Turn off the catalog_preproduction_oai_harvest
1. Set variables required for the full reindex:
   1. Set the variable `ALMASFTP_S3_ORIGINAL_DATA_NAMESPACE` to the folder name the sftp -> s3 dag used as a namespace, which will appear in the dags Success slack post. We can also reuse an existing namespace to use data previously exported by that dag.
   1. Ensure the variable `CATALOG_PRE_PRODUCTION_SOLR_CONFIG` is using the correct version of solr configs
   1. Ensure the variable `PRE_PRODUCTION_COB_INDEX_VERSION` is the correct version of cob index
   1. Ensure the variable `CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE` is set to a date time earlier than the date in the variable `ALMASFTP_S3_ORIGINAL_DATA_NAMESPACE`.
   1. Update the `CATALOG_PRE_PRODUCTION_SOLR_COLLECTION` variable to be None.
1. Trigger `catalog_full_reindex` Dag and wait for completion

## OAI Harvests

1. Ensure the `catalog_preproduction_oai_harvest` Dag is On (may be turned off)
1. Create a PR to `tul_cob` changing CATALOG_COLLECTION in .env to use `CATALOG_PRE_PRODUCTION_SOLR_COLLECTION` and wait for merge and deploy to LibrarySearch QA
1. Initiate Testing of changes on LibrarySearch QA and wait for approval to proceed
1. Create Release of `tul_cob` based on approved changes (usually main) and wait for deploy to Library Search Stage
1. Initiate lightweight review of changes LibrarySearch Stage and wait for approval to proceed.

## Deploy to Production

1. Before the deploy, turn off Pre Production and Production OAI Harvest Dags
2. Approve Deploy To Prod task in CircleCI that was created when the Release and wait for deploy to prod to complete. This deploy [updates the airflow variable](https://github.com/tulibraries/tul_cob/blob/main/.circleci/update-airflow.sh) `CATALOG_PRODUCTION_SOLR_COLLECTION` to the same value as `CATALOG_PRE_PRODUCTION_SOLR_COLLECTION`
3. After the deployment is complete we, update the `CATALOG_PROD_HARVEST_FROM_DATE` variable with the `CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE` variable value.
4. Turn the Production OAI HArvest back on
