
# Production Solr Collection Swap Process Steps

This document outlines the expected steps needed to complete the Full Reindex Process to go from exporting data from Alma to having it live in the Production Library Search. This is intended as a checklist for the person responsible for shepherding the process to ensure that the steps that the required manual steps are followed.

These steps are broken down into three sections as these parts can be run sequentially, but they may also be run independently.

## Full Reindex

1. Initiate Alma FTP Export and wait for completion.
1. Trigger the `catalog_move_alma_sftp_to_s3` Dag and wait for completion.
   1. Skip this step if you want to reuse the s3 alma_sftp files from the last run.
3. If needed, move the latest boundwith export file from almasftp to s3 (To be automated).
   1. From almasftp server export AWS credentials (if needed) and then run following command (example):
   1. `aws s3 sync . s3://tulib-airflow-prod/almasftp --exclude '*'  --include 'alma_bibs__boundwith2_2021021914_22144742000003811_new.xml.tar.gz' `
   1. Delete the old boundwith file on s3.
4. Turn off the catalog_preproduction_oai_harvest
5. Set variables required for the full reindex:
   1. Ensure the variable `CATALOG_PRE_PRODUCTION_SOLR_CONFIG` is using the correct version of solr configs
   1. Ensure the variable `PRE_PRODUCTION_COB_INDEX_VERSION` is the correct version of cob index
   1. Update the `CATALOG_PRE_PRODUCTION_SOLR_COLLECTION` variable to be None.
6. Trigger `catalog_full_reindex` Dag and wait for completion

### Caveats and Incidentals
* `cob_datapipeline` deployments to airflow-prod only happen on new releases for that repo (plus verified CI deployment step).
* `ansible-playbook-airflow` prod updates need shepherding from qa to main branch and then a verified deployment step in the CI.

#### If something goes wrong with the full reindex
If something goes wrong with the full reindex and you need to rerun it from the start you can reuse the files that were moved to s3 simply by skipping the `catalog_move_alma_sftp_to_s3` dag run step.

## OAI Harvests

1. Ensure the `catalog_preproduction_oai_harvest` Dag is On (may be turned off)
1. Create a PR to `tul_cob` changing CATALOG_COLLECTION in .env to use `CATALOG_PRE_PRODUCTION_SOLR_COLLECTION` and wait for merge and deploy to LibrarySearch QA
1. Initiate Testing of changes on LibrarySearch QA and wait for approval to proceed
1. Create Release of `tul_cob` based on approved changes (usually main) and wait for deploy to Library Search Stage
1. Initiate lightweight review of changes LibrarySearch Stage and wait for approval to proceed.

## Deploy to Production

1. Before the deploy, turn off Pre Production and Production OAI Harvest Dags
1. Approve Deploy To Prod task in CircleCI that was created when the Release and wait for deploy to prod to complete. This deploy [updates the airflow variable](https://github.com/tulibraries/tul_cob/blob/main/.circleci/update-airflow.sh) `CATALOG_PRODUCTION_SOLR_COLLECTION` to the same value as `CATALOG_PRE_PRODUCTION_SOLR_COLLECTION`
1. After the deployment is complete we, update the `CATALOG_PROD_HARVEST_FROM_DATE` variable with the `CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE` variable value.
1. Turn the Production OAI HArvest back on
