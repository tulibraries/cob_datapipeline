
# Full Reindex Process Steps

This document outlines the expected steps needed to complete the Full Reindex Process to go from exporting data from Alma to having it live in the Production Library Search. This is intended as a checklist for the person responsible for shepherding theprocess to ensure that the steps that the required manual steps are followed.

1. Initiate Alma FTP Export and wait for completion.
1. Trigger the `catalog_move_alma_sftp_to_s3` Dag and wait for completion.
1. Set variables required for the full reindex:
   1. Set the variable `ALMASFTP_S3_ORIGINAL_DATA_NAMESPACE` to the folder name the sftp -> s3 dag used as a namespace, which will appear in the dags Success slack post. We can also reuse an existing namespace to use data previously exported by that dag.
   2. Ensure the variable `CATALOG_PRE_PRODUCTION_SOLR_CONFIG` is using the correct version of solr configs
   3. Ensure the variable `PRE_PRODUCTION_COB_INDEX_VERSION` is the correct version of cob index
   4. Ensure the variable `CATALOG_PRE_PRODUCTION_HARVEST_FROM_DATE` is set to a date time earlier than the date in the variable `ALMASFTP_S3_ORIGINAL_DATA_NAMESPACE`.
2. Trigger `catalog_full_reindex` Dag and wait for completion
3. Update the variable `CATALOG_PRE_PRODUCTION_SOLR_COLLECTION` to the name of the collection created by the `catalog_full_reindex` dag (s)  
4. Ensure the `catalog_preproduction_oai_harvest` Dag is On (may be turned off)
5. Create a PR to `tul_cob` changing SOLR_URL in .env to use `CATALOG_PRE_PRODUCTION_SOLR_COLLECTION` and wait for merge and deploy to LibrarySearch QA
6. Initiate Testing of changes on LibrarySearch QA and wait for approval to proceed
7. Create Release of `tul_cob` based on approved changes (usually main) and wait for deploy to Library Search Stage
8. Initiate lightweight review of changes LibrarySearch Stage and wait for approval to proceed.
9.  Approve Deploy To Prod task in CircleCI that was created when the Release and wait for deploy to prod to complete. This deploy updates the airflow variable `CATALOG_PRODUCTION_SOLR_COLLECTION` to the same value as `CATALOG_PRE_PRODUCTION_SOLR_COLLECTION`
10. Turn off `catalog_preproduction_oai_harvest` so it doesn’t continuously error out when the pre production and production solr variables match
