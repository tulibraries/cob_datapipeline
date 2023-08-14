# Production Solr Collection Updates for AZ and Web Content

This document outlines the expected steps needed to complete the Full Reindex Process to go from exporting data from AZ Databases or the Library website to having it live in Production Library Search. This is intended as a checklist for the person responsible for shepherding the process to ensure that the required manual steps are followed.

There are separate dags for qa and production instances of both az and web content.  The directions below are for reindexing in the production enviroment. 

## Setting required variables
Set variables required for the full reindex:
   1. Ensure the variable `AZ_SOLR_CONFIG` is using the correct version of solr configs (the last part of the configset should match the latest release version in tul_cob-az-solr)
   1. Ensure the variable `WEB_CONTENT_SOLR_CONFIG` is using the correct version of solr configs (the last part of the configset should match the latest release version in tul_cob-web-solr)

## Full Reindex
These dags both run on a schedule, so it is not necesary to trigger a dag run. The prod_az_index dag runs weekly, while the prod_web_content_reindex dag runs on a daily schedule. Manual dag runs can always be triggered if you want to ensure that everything is running as expected with the variable updates.
1. Trigger the `prod_az_reindex` Dag and wait for completion.
1. Trigger the `prod_web_content_reindex` Dag and wait for completion.

## Deploy to Production
1. Create a PR to `tul_cob` changing AZ_COLLECTION and WEB_CONTENT_COLLECTION  in .env to use the latest solr aliases.
   For example, tul_cob-web-4-prod. (The number should match the latest version of the solr repository.)
1. Wait for merge and deploy to LibrarySearch QA
1. Initiate Testing of changes on LibrarySearch QA and wait for approval to proceed
1. Create Release of `tul_cob` based on approved changes (usually main) and wait for deploy to Library Search production.
