"""Airflow DAG to perform a full re-index tul_cob catalog into Stage SolrCloud Collection."""
from cob_datapipeline.catalog_full_reindex_dag import create_dag
from airflow import DAG # Required or airflow-webserver skips file.

DAG = create_dag("stage", "1.0.0")
globals()[DAG.dag_id] = DAG
