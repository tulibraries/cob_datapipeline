import os
import unittest
import airflow
from cob_datapipeline.sc_catalog_pipeline_dag import DAG

class TestScCatalogPipelineDag(unittest.TestCase):
    # Unit Tests for solrcloud catalog pipeline dag file.

    def setUp(self):
        # Method to set up the DAG Class instance for testing.
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))

    def test_dag_loads(self):
        # Unit test that the DAG identifier is set correctly.
        self.assertEqual(DAG.dag_id, "sc_catalog_pipeline")

    def test_dag_tasks_present(self):
        """Unit test that the DAG instance contains the expected tasks."""
        self.assertEqual(self.tasks, [
            "remote_trigger_message",
            "require_dag_run",
            "set_collection_name",
            "get_num_solr_docs_pre",
            "bw_oai_harvest",
            "list_alma_bw_s3_data",
            "prepare_boundwiths",
            "oai_harvest",
            "index_updates_oai_marc",
            "index_deletes_oai_marc",
            "get_num_solr_docs_post",
            "update_date_variables",
            "slack_post_succ"
            ])

    def test_dag_task_order(self):
        """Unit test that the DAG instance contains the expected dependencies."""
        expected_task_deps = {
            "require_dag_run": ["remote_trigger_message"],
            "get_num_solr_docs_pre": ["require_dag_run"],
            "set_collection_name": ["get_num_solr_docs_pre"],
            "bw_oai_harvest": ["set_collection_name"],
            "list_alma_bw_s3_data": ["bw_oai_harvest"],
            "prepare_boundwiths": ["list_alma_bw_s3_data"],
            "oai_harvest": ["prepare_boundwiths"],
            "index_updates_oai_marc": ["oai_harvest"],
            "index_deletes_oai_marc": ["oai_harvest"],
            "get_num_solr_docs_post": ["index_deletes_oai_marc", "index_updates_oai_marc"],
            "update_date_variables": ["get_num_solr_docs_post"],
            "slack_post_succ": ["update_date_variables"],
        }
        for task, upstream_tasks in expected_task_deps.items():
            upstream_list = [up_task.task_id for up_task in DAG.get_task(task).upstream_list]
            self.assertCountEqual(upstream_tasks, upstream_list)
