import os
import unittest
import airflow
from cob_datapipeline.catalog_full_reindex_sc_dag import DAG

class TestCatalogFullReindexScDag(unittest.TestCase):
    """Unit Test for catalog full reindex solrcloud dag file."""

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))
    def test_dag_loads(self):
        """Unit test that the DAG identifier is set correctly."""
        self.assertEqual(DAG.dag_id, "catalog_full_reindex_sc")
    def test_dag_tasks_present(self):
        """Unit test that the DAG instance contains the expected tasks."""
        self.assertEqual(self.tasks, [
            "get_num_solr_docs_pre",
            "almasftp",
            "git_pull_catalog_sc",
            "create_collection",
            "addxmlns",
            "ingest_sftp_marc",
            "parse_sftpdump",
            "ingest_boundwith_merged",
            "solr_alias_swap",
            "get_num_solr_docs_post",
            "archive_sftpdump",
            "slack_post_succ"
            ])
    def test_dag_task_order(self):
        """Unit test that the DAG instance contains the expected dependencies."""
        expected_task_deps = {
            "almasftp": ["get_num_solr_docs_pre"],
            "git_pull_catalog_sc": ["get_num_solr_docs_pre"]
        }
        for task, upstream_tasks in expected_task_deps.items():
            upstream_list = [up_task.task_id for up_task in DAG.get_task(task).upstream_list]
            self.assertCountEqual(upstream_tasks, upstream_list)
    def test_git_pull_catalog_sc_task(self):
        """Test that we pull the correct branch from GitHub"""
        airflow_home = airflow.models.Variable.get("AIRFLOW_HOME")
        task = DAG.get_task("git_pull_catalog_sc")
        expected_bash_path = airflow_home + "/dags/cob_datapipeline/scripts/git_pull_catalog_sc.sh "
        self.assertEqual(task.env["LATEST_RELEASE"], "False")
        self.assertEqual(task.env["GIT_BRANCH"], "qa")
        self.assertEqual(task.bash_command, expected_bash_path)
    def test_sc_addxmlns_task(self):
        """Test that we inject namespaces and untar files correctly"""
        airflow_home = airflow.models.Variable.get("AIRFLOW_HOME")
        airflow_harvest_path = airflow.models.Variable.get("ALMASFTP_HARVEST_PATH")
        task = DAG.get_task("addxmlns")
        expected_bash_path = airflow_home + "/dags/cob_datapipeline/scripts/sc_addxmlns.sh "
        self.assertEqual(task.env["ALMASFTP_HARVEST_PATH"], os.getcwd() + "/data/sftpdump")
        self.assertEqual(task.bash_command, expected_bash_path)
