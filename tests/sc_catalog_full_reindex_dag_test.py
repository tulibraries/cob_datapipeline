import os
import unittest
import airflow
from cob_datapipeline.sc_catalog_full_reindex_dag import DAG

class TestCatalogFullReindexScDag(unittest.TestCase):
    # Unit Tests for solrcloud catalog full reindex dag file.

    def setUp(self):
        # Method to set up the DAG Class instance for testing.
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))

    def test_dag_loads(self):
        # Unit test that the DAG identifier is set correctly.
        self.assertEqual(DAG.dag_id, "sc_catalog_full_reindex")

    def test_dag_tasks_present(self):
        # Unit test that the DAG instance contains the expected tasks.
        self.assertEqual(self.tasks, [
            "get_num_solr_docs_pre",
            "alma_sftp",
            "create_collection",
            "sc_add_xml_namespaces",
            "sc_ingest_sftp_marc",
            "sc_parse_sftpdump_dates",
            "sc_ingest_marc_boundwith",
            "solr_alias_swap",
            "get_num_solr_docs_post",
            "archive_sftpdump",
            "slack_post_succ"
            ])

    def test_dag_task_order(self):
        # Unit test that the DAG instance contains the expected dependencies.
        expected_task_deps = {
            "alma_sftp": ["get_num_solr_docs_pre"],
            "create_collection": ["alma_sftp"],
            "sc_add_xml_namespaces": ["create_collection"],
            "sc_parse_sftpdump_dates": ["sc_ingest_sftp_marc"],
            "sc_ingest_marc_boundwith": ["sc_parse_sftpdump_dates"],
            "solr_alias_swap": ["sc_ingest_marc_boundwith"],
            "get_num_solr_docs_post": ["solr_alias_swap"],
            "archive_sftpdump": ["sc_parse_sftpdump_dates"],
            "slack_post_succ": ["get_num_solr_docs_post", "archive_sftpdump"],
        }
        for task, upstream_tasks in expected_task_deps.items():
            upstream_list = [up_task.task_id for up_task in DAG.get_task(task).upstream_list]
            self.assertCountEqual(upstream_tasks, upstream_list)

    def test_sc_add_xml_namespaces(self):
        # Test that we inject namespaces and untar files correctly
        airflow_home = airflow.models.Variable.get("AIRFLOW_HOME")
        airflow_harvest_path = airflow.models.Variable.get("ALMASFTP_HARVEST_PATH")
        task = DAG.get_task("sc_add_xml_namespaces")
        expected_bash_path = airflow_home + "/dags/cob_datapipeline/scripts/sc_add_xml_namespaces.sh "
        self.assertEqual(task.env["ALMASFTP_HARVEST_PATH"], os.getcwd() + "/data/sftpdump")
        self.assertEqual(task.bash_command, expected_bash_path)

    def test_sc_ingest_sftp_marc(self):
        # Test that we ingest sftp files
        airflow_home = airflow.models.Variable.get("AIRFLOW_HOME")
        task = DAG.get_task("sc_ingest_sftp_marc")
        expected_bash_path = airflow_home + "/dags/cob_datapipeline/scripts/sc_ingest_marc_multi.sh "
        self.assertEqual(task.env["HOME"], os.getcwd())
        self.assertEqual(task.env["ALMASFTP_HARVEST_PATH"], os.getcwd() + "/data/sftpdump")
        self.assertEqual(task.bash_command, expected_bash_path)

    def test_sc_ingest_marc_boundwith(self):
        # Test that we ingest the boundwith.xml file
        airflow_home = airflow.models.Variable.get("AIRFLOW_HOME")
        task = DAG.get_task("sc_ingest_marc_boundwith")
        expected_bash_path = airflow_home + "/dags/cob_datapipeline/scripts/sc_ingest_marc_multi.sh "
        self.assertEqual(task.env["HOME"], os.getcwd())
        self.assertEqual(task.env["ALMASFTP_HARVEST_PATH"], os.getcwd() + "/data/sftpdump")
        self.assertEqual(task.env["DATA_IN"], "boundwith_merged.xml")
        self.assertEqual(task.bash_command, expected_bash_path)

    def test_archive_sftpdump(self):
        # Test that we move the files into the archive directory
        airflow_home = airflow.models.Variable.get("AIRFLOW_HOME")
        task = DAG.get_task("archive_sftpdump")
        expected_bash_path = airflow_home + "/dags/cob_datapipeline/scripts/sc_sftp_cleanup.sh "
        self.assertEqual(task.env["ALMASFTP_HARVEST_PATH"], os.getcwd() + "/data/sftpdump")
        self.assertEqual(task.env["ALMASFTP_HARVEST_RAW_DATE"], "none")
        self.assertEqual(task.bash_command, expected_bash_path)
