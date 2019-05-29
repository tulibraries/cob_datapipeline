import unittest
import airflow
from cob_datapipeline.tul_cob_az_reindex_dag import dag
from cob_datapipeline.globals import AIRFLOW_HOME

class TestTulCobAZReindexDag(unittest.TestCase):

    def setUp(self):
        self.tasks = list(map(lambda t: t.task_id, dag.tasks))

    def test_dag_loads(self):
        self.assertEqual(dag.dag_id, "tul_cob_az_reindex")

    def test_dag_tasks_present(self):
        self.assertEqual(self.tasks, [
            "get_num_solr_docs_pre",
            "git_pull_tulcob",
            "get_database_docs",
            "ingest_databases",
            "get_num_solr_docs_post",
            "slack_post_succ",
            ])

    def test_dag_task_order(self):
        expected_task_deps = {
                "git_pull_tulcob": "get_num_solr_docs_pre",
                "get_database_docs": "git_pull_tulcob",
                "ingest_databases": "get_database_docs",
                "get_num_solr_docs_post": "ingest_databases",
                "slack_post_succ": "get_num_solr_docs_post",
                }

        for task, upstream_task in expected_task_deps.items():
            actual_ut = dag.get_task(task).upstream_list[0].task_id
            self.assertEqual(upstream_task, actual_ut)

    def test_get_database_docs(self):
        task = dag.get_task("get_database_docs")
        self.assertEqual(task.bash_command, AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/get_database_docs.sh ")
        self.assertEqual(task.env["AZ_CLIENT_ID"], "AZ_CLIENT_ID")
        self.assertEqual(task.env["AZ_CLIENT_SECRET"], "AZ_CLIENT_SECRET")

    def test_ingest_dabasses_task(self):
       task = dag.get_task("ingest_databases")
       self.assertEqual(task.bash_command, AIRFLOW_HOME + "/dags/cob_datapipeline/scripts/ingest_databases.sh ")
       self.assertEqual(task.env["SOLR_AZ_URL"], "http://host.docker.internal:8983/solr/az-database")
       self.assertEqual(task.env["AIRFLOW_HOME"], "/usr/local/airflow")
       self.assertEqual(task.env["AIRFLOW_DATA_DIR"], "/usr/local/airflow/data")
       self.assertEqual(task.env["AIRFLOW_LOG_DIR"], "/usr/local/airflow/logs")
