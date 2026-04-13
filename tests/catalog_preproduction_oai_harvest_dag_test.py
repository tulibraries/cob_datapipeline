"""Unit Tests for the TUL Cob Catalog PreProduction OAI Harvest DAG."""
import os
import unittest

from airflow.models import TaskInstance as TI
from airflow.models.dagrun import DagRun
from airflow.models.renderedtifields import RenderedTaskInstanceFields as RTIF
from airflow.settings import Session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType

from cob_datapipeline.catalog_preproduction_oai_harvest_dag import DAG
from tests.helpers import DEFAULT_DATE

class TestCatalogPreproductionOaiHarvest(unittest.TestCase):
    """Unit Tests for solrcloud catalog preproduction oai harvest dag file."""

    def render_task(self, task_id, logical_date):
        task = DAG.get_task(task_id)
        session = Session()
        run_id = f"test_run_{task_id}_{logical_date.minute}"

        session.add(DagRun(
            dag_id=DAG.dag_id,
            run_id=run_id,
            logical_date=logical_date,
            run_after=logical_date,
            state=DagRunState.RUNNING,
            run_type=DagRunType.MANUAL,
        ))
        session.commit()

        task_instance = TI(
            task=task,
            run_id=run_id,
            dag_version_id=None,
            state=State.SUCCESS,
        )

        return task, RTIF(ti=task_instance).rendered_fields

    def test_index_deletes_oai_marc(self):
        task, rendered_fields = self.render_task(
            "index_deletes_oai_marc",
            DEFAULT_DATE.replace(minute=1),
        )
        expected_bash_path = os.getcwd() + "/dags/cob_datapipeline/scripts/ingest_marc.sh "
        self.assertEqual(task.env["COMMAND"], "delete --suppress")
        self.assertEqual(rendered_fields.get("bash_command"), expected_bash_path)

    def test_index_ingest_oai_marc(self):
        task, rendered_fields = self.render_task(
            "index_updates_oai_marc",
            DEFAULT_DATE.replace(minute=2),
        )
        expected_bash_path = os.getcwd() + "/dags/cob_datapipeline/scripts/ingest_marc.sh "
        self.assertEqual(task.env["COMMAND"], "ingest")
        self.assertEqual(rendered_fields.get("bash_command"), expected_bash_path)

    def test_solr_commit_endpoint_renders(self):
        _, rendered_fields = self.render_task(
            "solr_commit",
            DEFAULT_DATE.replace(minute=3),
        )
        self.assertEqual(
            rendered_fields.get("endpoint"),
            "/solr/FOO/update?commit=true",
        )
