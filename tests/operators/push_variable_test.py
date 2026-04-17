# pylint: disable=missing-docstring,line-too-long
import unittest
from airflow.models import DAG, TaskInstance as TI
from airflow.models.dagrun import DagRun
from airflow.settings import Session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType
from cob_datapipeline.operators import PushVariable
from cob_datapipeline.models import ListVariable
from tests.helpers import DEFAULT_DATE, get_rendered_task_fields


class PushVariableTest(unittest.TestCase):
    def setUp(self):
        ListVariable.delete('foo')

    def test_execute_no_value(self):
        with self.assertLogs('cob_datapipeline.models.list_variable') as log:
            PushVariable(task_id='test_task', name='foo').execute()
            expected_log_message = 'INFO:cob_datapipeline.models.list_variable:Skipping empty value push.'
        self.assertIn(expected_log_message, log.output)

    def test_execute_with_value(self):
        PushVariable(task_id='test_task', name='foo', value=1).execute()
        self.assertEqual(ListVariable.get('foo'), [1])

    def test_execute_with_mulitple_values(self):
        PushVariable(task_id='test_task', name='foo', value=1).execute()
        PushVariable(task_id='test_task', name='foo', value=2).execute()
        PushVariable(task_id='test_task', name='foo', value=3).execute()
        self.assertEqual(ListVariable.get('foo'), [1, 2, 3])

    def test_execute_with_mulitple_equal_values(self):
        PushVariable(task_id='test_task', name='foo', value=1).execute()
        PushVariable(task_id='test_task', name='foo', value=2).execute()
        PushVariable(task_id='test_task', name='foo', value=1).execute()
        self.assertEqual(ListVariable.get('foo'), [1, 2])

    def test_execute_with_mulitple_unique_false(self):
        PushVariable(task_id='test_task', name='foo', value=1).execute()
        PushVariable(task_id='test_task', name='foo', value=2).execute()
        PushVariable(task_id='test_task', name='foo', value=1, unique=False).execute()
        self.assertEqual(ListVariable.get('foo'), [1, 2, 1])

    def test_execute_with_existing_empty_var(self):
        ListVariable.set('foo', '')
        PushVariable(task_id='test_task', name='foo', value=1).execute()
        self.assertEqual(ListVariable.get('foo'), [1])

    def test_execute_with_existing_templated_value(self):
        dag = DAG(dag_id='test_dag', start_date=DEFAULT_DATE)
        run_id = "test_existing_templated_value"
        session = Session()
        session.add(DagRun(
            dag_id=dag.dag_id,
            run_id=run_id,
            logical_date=DEFAULT_DATE,
            run_after=DEFAULT_DATE,
            state=DagRunState.RUNNING,
            run_type=DagRunType.MANUAL,
        ))
        session.commit()

        task = PushVariable(dag=dag, task_id='test_task', name='foo', value='{{task_instance.task_id}}')
        task_instance = TI(task=task, run_id=run_id, dag_version_id=None, state=State.SUCCESS)

        self.assertEqual(get_rendered_task_fields(task_instance).get('value'), 'test_task')
