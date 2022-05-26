# pylint: disable=missing-docstring,line-too-long
import unittest
from airflow.models import DAG
from airflow.models.taskinstance import TaskInstance as TI
from airflow.models.renderedtifields import RenderedTaskInstanceFields as RTIF
from cob_datapipeline.operators import PushVariable
from cob_datapipeline.models import ListVariable
from tests.helpers import DEFAULT_DATE
from airflow.utils.state import State
from airflow import settings


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
        session = settings.Session();
        dag = DAG(dag_id='test_dag', start_date=DEFAULT_DATE)
        with dag:
            dr = dag.create_dagrun(
                    run_id="test_existing_templated_value", state=State.SUCCESS,
                    execution_date=DEFAULT_DATE, start_date=DEFAULT_DATE,
                    session=session,
                    )

            task = PushVariable(dag=dag, task_id='test_task', name='foo', value='{{task_instance.task_id}}')
            task_instance = TI(task=task, run_id=dr.run_id, state=State.SUCCESS)
            rendered_ti_fields = RTIF(ti=task_instance)

            self.assertEqual(rendered_ti_fields.rendered_fields.get('value'), 'test_task')
