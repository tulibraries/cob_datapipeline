"""This module includes utility/helper methods to use in our tests."""

import json
import uuid

from airflow.api_fastapi.execution_api.datamodels.taskinstance import (
    DagRun as DagRunData,
    TIRunContext,
)
from airflow.models import Connection, Variable
from airflow.models.renderedtifields import RenderedTaskInstanceFields
from airflow.settings import Session
from airflow.sdk import timezone
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

DEFAULT_DATE = timezone.datetime(2020, 1, 1)


class _TestConnectionAccessor:
    """DB-backed connection accessor for template rendering in tests."""

    def __getattr__(self, conn_id):
        return self.get(conn_id)

    def get(self, conn_id, default_conn=None):
        conn = Session.query(Connection).filter(Connection.conn_id == conn_id).one_or_none()
        return conn if conn is not None else default_conn


class _TestVariableAccessor:
    """DB-backed variable accessor for template rendering in tests."""

    def __init__(self, deserialize_json):
        self._deserialize_json = deserialize_json

    def __getattr__(self, key):
        value = self.get(key)
        if value is None:
            raise AttributeError(key)
        return value

    def get(self, key, default=None):
        var = Session.query(Variable).filter(Variable.key == key).one_or_none()
        if var is None:
            return default
        value = var.val
        if value is None:
            return default
        if self._deserialize_json:
            return json.loads(value)
        return value


def get_connection(conn_id=None):
    """Can be used as patch for hook.get_connection."""
    return Connection(
        conn_id=conn_id,
        conn_type='http',
        schema='https',
        host='testhost',)


def get_runtime_template_context(task_instance):
    """Build an Airflow 3 runtime template context for a task instance."""
    dag_run = task_instance.get_dagrun()
    task_instance_payload = {
        "id": getattr(task_instance, "id", None) or uuid.uuid4(),
        "task_id": task_instance.task_id,
        "dag_id": task_instance.dag_id,
        "run_id": task_instance.run_id,
        "try_number": task_instance.try_number,
        "dag_version_id": task_instance.dag_version_id or uuid.UUID(int=0),
        "map_index": task_instance.map_index,
        "hostname": getattr(task_instance, "hostname", None),
        "context_carrier": getattr(task_instance, "context_carrier", None),
    }
    runtime_ti = RuntimeTaskInstance.model_construct(
        **task_instance_payload,
        task=task_instance.task,
        _ti_context_from_server=TIRunContext(
            dag_run=DagRunData.model_validate(dag_run, from_attributes=True),
            max_tries=task_instance.max_tries,
            variables=[],
            connections=[],
            xcom_keys_to_clear=[],
        ),
        max_tries=task_instance.max_tries,
        start_date=task_instance.start_date or dag_run.start_date or timezone.utcnow(),
        end_date=task_instance.end_date,
        state=task_instance.state,
    )
    context = runtime_ti.get_template_context()
    context["ti"] = task_instance
    context["task_instance"] = task_instance
    context["var"] = {
        "json": _TestVariableAccessor(deserialize_json=True),
        "value": _TestVariableAccessor(deserialize_json=False),
    }
    context["conn"] = _TestConnectionAccessor()
    return context


def render_task_instance_fields(task_instance, context=None):
    """Render template fields onto the task bound to a task instance."""
    context = context or get_runtime_template_context(task_instance)
    task_instance.task.render_template_fields(context)
    return task_instance.task


def get_rendered_task_fields(task_instance):
    """Return serialized rendered template fields for a task instance."""
    render_task_instance_fields(task_instance)
    return RenderedTaskInstanceFields(
        ti=task_instance,
        render_templates=False,
    ).rendered_fields
