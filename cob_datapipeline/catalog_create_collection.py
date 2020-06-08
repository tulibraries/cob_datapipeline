import logging
from tulflow.solr_api_utils import SolrApiUtils
from airflow.models import Variable

def collection_name(configset, cob_index_version):
    default_name = "{{ configset }}.{{ cob_index_version }}-{{ ti.xcom_pull(task_ids='set_s3_namespace') }}"

    configured_name = Variable.get("CATALOG_PRE_PRODUCTION_SOLR_COLLECTION", default_name)

    if (configured_name == None
            or configured_name == "None"
            or configured_name == ""):
        return default_name


    return configured_name

def create_missing_collection(**context):
    conn = context.get("conn")
    sc = SolrApiUtils(
            solr_url=conn.host,
            auth_user=conn.login,
            auth_pass=conn.password,
            )

    coll = context.get("collection")
    if sc.collection_exists(coll):
        logging.info(f"Collection {coll} already present. Skipping collection creation.")
        return

    sc.create_collection(
            collection=coll,
            configset=context.get("configset"),
            replicationFactor=context.get("replication_factor"),
            )
