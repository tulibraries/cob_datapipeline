from airflow.models import Variable
from tulflow.solr_api_utils import SolrApiUtils
import logging

def determine_most_recent_date(files_list):
    # Expecting list of filenames in the format alma_bibs__2020050503_18470272620003811_new_1.xml
    return max([int(f.split("_")[3]) for f in files_list])


def catalog_safety_check(**context):
    pre_prod_collection = Variable.get("CATALOG_PRE_PRODUCTION_SOLR_COLLECTION", None)
    prod_collection = Variable.get("CATALOG_PRODUCTION_SOLR_COLLECTION", None)

    if pre_prod_collection == prod_collection and pre_prod_collection != None:
        raise Exception("The pre production collection cannot be equal to the production collection.")

def catalog_collection_name(configset, cob_index_version):
    default_name = "{{ configset }}.{{ cob_index_version }}-{{ ti.xcom_pull(task_ids='set_s3_namespace') }}"

    configured_name = Variable.get("CATALOG_PRE_PRODUCTION_SOLR_COLLECTION", default_name)

    if (configured_name == None
            or configured_name == "None"
            or configured_name == ""):
        return default_name


    return configured_name

def catalog_create_missing_collection(**context):
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
