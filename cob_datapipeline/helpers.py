"""Helper methods for dags"""
import logging

from tulflow import process
from tulflow.solr_api_utils import SolrApiUtils
from lxml import etree
from airflow.models import Variable

def determine_most_recent_date(files_list):
    # Expecting list of filenames in the format alma_bibs__2020050503_18470272620003811_new_1.xml
    # OR alma_bibs__boundwith2_2022031420_27396291920003811_new.xml.tar.gz
    date_position = 3
    if len(files_list) > 0 and "boundwith" in files_list[0]:
        date_position = 4

    return max([int(f.split("_")[date_position]) for f in files_list])


def airflow_s3_access_kwargs():
    return {
        "access_id": "{{ conn.AIRFLOW_S3.login }}",
        "access_secret": "{{ conn.AIRFLOW_S3.password }}",
    }


def airflow_s3_env():
    return {
        "AWS_ACCESS_KEY_ID": "{{ conn.AIRFLOW_S3.login }}",
        "AWS_SECRET_ACCESS_KEY": "{{ conn.AIRFLOW_S3.password }}",
    }


def solr_auth_env(conn_id="SOLRCLOUD-WRITER"):
    return {
        "SOLR_AUTH_USER": "{{ conn.get('" + conn_id + "').login or '' }}",
        "SOLR_AUTH_PASSWORD": "{{ conn.get('" + conn_id + "').password or '' }}",
    }


def catalog_safety_check():
    pre_prod_collection = Variable.get("CATALOG_PRE_PRODUCTION_SOLR_COLLECTION", default_var=None)
    prod_collection = Variable.get("CATALOG_PRODUCTION_SOLR_COLLECTION", default_var=None)

    if pre_prod_collection == prod_collection and pre_prod_collection != None:
        raise Exception("The pre production collection cannot be equal to the production collection.")

def catalog_collection_name(configset, cob_index_version):
    default_name = f"{ configset }.{ cob_index_version }" + "-{{ ti.xcom_pull(task_ids='set_s3_namespace') }}"
    return (
        "{% set configured = var.value.get('CATALOG_PRE_PRODUCTION_SOLR_COLLECTION') %}"
        "{% if configured in [None, '', 'None'] %}"
        f"{ default_name }"
        "{% else %}"
        "{{ configured }}"
        "{% endif %}"
    )

def catalog_create_missing_collection(**kwargs):
    conn = kwargs.get("conn")
    sc = SolrApiUtils(
        solr_url=conn.host,
        auth_user=conn.login,
        auth_pass=conn.password,
    )

    coll = kwargs.get("collection")
    if sc.collection_exists(coll):
        logging.info(f"Collection {coll} already present. Skipping collection creation.")
        return

    sc.create_collection(
        collection=coll,
        configset=kwargs.get("configset"),
        replicationFactor=kwargs.get("replication_factor"),
    )

def cleanup_metadata(**op_kwargs):
    """Helper method to clean up DSpace data"""
    access_id = op_kwargs.get("access_id")
    access_secret = op_kwargs.get("access_secret")
    bucket = op_kwargs.get("bucket_name")
    dest_prefix = op_kwargs.get("destination_prefix")
    source_prefix = op_kwargs.get("source_prefix")
    logging.info("Beginning namespace cleanup")

    for s3_key in process.list_s3_content(bucket, access_id, access_secret, source_prefix):
        logging.info("Removing namespaces from: %s", s3_key)
        s3_content = process.get_s3_content(bucket, s3_key, access_id, access_secret)
        parser = etree.XMLParser(remove_blank_text=True)
        collection = etree.fromstring(s3_content, parser)

        # Removes namespaces
        for elem in collection.getiterator():
            elem.tag = etree.QName(elem).localname

        # Removes extra metadata element
        for record in collection:
            for metadata in record.findall("metadata"):
                for child in metadata:
                    metadata.remove(child)
                    metadata.extend(child)

        etree.cleanup_namespaces(collection)
        filename = s3_key.replace(source_prefix, dest_prefix)
        transformed_xml = etree.tostring(collection, encoding="utf-8")
        process.generate_s3_object(transformed_xml, bucket, filename, access_id, access_secret)

def choose_indexing_branch(**context):
    ti = context['ti']
    harvest_data = ti.xcom_pull(task_ids="oai_harvest")
    updates = harvest_data.get("updated", 0) > 0
    deletes = harvest_data.get("deleted", 0) > 0

    if updates and deletes:
        return "updates_and_deletes_branch"
    elif updates:
        return "updates_only_branch"
    elif deletes:
        return "deletes_only_branch"
    else:
        return "no_updates_no_deletes_branch"
