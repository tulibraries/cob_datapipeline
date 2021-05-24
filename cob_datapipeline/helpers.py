"""Helper methods for dags"""
import logging
from tulflow import process
from tulflow.solr_api_utils import SolrApiUtils
from lxml import etree
from airflow.models import Variable

def determine_most_recent_date(files_list):
    # Expecting list of filenames in the format alma_bibs__2020050503_18470272620003811_new_1.xml
    return max([int(f.split("_")[3]) for f in files_list])


def catalog_safety_check(**context):
    pre_prod_collection = Variable.get("CATALOG_PRE_PRODUCTION_SOLR_COLLECTION", None)
    prod_collection = Variable.get("CATALOG_PRODUCTION_SOLR_COLLECTION", None)

    if pre_prod_collection == prod_collection and pre_prod_collection != None:
        raise Exception("The pre production collection cannot be equal to the production collection.")

def catalog_collection_name(configset, cob_index_version):
    default_name = f"{ configset }.{ cob_index_version }" + "-{{ ti.xcom_pull(task_ids='set_s3_namespace') }}"
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
