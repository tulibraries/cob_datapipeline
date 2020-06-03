from airflow.models import Variable

def safety_check(**context):
    pre_prod_collection = Variable.get("CATALOG_PRE_PRODUCTION_SOLR_COLLECTION")
    prod_collection = Variable.get("CATALOG_PRODUCTION_SOLR_COLLECTION")

    if pre_prod_collection == prod_collection:
        raise Exception("The pre production collection cannot be equal to the production collection.")
