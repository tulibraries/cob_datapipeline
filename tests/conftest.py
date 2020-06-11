# PyTest Configuration file.
import os
import subprocess
import airflow

def pytest_sessionstart():
    """
    Allows plugins and conftest files to perform initial configuration.
    This hook is called for every plugin and initial conftest
    file after command line options have been parsed.
    """
    repo_dir = os.getcwd()
    subprocess.run("airflow initdb", shell=True)
    subprocess.run("mkdir -p dags/cob_datapipeline", shell=True)
    subprocess.run("mkdir -p data", shell=True)
    subprocess.run("mkdir -p logs", shell=True)
    subprocess.run("cp *.py dags/cob_datapipeline", shell=True)
    subprocess.run("cp -r configs dags/cob_datapipeline", shell=True)
    subprocess.run("cp -r scripts dags/cob_datapipeline", shell=True)
    airflow.models.Variable.set("AIRFLOW_HOME", repo_dir)
    airflow.models.Variable.set("AIRFLOW_DATA_BUCKET", "test_bucket")
    airflow.models.Variable.set("AIRFLOW_DATA_DIR", repo_dir + "/data")
    airflow.models.Variable.set("AIRFLOW_LOG_DIR", repo_dir + "/logs")
    airflow.models.Variable.set("AIRFLOW_USER_HOME", repo_dir)
    airflow.models.Variable.set("ALMA_API_KEY", "key")
    airflow.models.Variable.set("ALMAOAI_LAST_HARVEST_FROM_DATE", "none")
    airflow.models.Variable.set("ALMAOAI_LAST_HARVEST_DATE", "none")
    airflow.models.Variable.set("ALMASFTP_HARVEST_PATH", repo_dir + "/data/sftpdump/")
    airflow.models.Variable.set("ALMASFTP_HOST", "127.0.0.1")
    airflow.models.Variable.set("ALMASFTP_PORT", 9229)
    airflow.models.Variable.set("ALMASFTP_S3_ORIGINAL_DATA_NAMESPACE", "2020-06-08")
    airflow.models.Variable.set("ALMASFTP_S3_PREFIX", "almasftp")
    airflow.models.Variable.set("ALMASFTP_USER", "almasftp")
    airflow.models.Variable.set("ALMASFTP_PASSWD", "password")
    airflow.models.Variable.set("ALMASFTP_HARVEST_RAW_DATE", "none")
    airflow.models.Variable.set("AZ_BRANCH", "AZ_BRANCH")
    airflow.models.Variable.set("AZ_PROD_BRANCH", "AZ_BRANCH")
    airflow.models.Variable.set("AZ_QA_BRANCH", "AZ_BRANCH")
    airflow.models.Variable.set("AZ_STAGE_BRANCH", "AZ_BRANCH")
    airflow.models.Variable.set("AZ_CONFIGSET", "tul_cob-az-1")
    airflow.models.Variable.set("AZ_CORE", "az-database")
    airflow.models.Variable.set("AZ_CLIENT_ID", "AZ_CLIENT_ID")
    airflow.models.Variable.set("AZ_CLIENT_SECRET", "AZ_CLIENT_SECRET")
    airflow.models.Variable.set("AZ_DELETE_SWITCH", "--delete")
    airflow.models.Variable.set("AZ_INDEX_SCHEDULE_INTERVAL", "@weekly")
    airflow.models.Variable.set("AZ_SOLR_CONFIG", {"configset": "tul_cob-az-0", "replication_factor": 2}, serialize_json=True)
    airflow.models.Variable.set("AZ_SOLR_CONFIG_QA", {"configset": "tul_cob-az-0", "replication_factor": 2}, serialize_json=True)
    airflow.models.Variable.set("AZ_SOLR_CONFIG_STAGE", {"configset": "tul_cob-az-0", "replication_factor": 2}, serialize_json=True)
    airflow.models.Variable.set("PROD_COB_INDEX_VERSION", "CATALOG_BRANCH")
    airflow.models.Variable.set("CATALOG_PROD_LATEST_RELEASE", "False")
    airflow.models.Variable.set("CATALOG_PRODUCTION_SOLR_COLLECTION", "foo")
    airflow.models.Variable.set("PREPRODUCTION_COB_INDEX_VERSION", "CATALOG_BRANCH")
    airflow.models.Variable.set("CATALOG_QA_LATEST_RELEASE", "False")
    airflow.models.Variable.set("CATALOG_STAGE_BRANCH", "CATALOG_BRANCH")
    airflow.models.Variable.set("CATALOG_STAGE_LATEST_RELEASE", "False")
    airflow.models.Variable.set("CATALOG_CONFIGSET", "tul_cob-web-0")
    airflow.models.Variable.set("CATALOG_CORE", "CATALOG_CORE")
    airflow.models.Variable.set("CATALOG_REPLICATION_FACTOR", 2)
    airflow.models.Variable.set("CATALOG_SOLR_CONFIG", {"configset": "tul_cob-catalog-0", "replication_factor": 2}, serialize_json=True)
    airflow.models.Variable.set("CATALOG_FULL_REINDEX_SOLR_CONFIG_QA", {"configset": "tul_cob-catalog-0", "replication_factor": 2}, serialize_json=True)
    airflow.models.Variable.set("CATALOG_OAI_HAREVEST_SOLR_CONFIG_QA", {"configset": "tul_cob-catalog-0", "replication_factor": 2}, serialize_json=True)
    airflow.models.Variable.set("CATALOG_SOLR_CONFIG_STAGE", {"configset": "tul_cob-catalog-0", "replication_factor": 2}, serialize_json=True)
    airflow.models.Variable.set("CATALOG_FULL_REINDEX_SOLR_CONFIG_PROD", {"configset": "tul_cob-catalog-0", "replication_factor": 2}, serialize_json=True)
    airflow.models.Variable.set("CATALOG_OAI_HARVEST_SOLR_CONFIG_PROD", {"configset": "tul_cob-catalog-0", "replication_factor": 2}, serialize_json=True)
    airflow.models.Variable.set("CATALOG_OAI_HARVEST_SOLR_CONFIG_QA", {"configset": "tul_cob-catalog-0", "replication_factor": 2}, serialize_json=True)
    airflow.models.Variable.set("CATALOG_PRE_PRODUCTION_SOLR_COLLECTION", "FOO")
    airflow.models.Variable.set("SOLR_AUTH_USER", "SOLR_AUTH_USER")
    airflow.models.Variable.set("SOLR_AUTH_PASSWORD", "SOLR_AUTH_PASSWORD")
    airflow.models.Variable.set("WEB_CONTENT_BASE_URL", "WEB_CONTENT_BASE_URL")
    airflow.models.Variable.set("WEB_CONTENT_PROD_BASE_URL", "WEB_CONTENT_BASE_URL")
    airflow.models.Variable.set("WEB_CONTENT_QA_BASE_URL", "WEB_CONTENT_BASE_URL")
    airflow.models.Variable.set("WEB_CONTENT_STAGE_BASE_URL", "WEB_CONTENT_BASE_URL")
    airflow.models.Variable.set("WEB_CONTENT_BASIC_AUTH_USER", "WEB_CONTENT_BASIC_AUTH_USER")
    airflow.models.Variable.set("WEB_CONTENT_BASIC_AUTH_PASSWORD", "WEB_CONTENT_BASIC_AUTH_PASSWORD")
    airflow.models.Variable.set("WEB_CONTENT_BRANCH", "WEB_CONTENT_BRANCH")
    airflow.models.Variable.set("WEB_CONTENT_PROD_BRANCH", "WEB_CONTENT_BRANCH")
    airflow.models.Variable.set("WEB_CONTENT_QA_BRANCH", "WEB_CONTENT_BRANCH")
    airflow.models.Variable.set("WEB_CONTENT_STAGE_BRANCH", "WEB_CONTENT_BRANCH")
    airflow.models.Variable.set("WEB_CONTENT_CONFIGSET", "tul_cob-web-1")
    airflow.models.Variable.set("WEB_CONTENT_CORE", "WEB_CONTENT_CORE")
    airflow.models.Variable.set("WEB_CONTENT_REPLICATION_FACTOR", 2)
    airflow.models.Variable.set("WEB_CONTENT_SOLR_CONFIG", {"configset": "tul_cob-web-2", "replication_factor": 2}, serialize_json=True)
    airflow.models.Variable.set("WEB_CONTENT_SCHEDULE_INTERVAL", "WEB_CONTENT_SCHEDULE_INTERVAL")

    airflow.models.Variable.set("CATALOG_OAI_PUBLISH_INTERVAL", 0)
    airflow.models.Variable.set("CATALOG_PROD_HARVEST_FROM_DATE", "CATALOG_PROD_HARVEST_FROM_DATE")
    airflow.models.Variable.set("CATALOG_QA_HARVEST_FROM_DATE", "CATALOG_QA_HARVEST_FROM_DATE")
    airflow.models.Variable.set("CATALOG_STAGE_HARVEST_FROM_DATE", "CATALOG_STAGE_HARVEST_FROM_DATE")
    airflow.models.Variable.set("CATALOG_HARVEST_UNTIL_DATE", "2020-01-01T00:00:00Z")
    airflow.models.Variable.set("CATALOG_OAI_CONFIG", {"endpoint": "https://temple.alma.exlibrisgroup.com/view/oai/01TULI_INST/request", "included_sets": ["blacklight"], "md_prefix": ""}, serialize_json=True)
    airflow.models.Variable.set("CATALOG_OAI_BW_CONFIG", {"endpoint": "https://temple.alma.exlibrisgroup.com/view/oai/01TULI_INST/request", "included_sets": ["blacklight-bw"], "md_prefix": "marc21"}, serialize_json=True)
    airflow.models.Variable.set("CATALOG_PROD_BRANCH", "CATALOG_PROD_BRANCH")
    airflow.models.Variable.set("CATALOG_QA_BRANCH", "CATALOG_QA_BRANCH")
    airflow.models.Variable.set("CATALOG_STAGE_BRANCH", "CATALOG_STAGE_BRANCH")
    airflow.models.Variable.set("CATALOG_PROD_LATEST_RELEASE", "CATALOG_PROD_LATEST_RELEASE")
    airflow.models.Variable.set("CATALOG_STAGE_LATEST_RELEASE", "CATALOG_STAGE_LATEST_RELEASE")
    airflow.models.Variable.set("CATALOG_QA_LATEST_RELEASE", "CATALOG_QA_LATEST_RELEASE")

    airflow.models.Variable.set("CATALOG_FULL_REINDEX_SOLR_CONFIG", {"configset": "tul_cob-catalog-2", "replication_factor": 2}, serialize_json=True)

    slack = airflow.models.Connection(
        conn_id="AIRFLOW_CONN_SLACK_WEBHOOK",
        conn_type="http",
        host="127.0.0.1/services",
        port=""
    )
    solrcloud = airflow.models.Connection(
        conn_id="SOLRCLOUD",
        conn_type="http",
        host="http://127.0.0.1",
        port="8983",
        login="puppy",
        password="chow"
    )
    solrcloud_writer = airflow.models.Connection(
        conn_id="SOLRCLOUD-WRITER",
        conn_type="http",
        host="127.0.0.1",
        port="8983",
        login="puppy",
        password="chow"
    )
    aws = airflow.models.Connection(
        conn_id="AIRFLOW_S3",
        conn_type="aws",
        login="puppy",
        password="chow"
    )
    airflow_session = airflow.settings.Session()
    airflow_session.add(slack)
    airflow_session.add(solrcloud)
    airflow_session.add(solrcloud_writer)
    airflow_session.add(aws)
    airflow_session.commit()

def pytest_sessionfinish():
    """
    Called after whole test run finished, right before
    returning the exit status to the system.
    """
    subprocess.run("rm -rf dags", shell=True)
    subprocess.run("rm -rf data", shell=True)
    subprocess.run("rm -rf logs", shell=True)
    subprocess.run("yes | airflow resetdb", shell=True)
