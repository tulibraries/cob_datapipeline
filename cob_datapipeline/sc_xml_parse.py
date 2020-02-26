"""Python Functions to Parse Boundwith Files for Child Identification & XML Updates."""
import ast
import csv
from datetime import datetime
import io
import logging
import re
from lxml import etree
import pandas
from tulflow import process
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable

NS = {
    "marc21": "http://www.loc.gov/MARC21/slim",
    "oai": "http://www.openarchives.org/OAI/2.0/"
    }

def update_variables(**kwargs):
    """For a given dictionary of variables, update them in Airflow."""
    update_dict = kwargs.get("UPDATE")
    for key, value in update_dict.items():
        Variable.set(key, value)

def parse_dates_from_s3_keys(**kwargs):
    """Given a list of Alma SFTP S3 Keys, Parse Names & return Earliest Date."""
    prefix = kwargs.get("SOURCE_FOLDER")

    keys = [key for key in ast.literal_eval(kwargs.get("S3_KEYS")) if key.startswith(prefix)]
    datestamps = set()
    for key in keys:
        datestamp_re = re.compile(r"alma_bibs__(.*)_.*")
        rxresults = datestamp_re.search(key)
        if rxresults is not None:
            datestamp = rxresults.group(1)
            datestamps.add(datestamp)

    oldest_datestamp = min(datestamps)
    return datetime.strptime(oldest_datestamp, "%Y-%m-%dT%H:%M:%Sz")

def prepare_boundwiths(**kwargs):
    """Grab Boundwith Files and Generate Child Lookup."""
    access_id = kwargs.get("AWS_ACCESS_KEY_ID")
    access_secret = kwargs.get("AWS_SECRET_ACCESS_KEY")
    bucket = kwargs.get("BUCKET")
    bw_prefix = kwargs.get("SOURCE_FOLDER")
    lookup_prefix = kwargs.get("DEST_FOLDER")

    bw_keys = [key for key in ast.literal_eval(kwargs.get("S3_KEYS")) if key.startswith(bw_prefix)]
    csv_in_mem = io.StringIO()
    lookup_csv = csv.DictWriter(csv_in_mem, fieldnames=["child_id", "parent_id", "parent_xml"])
    lookup_csv.writeheader()

    logging.info("Starting to iterate over S3 Boundwith objects")
    for key in bw_keys:
        logging.info("Loading s3 key %s", key)
        source_obj = process.get_s3_content(bucket, key, access_id, access_secret)
        source_data = process.expand_alma_sftp_tarball(key, source_obj)
        source_xml = process.add_marc21xml_root_ns(source_data)
        for record in source_xml.findall("{http://www.loc.gov/MARC21/slim}record"):
            boundwith_record_process(record, lookup_csv)
    process.generate_s3_object(
        csv_in_mem.getvalue(),
        bucket,
        lookup_prefix,
        access_id,
        access_secret
    )

def prepare_oai_boundwiths(**kwargs):
    """Grab Boundwith Files and Generate Child Lookup."""
    access_id = kwargs.get("AWS_ACCESS_KEY_ID")
    access_secret = kwargs.get("AWS_SECRET_ACCESS_KEY")
    bucket = kwargs.get("BUCKET")
    bw_prefix = kwargs.get("SOURCE_FOLDER")
    lookup_prefix = kwargs.get("DEST_FOLDER")

    bw_keys = [key for key in ast.literal_eval(kwargs.get("S3_KEYS")) if key.startswith(bw_prefix)]
    csv_in_mem = io.StringIO()
    lookup_csv = csv.DictWriter(csv_in_mem, fieldnames=["child_id", "parent_id", "parent_xml"])
    lookup_csv.writeheader()

    logging.info("Starting to iterate over S3 Boundwith objects")
    for key in bw_keys:
        logging.info("Loading s3 key %s", key)
        source_obj = process.get_s3_content(bucket, key, access_id, access_secret)
        source_xml = process.add_marc21xml_root_ns(source_obj)
        for record in source_xml.xpath("oai:record/oai:metadata/marc21:record", namespaces=NS):
            boundwith_record_process(record, lookup_csv)
    process.generate_s3_object(
        csv_in_mem.getvalue(),
        bucket,
        lookup_prefix,
        access_id,
        access_secret
    )

def boundwith_record_process(record, lookup_csv):
    """Once desired MARC/XML record is found, iterate over it & update lookup CSV."""
    # Get Parent XML nodes of interest
    parent_id = process.get_record_001(record)
    parent_xml_items = record.xpath("marc21:datafield[@tag='ITM']", namespaces=NS)
    parent_xml_hldgs = record.xpath("marc21:datafield[@tag='HLD']", namespaces=NS)
    parent_xml_new_field = process.generate_bw_parent_field(parent_id)
    # Generate Parent XML string (bytes) to be injected into Child Records
    parent_xml_str = b""
    for parent_xml_item in parent_xml_items:
        if parent_xml_item is not None:
            parent_xml_str = etree.tostring(parent_xml_item) + b"||"
    for parent_xml_hldg in parent_xml_hldgs:
        if parent_xml_hldg is not None:
            parent_xml_str += etree.tostring(parent_xml_hldg) + b"||"
    parent_xml_str += etree.tostring(parent_xml_new_field)
    parent_xml_str = parent_xml_str.rstrip()
    # Gather Children Identifiers, Verify they are MMS Identifiers, & Add to Lookup
    children_ids = record.xpath(
        "marc21:datafield[@tag='774']//marc21:subfield[@code='w']",
        namespaces=NS
    )
    for child_id in children_ids:
        if re.match("^99[0-9]*3811$", child_id.text.strip()):
            child_mms = child_id.text.strip()
            lookup_csv.writerow({
                "child_id": child_mms,
                "parent_id": parent_id,
                "parent_xml": parent_xml_str.decode("utf-8")
            })

def prepare_alma_data(**kwargs):
    """Update XML records by injecting parent xml when record 001 is in lookup child_id column."""
    access_id = kwargs.get("AWS_ACCESS_KEY_ID")
    access_secret = kwargs.get("AWS_SECRET_ACCESS_KEY")
    bucket = kwargs.get("BUCKET")
    dest_prefix = kwargs.get("DEST_PREFIX")
    lookup_key = kwargs.get("LOOKUP_KEY")
    src_prefix = kwargs.get("SOURCE_PREFIX")
    src_suffix = kwargs.get("SOURCE_SUFFIX")
    s3_keys = ast.literal_eval(kwargs.get("S3_KEYS"))

    # Generate list of S3 keys we want to index
    alma_keys = [key for key in s3_keys if key.startswith(src_prefix) and key.endswith(src_suffix)]

    # Read Boundwith Lookup file into Memory, with child_id column as array
    csv_data = process.get_s3_content(bucket, lookup_key, access_id, access_secret)
    lookup_csv = pandas.read_csv(io.BytesIO(csv_data), header=0)

    # Process filtered set of keys to untar, ungzip, add MARC21 XML namespaces,
    # & inject parent XML if the record is an identified (via lookup) child record.
    logging.info("Starting to iterate over S3 objects")
    for key in alma_keys:
        logging.info("Loading s3 key %s", key)
        src_obj = process.get_s3_content(bucket, key, access_id, access_secret)
        src_data = process.expand_alma_sftp_tarball(key, src_obj)
        src_xml = process.add_marc21xml_root_ns(src_data)
        for record in src_xml.findall("{http://www.loc.gov/MARC21/slim}record"):
            record_id = process.get_record_001(record)
            parent_txt = lookup_csv.loc[lookup_csv.child_id == int(record_id), 'parent_xml'].values
            if len(set(parent_txt)) >= 1:
                logging.info("Child XML record found %s", record_id)
                for parent_node in parent_txt[0].split("||"):
                    try:
                        record.append(etree.fromstring(parent_node))
                    except etree.XMLSyntaxError as error:
                        logging.error("Problem with string syntax:")
                        logging.error(error)
                        logging.error(parent_node)
        dest_key = key.replace(src_suffix, "").replace(src_prefix, dest_prefix + "/alma_bibs__")
        process.generate_s3_object(
            etree.tostring(src_xml),
            bucket,
            dest_key,
            access_id,
            access_secret
        )

def delete_oai_solr(**kwargs):
    """Grab XML from S3, Parse ID & Datestamp, then Issue delete to Solr."""
    access_id = kwargs.get("AWS_ACCESS_KEY_ID")
    access_secret = kwargs.get("AWS_SECRET_ACCESS_KEY")
    bucket = kwargs.get("BUCKET")
    src_prefix = kwargs.get("SOURCE_PREFIX")
    oai_prefix = kwargs.get("OAI_PREFIX")
    alias = kwargs.get("ALIAS")
    solr_conn_id = kwargs.get("SOLR_CONN")

    # Generate list of S3 keys we want to delete
    alma_keys = process.list_s3_content(bucket, access_id, access_secret, prefix=src_prefix)

    # For each record, grab datestamp & id; parse id; & send delete to Solr
    logging.info("Starting to iterate over S3 objects")
    for key in alma_keys:
        logging.info("Loading s3 key %s", key)
        src_obj = process.get_s3_content(bucket, key, access_id, access_secret)
        src_xml = etree.fromstring(src_obj)
        for oai_record in src_xml.findall("{http://www.openarchives.org/OAI/2.0/}record"):
            oai_record_ids = oai_record.xpath("oai:header/oai:identifier", namespaces=NS)
            oai_record_dates = oai_record.xpath("oai:header/oai:datestamp", namespaces=NS)

            if oai_record_ids == [] or oai_record_ids[0].text is None:
                logging.error("Record without an OAI Identifier:")
                logging.error(str(etree.tostring(oai_record)))
                return None

            if len(oai_record_ids) > 1:
                logging.error("Record with multiple OAI Identifiers:")
                logging.error(str(etree.tostring(oai_record)))
                return None

            if oai_record_dates == [] or oai_record_dates[0].text is None:
                logging.error("Record without an OAI Datestamp:")
                logging.error(str(etree.tostring(oai_record)))
                return None

            if len(oai_record_dates) > 1:
                logging.error("Record with multiple OAI Datestamps:")
                logging.error(str(etree.tostring(oai_record)))
                return None

            oai_record_id = oai_record_ids[0].text
            record_id = oai_record_id.replace(oai_prefix, "")
            oai_record_date = oai_record_dates[0].text

            hook = HttpHook("POST", solr_conn_id)
            hook.run(
                    endpoint=f"/solr/{alias}/update",
                    data=f"<delete><query>record_update_date:[0 TO {oai_record_date}] AND id:{record_id}</query></delete>",
                    headers={"Content-Type": "text/xml"}
                    )
