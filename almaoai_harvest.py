from datetime import datetime, timedelta
import os.path
import xml.etree.ElementTree
import xml.dom.minidom
from airflow.models import Variable
from airflow import AirflowException
from cob_datapipeline.oai_harvest import oai_harvest

NEW_FIRST_LINE = '<?xml version="1.0" encoding="UTF-8"?>'
NEW_ROOT = '<collection xmlns="http://www.loc.gov/MARC21/slim" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">'
NEW_ROOT_CLOSING_TAG = '</collection>'


def boundwithparents_oai_harvest(ds, **kwargs):
    data_dir = Variable.get("AIRFLOW_DATA_DIR")
    endpoint_url = Variable.get("ALMA_OAI_ENDPOINT") + "/request"
    set_spec = "alma_bibs_boundwith_parents"

    outfilename = data_dir + '/boundwith_parents.xml'
    if os.path.isfile(outfilename):
        print('Not re-harvesting until ingest_marc succeeds and moves old {}.'.format(outfilename))
        return
    else:
        outfile = open(outfilename, 'w')
    outfile.write(NEW_FIRST_LINE)
    outfile.write(NEW_ROOT)

    kwargs = {}
    process_kwargs = {'outfile': outfile}
    oai_harvest(endpoint_url, set_spec, boundwith_process_record, process_kwargs, **kwargs)

    outfile.write(NEW_ROOT_CLOSING_TAG)
    outfile.close()


def boundwithchildren_oai_harvest(ds, **kwargs):
    data_dir = Variable.get("AIRFLOW_DATA_DIR")
    endpoint_url = Variable.get("ALMA_OAI_ENDPOINT") + "/request"
    set_spec = "alma_bibs_boundwith_children"

    outfilename = data_dir + '/boundwith_children.xml'
    if os.path.isfile(outfilename):
        print('Not re-harvesting until ingest_marc succeeds and moves old {}.'.format(outfilename))
        return
    else:
        outfile = open(outfilename, 'w')
    outfile.write(NEW_FIRST_LINE)
    outfile.write(NEW_ROOT)

    kwargs = {}
    process_kwargs = {'outfile': outfile}
    oai_harvest(endpoint_url, set_spec, boundwith_process_record, process_kwargs, **kwargs)

    outfile.write(NEW_ROOT_CLOSING_TAG)
    outfile.close()


def boundwith_process_record(record, process_args):
    outfile = process_args['outfile']
    tree = xml.etree.ElementTree.fromstring(record.raw)
    # outfile.write(xml.etree.ElementTree.tostring(subrecord, encoding='unicode'))
    header = tree[0]
    subrecord = None
    if len(list(tree)) > 1 and len(list(tree[1])):
        subrecord = tree[1][0]
    if subrecord is not None:
        subrecord.insert(0, header)
        outfile.write('{}\n'.format(xml.etree.ElementTree.tostring(subrecord, encoding='unicode')))
    return process_args


def almaoai_harvest(ds, **kwargs):
    try:
        outfile = None
        deletedfile = None
        num_deleted_recs = 0
        num_updated_recs = 0
        # At some point support for HHMMSS granularity was added to the alma endpoint
        # This can be verified at https://temple.alma.exlibrisgroup.com/view/oai/01TULI_INST/request?verb=Identify
        # NOTE: OAI date ranges are inclusive on both ends
        UTC_DATESTAMP_FSTR = '%Y-%m-%dT%H:%M:%SZ'
        data_dir = Variable.get("AIRFLOW_DATA_DIR")
        endpoint_url = Variable.get("ALMA_OAI_ENDPOINT") + "/request"
        # e.g. 'https://temple.alma.exlibrisgroup.com/view/oai/01TULI_INST/request'
        oai_publish_interval = Variable.get("ALMA_OAI_PUBLISH_INTERVAL")
        set_spec = 'blacklight'

        date_current_harvest = datetime.now()
        date_last_harvest = datetime.strptime(Variable.get("almaoai_last_harvest_date"), UTC_DATESTAMP_FSTR)
        harvest_from_date = (date_last_harvest - timedelta(hours=int(oai_publish_interval))).strftime(UTC_DATESTAMP_FSTR)
        harvest_until_date = date_current_harvest.strftime(UTC_DATESTAMP_FSTR)

        outfilename = data_dir + '/oairecords.xml'
        if os.path.isfile(outfilename):
            print('Not re-harvesting until ingest_marc succeeds and moves old oairecords.xml.')
            return
        else:
            outfile = open(outfilename, 'w')

        deletedfilename = data_dir + '/oairecords_deleted.xml'
        deletedfile = open(deletedfilename, 'w')

        # xml.etree.ElementTree.register_namespace('xmlns','http://www.loc.gov/MARC21/slim')
        outfile.write(NEW_FIRST_LINE)
        outfile.write(NEW_ROOT)
        deletedfile.write(NEW_FIRST_LINE)
        deletedfile.write(NEW_ROOT)

        kwargs = {'harvest_from_date': str(harvest_from_date), 'harvest_until_date': str(harvest_until_date)}
        process_kwargs = {'outfile': outfile, 'deletedfile': deletedfile, 'num_updated_recs': 0, 'num_deleted_recs': 0}

        oai_harvest(endpoint_url, set_spec, tulcob_process_records, process_kwargs, **kwargs)

        outfile.write(NEW_ROOT_CLOSING_TAG)
        outfile.close()
        deletedfile.write(NEW_ROOT_CLOSING_TAG)
        deletedfile.close()
        print("num_updated_recs {}".format(num_updated_recs))
        Variable.set("almaoai_last_num_oai_update_recs", num_updated_recs)
        print("num_deleted_recs {}".format(num_deleted_recs))
        Variable.set("almaoai_last_num_oai_delete_recs", num_deleted_recs)
        # If we got nothing, it might be because the OAI publish interval
        # changed on us. Don't update harvest date because we should come back
        # to this same time again in hopes the OAI endpoint got new data for
        # this time interval
        if num_updated_recs == 0:
            print("Got no OAI records, we'll revisit this date next harvest.")
        else:
            Variable.set("almaoai_last_harvest_date", harvest_until_date)
            Variable.set("almaoai_last_harvest_from_date", harvest_from_date)
    except Exception as ex:
        print(str(ex))
        if outfile is not None:
            if outfile.closed is not True:
                outfile.close()
            os.remove(outfilename)
            #if we died in the middle of a harvest, don't keep a partial download
        if deletedfile is not None:
            if deletedfile.closed is not True:
                deletedfile.close()
            os.remove(deletedfilename)

        raise AirflowException('Harvest failed.')


def tulcob_process_records(record, process_args):
    outfile = process_args['outfile']
    deletedfile = process_args['deletedfile']
    num_updated_recs = process_args['num_updated_recs']
    num_deleted_recs = process_args['num_deleted_recs']

    tree = xml.etree.ElementTree.fromstring(record.raw)
    # outfile.write(xml.etree.ElementTree.tostring(subrecord, encoding='unicode'))
    header = tree[0]
    subrecord = None
    if len(list(tree)) > 1 and len(list(tree[1])):
        subrecord = tree[1][0]
    if subrecord is not None:
        subrecord.insert(0, header)
        outfile.write('{}\n'.format(xml.etree.ElementTree.tostring(subrecord, encoding='unicode')))
        num_updated_recs += 1
    elif header.get('status') == 'deleted':
        record_text = xml.etree.ElementTree.tostring(header, encoding='unicode')
        deletedfile.write('<record>{}</record>\n'.format(record_text))
        num_deleted_recs += 1
    else:
        print('subrecord issue?')
        print(record.raw)
    process_args['num_updated_recs'] = num_updated_recs
    process_args['num_deleted_recs'] = num_deleted_recs
    return process_args
