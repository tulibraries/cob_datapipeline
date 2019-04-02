import datetime
import time
from sickle import Sickle
from airflow.models import Variable
from airflow import AirflowException
import xml.etree.ElementTree
import xml.dom.minidom
import os.path


def almaoai_harvest(ds, **kwargs):
    try:
        outfile = None
        deletedfile = None
        r = None
        # At some point support for HHMMSS granularity was added to the alma endpoint
        # This can be verified at https://temple.alma.exlibrisgroup.com/view/oai/01TULI_INST/request?verb=Identify
        UTC_DATESTAMP_FSTR = '%Y-%m-%dT%H:%M:%SZ'
        data_dir = Variable.get("AIRFLOW_DATA_DIR")
        endpoint_url = Variable.get("ALMA_OAI_ENDPOINT") #'https://temple.alma.exlibrisgroup.com/view/oai/01TULI_INST/request'
        date_current_harvest = datetime.datetime.now().strftime(UTC_DATESTAMP_FSTR)
        num_deleted_recs = 0
        num_updated_recs = 0
        # note: OAI date ranges are inclusive on both ends

        try:
            date = Variable.get("almaoai_last_harvest_date")
        except KeyError:
            Variable.set("almaoai_last_harvest_date", date_current_harvest)
            date = date_current_harvest

        print("Harvesting starting from {}".format(date))

        outfilename = data_dir + '/oairecords.xml'
        if os.path.isfile(outfilename):
            print('Not re-harvesting until ingest_marc succeeds and moves old oairecords.xml.')
            return
        else:
            outfile = open(outfilename, 'w')

        deletedfilename = data_dir + '/oairecords_deleted.xml'
        deletedfile = open(deletedfilename, 'w')

        sickle = Sickle(endpoint_url)
        records = sickle.ListRecords(**{'metadataPrefix': 'marc21', 'set': 'blacklight', 'from': '{}'.format(date)})
        # xml.etree.ElementTree.register_namespace('xmlns','http://www.loc.gov/MARC21/slim')
        newfirstline = '<?xml version="1.0" encoding="UTF-8"?>'
        newroot = '<collection xmlns="http://www.loc.gov/MARC21/slim" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">'
        newrootclosingtag = '</collection>'
        outfile.write(newfirstline)
        outfile.write(newroot)
        deletedfile.write(newfirstline)
        deletedfile.write(newroot)
        for r in records:
            tree = xml.etree.ElementTree.fromstring(r.raw)
            # outfile.write(xml.etree.ElementTree.tostring(subrecord, encoding='unicode'))
            header = tree[0]
            subrecord = None
            if len(list(tree)) > 1 and len(list(tree[1])) > 0:
                subrecord = tree[1][0]
            if subrecord is not None:
                # if subrecord.get('status') == 'deleted':
                #     deletedfile.write('<record>{}</record>'.format(xml.etree.ElementTree.tostring(header, encoding='unicode')))
                # else:
                subrecord.insert(0, header)
                #outfile.write(xml.dom.minidom.parseString(xml.etree.ElementTree.tostring(subrecord,'unicode')).toprettyxml(indent="\t"))
                outfile.write(xml.etree.ElementTree.tostring(subrecord, encoding='unicode'))
                num_updated_recs += 1
            elif header.get('status') == 'deleted':
                deletedfile.write('<record>{}</record>'.format(xml.etree.ElementTree.tostring(header, encoding='unicode')))
                num_deleted_recs += 1
            else:
                print('subrecord issue?')
                print(r.raw)
        outfile.write(newrootclosingtag)
        outfile.close()
        deletedfile.write(newrootclosingtag)
        deletedfile.close()
        print("num_updated_recs {}".format(num_updated_recs))
        Variable.set("almaoai_last_num_oai_update_recs", num_updated_recs)
        print("num_deleted_recs {}".format(num_deleted_recs))
        Variable.set("almaoai_last_num_oai_delete_recs", num_deleted_recs)
        Variable.set("almaoai_last_harvest_date", date_current_harvest)
    except Exception as e:
        if outfile is not None and outfile.closed is not True:
            outfile.close()
        if deletedfile is not None and deletedfile.closed is not True:
            deletedfile.close()
        if r is not None:
            print(r.raw)
        print(str(e))

        raise AirflowException('Harvest failed.')
