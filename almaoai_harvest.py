import datetime
import time
from sickle import Sickle
from airflow.models import Variable
from airflow import AirflowException
import xml.etree.ElementTree
import xml.dom.minidom
import os.path

def almaoai_harvest():
    try:
        outfile = None
        deletedfile = None
        date_current_harvest = datetime.datetime.now()
        try:
            date = Variable.get("almaoai_last_harvest_date")
        except KeyError:
            Variable.set("almaoai_last_harvest_date", date_current_harvest.strftime('%Y-%m-%d'))
            date = date_current_harvest

        outfilename = Variable.get("AIRFLOW_HOME") + '/oairecords.xml'
        if os.path.isfile(outfilename):
            print('Not re-harvesting until ingest_marc succeeds and moves old oairecords.xml.')
            return None
        else:
            outfile = open(outfilename,'w')

        deletedfilename = Variable.get("AIRFLOW_HOME") + '/oairecords_deleted.xml'
        deletedfile = open(deletedfilename,'w')

        endpoint_url = 'https://temple.alma.exlibrisgroup.com/view/oai/01TULI_INST/request'
        sickle = Sickle(endpoint_url)
        records = sickle.ListRecords(**{'metadataPrefix':'marc21','set':'blacklight','from':'{}'.format(date)})
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
                subrecord.insert(0,header)
                outfile.write(xml.dom.minidom.parseString(xml.etree.ElementTree.tostring(tree,'unicode')).toprettyxml(indent="\t"))
            elif header.get('status') == 'deleted':
                deletedfile.write('<record>{}</record>'.format(xml.etree.ElementTree.tostring(header, encoding='unicode')))
            else:
                print('subrecord issue?')
                print(r.raw)
        outfile.write(newrootclosingtag)
        outfile.close()
        deletedfile.write(newrootclosingtag)
        deletedfile.close()
        Variable.set("almaoai_last_harvest_date", date_current_harvest.strftime('%Y-%m-%d'))
    except:
        if outfile is not None and outfile.closed is not True:
            outfile.close()
        if deletedfile is not None and deletedfile.closed is not True:
            deletedfile.close()
        print(r.raw)
        raise AirflowException('Harvest failed.')
