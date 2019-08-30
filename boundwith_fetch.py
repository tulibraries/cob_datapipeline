from collections import defaultdict
import os.path
import urllib
import xmltodict
from airflow.models import Variable
from airflow import AirflowException

'''
https://api-na.hosted.exlibrisgroup.com/almaws/v1/conf/sets/4165880080003811/
     members?limit=100&offset=0&apikey=xxxx
'''

ALMA_REST_ENDPOINT = 'https://api-na.hosted.exlibrisgroup.com/almaws/v1/'
ALMA_SETS_API_PATH = 'conf/sets/'
ALMA_BIBS_API_PATH = 'bibs/'
BOUNDWITH_HOST_RECORDS_SETID = '4165880080003811'
BOUNDWITH_ITEMIZED_SETID = '11201989000003811'
ALMA_SETS_MEMBERS_PATH = '/members'
SET_XML_BEGIN = '<set link="string"> <name>Boundwith Children Testing</name> \
                 <type>ITEMIZED</type>  <content>BIB_MMS</content>  <private>true</private> \
                 <status>ACTIVE</status>'
SET_XML_END = '</set>'
MEMBER_XML = '<member link=""><id>0</id><description>Description</description></member>'

DICT_CONSTRUCTOR_LAMBA = lambda *args, **kwargs: defaultdict(list, *args, **kwargs)

# importlib.reload(xmltodict)

def delete_old_boundwith_itemized_children(apikey):
    # get boundwith children itemized set info for num records
    file = urllib.request.urlopen(ALMA_REST_ENDPOINT+ALMA_SETS_API_PATH+
                                  BOUNDWITH_ITEMIZED_SETID+'?apikey='+apikey)
    data = file.read()
    file.close()
    setdata = xmltodict.parse(data, dict_constructor=DICT_CONSTRUCTOR_LAMBA)
    numrecords = int(setdata['set'][0]['number_of_members'][0]['#text'][0])
    # loop through results and save them aside to delete later
    offset = 0
    numperpage = 100
    itemized_records = []
    while offset < numrecords:
        # page through boundwith child records
        url = "{}{}{}{}?limit={}&offset={}&apikey={}".format(ALMA_REST_ENDPOINT,
                                                             ALMA_SETS_API_PATH,
                                                             BOUNDWITH_ITEMIZED_SETID,
                                                             ALMA_SETS_MEMBERS_PATH,
                                                             str(numperpage),
                                                             str(offset),
                                                             apikey)
        file = urllib.request.urlopen(url)
        data = file.read()
        file.close()
        # hack up this xml the dumb way because who cares
        membersstart = str(data).find('<members')
        membersend = str(data).find('</members>')
        membersxml = str(data)[membersstart:membersend+10]
        membersxml = xmltodict.parse(membersxml, dict_constructor=DICT_CONSTRUCTOR_LAMBA)
        # just take the xml wholesale and use it for itemized set member request
        # doing it one page at a time ensures we never hit the 1000 member limit
        if membersxml != None:
            itemized_records.append(membersxml['members'])
        offset += numperpage
    # reset count and delete them all
    offset = 0
    for membersxml in itemized_records:
        setdata['set'][0]['members'] = membersxml
        rmsetxml = xmltodict.unparse(setdata)
        # delete members from set
        # POST /almaws/v1/conf/sets/{set_id}
        requrl = "{}{}{}?op=delete_members&apikey={}".format(ALMA_REST_ENDPOINT, ALMA_SETS_API_PATH,
                                                             BOUNDWITH_ITEMIZED_SETID, apikey)
        postreq = urllib.request.Request(requrl, data=rmsetxml.encode('utf-8'),
                                         headers={'Content-Type': 'application/xml'}, method='POST')
        try:
            httpresponse = urllib.request.urlopen(postreq)
        except urllib.error.HTTPError as ex:
            print(ex)
            print(requrl)
            print(rmsetxml)
            raise AirflowException('Could not POST delete members to URL {}'.format(requrl))
        data = httpresponse.read()
        # print('Set POST response: {}\n'.format(data))
        httpresponse.close()
    return setdata

def extract_members_from_xml(xmldata):
    # hack up this xml the dumb way because who cares
    membersstart = str(xmldata).find('<members')
    membersend = str(xmldata).find('</members>')
    membersxml = str(xmldata)[membersstart:membersend+10]
    membersxml = xmltodict.parse(membersxml, dict_constructor=DICT_CONSTRUCTOR_LAMBA)
    return membersxml

def extract_child_xml(datafieldxml):
    # iterate throught the parent bib xml to find the child id(s)
    # 774w fields = boundwith children ids
    childrenxml = ''
    for datafield in datafieldxml:
        if datafield['@tag'] == '774':
            for subfield in datafield['subfield']:
                if subfield['@code'] == 'w':
                    childid = subfield['#text'][0]
                    # print("childid: {}".format(childid))
                    newmemberxml = xmltodict.parse(MEMBER_XML,
                                                   dict_constructor=DICT_CONSTRUCTOR_LAMBA)
                    newmemberxml['member'][0]['id'] = childid
                    newmemberxml['member'][0]['@link'] = ALMA_REST_ENDPOINT+ \
                                                         ALMA_BIBS_API_PATH+ \
                                                         str(childid)
                    childrenxml += xmltodict.unparse(newmemberxml, full_document=False)
    return childrenxml


def create_boundwith_children_itemized_set(ds, **kwargs):
    apikey = kwargs['apikey']
    # start by getting all the parent records
    # get set info for num records
    file = urllib.request.urlopen(ALMA_REST_ENDPOINT+ALMA_SETS_API_PATH+
                                  BOUNDWITH_HOST_RECORDS_SETID+'?apikey='+apikey)
    data = file.read()
    file.close()
    setdata = xmltodict.parse(data, dict_constructor=DICT_CONSTRUCTOR_LAMBA)
    numrecords = int(setdata['set'][0]['number_of_members'][0]['#text'][0])
    #
    #
    # then delete old records from existing boundwith itemized set
    childrenset = delete_old_boundwith_itemized_children(apikey)
    # page through boundwith parent records & get the boundwith parent IDs
    offset = 0
    numperpage = 100
    print("Boundwith parents numrecords: {}".format(numrecords))
    while offset < numrecords:
        # get a page of results from the set
        requrl = "{}{}{}{}?limit={}&offset={}&apikey={}".format(ALMA_REST_ENDPOINT,
                                                                ALMA_SETS_API_PATH,
                                                                BOUNDWITH_HOST_RECORDS_SETID,
                                                                ALMA_SETS_MEMBERS_PATH,
                                                                str(numperpage),
                                                                str(offset),
                                                                apikey)
        file = urllib.request.urlopen(requrl)
        parentxmldata = file.read()
        file.close()
        # get the set members
        membersxml = extract_members_from_xml(parentxmldata)
        # iterate over every set member (parent) to get all child ids
        # print("membersxml: {}".format(membersxml))
        # print("members: {}".format(membersxml['members'][0]['member']))
        for member in membersxml['members'][0]['member']:
            # get the parent's full bib record from the alma API
            parentbiburl = member['@link']
            # print(parentbiburl)
            file = urllib.request.urlopen(parentbiburl+'?apikey='+apikey)
            childxmldata = file.read()
            file.close()
            parentbibxml = xmltodict.parse(childxmldata, dict_constructor=DICT_CONSTRUCTOR_LAMBA)
            # get the child ids from the parent xml
            childrenxml = extract_child_xml(parentbibxml['bib'][0]['record'][0]['datafield'])
            # can't get xmltodict to add children and unparse successfully
            # so we're doing this the dumb way too
            addsetxml = xmltodict.unparse(childrenset)
            membersstart = str(addsetxml).find('</number_of_members>')+len('</number_of_members>')
            addsetxml = str(addsetxml)[:membersstart] + '<members>' + \
                        childrenxml + '</members>' + SET_XML_END
            # print("Add set XML:\n" + addsetxml)
            # add members to set POST /almaws/v1/conf/sets/{set_id}
            postrequrl = '{}{}{}?op=add_members&apikey={}'.format(ALMA_REST_ENDPOINT,
                                                                  ALMA_SETS_API_PATH,
                                                                  BOUNDWITH_ITEMIZED_SETID,
                                                                  apikey)
            postreq = urllib.request.Request(postrequrl,
                                             data=addsetxml.encode('utf-8'),
                                             headers={'Content-Type': 'application/xml'},
                                             method='POST')
            try:
                httpresponse = urllib.request.urlopen(postreq)
            except urllib.error.HTTPError as ex:
                print(ex)
                print(ex.file.read())
                # print(postrequrl)
                print(addsetxml)
                # raise AirflowException('Could not POST new children set members to URL {}'.format(requrl))
        # data = httpresponse.read()
        # print('httpresponse: {}'.format(data))
        httpresponse.close()
        offset += numperpage



# UNUSED but might be useful later?
def get_boundwith_parents(ds, **kwargs):
    apikey = kwargs['apikey']
    # get set info for num records
    file = urllib.request.urlopen(ALMA_REST_ENDPOINT+ALMA_SETS_API_PATH+BOUNDWITH_HOST_RECORDS_SETID+'?apikey='+apikey)
    data = file.read()
    file.close()
    setdata = xmltodict.parse(data, dict_constructor=DICT_CONSTRUCTOR_LAMBA)
    numrecords = int(setdata['set'][0]['number_of_members'][0]['#text'][0])
    # open MARC XML FILE
    # data_dir = Variable.get("AIRFLOW_DATA_DIR")
    # outfilename = data_dir + '/boundwith_parents.xml'
    # if os.path.isfile(outfilename):
    #     print('Not re-harvesting until ingest_marc succeeds and moves old {}.'.format(outfilename))
    #     return
    # else:
    #     outfile = open(outfilename, 'w')
    # newfirstline = '<?xml version="1.0" encoding="UTF-8"?>'
    # newroot = '<collection xmlns="http://www.loc.gov/MARC21/slim" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">'
    # newrootclosingtag = '</collection>'
    # outfile.write(newfirstline)
    # outfile.write(newroot)

    offset = 0
    numperpage = 100
    # page through boundwith parent records
    while offset < numrecords:
        requrl = "{}{}{}{}?limit={}&offset={}&apikey={}".format(ALMA_REST_ENDPOINT,
                                                                ALMA_SETS_API_PATH,
                                                                BOUNDWITH_HOST_RECORDS_SETID,
                                                                ALMA_SETS_MEMBERS_PATH,
                                                                str(numperpage),
                                                                str(offset),
                                                                apikey)
        file = urllib.request.urlopen(requrl)
        data = file.read()
        file.close()
        print(data)
        offset += numperpage
    # close MARC XML file
    # outfile.write(newrootclosingtag)
    # outfile.close()


# UNUSED but might be useful later?
# def create_new_set(apikey):
#     ALMA_API_KEY = apikey
    # # create empty set to hold boudnwith parent items POST /almaws/v1/conf/sets
    # setxml = SET_XML_BEGIN + SET_XML_END
    # postreq = urllib.request.Request(ALMA_REST_ENDPOINT+ALMA_SETS_API_PATH+'?apikey='+ALMA_API_KEY, data=setxml.encode('utf-8'), headers={'Content-Type': 'application/xml'}, method='POST')
    # file = urllib.request.urlopen(postreq)
    # data = file.read()
    # file.close()
    # postxml = xmltodict.parse(data, dict_constructor=lambda *args, **kwargs: defaultdict(list, *args, **kwargs))
    # setid = postxml['set'][0]['id'][0]
    # print(setid)

        # hack up this xml the dumb way
        # membersstart = str(data).find('<members')
        # membersend = str(data).find('</members>')
        # membersxml = str(data)[membersstart:membersend+10]
        # membersxml = xmltodict.parse(membersxml, dict_constructor=lambda *args, **kwargs: defaultdict(list, *args, **kwargs))
        # this code pulls the URLs one at a time, so we can retrieve the MARC XML
        # setmembers = xmltodict.parse(data, dict_constructor=lambda *args, **kwargs: defaultdict(list, *args, **kwargs))
        # for member in setmembers['members'][0]['member']:
        #     parentid = member['id'][0]
        #     parentbiburl = setmembers['members'][0]['member'][0]['@link']
        #     file = urllib.request.urlopen(parentbiburl+'?apikey='+ALMA_API_KEY)
        #     data = file.read()
        #     file.close()
        #     parentbibxml = xmltodict.parse(data, dict_constructor=lambda *args, **kwargs: defaultdict(list, *args, **kwargs))
        #     # iterate throught the xml to find the parent ITM
        #     # we could do this with xpath but again, who cares
        #     parentitm = None
        #     for datafield in parentbibxml['bib'][0]['record'][0]['datafield']:
        #         if datafield['@tag'] == 'ITM':
        #             parentitm = datafield
        #
        #     # iterate throught the xml to find the child id
        #     for datafield in parentbibxml['bib'][0]['record'][0]['datafield']:
        #         if datafield['@tag'] == '774':
        #             for subfield in datafield['subfield']:
        #                 if subfield['@code'] == 'w':
        #                     childid = subfield['#text'][0]
        #                     file = urllib.request.urlopen(ALMA_REST_ENDPOINT+ALMA_BIBS_API_PATH+str(childid)+'?apikey='+ALMA_API_KEY)
        #                     data = file.read()
        #                     file.close()
        #                     childbibxml = xmltodict.parse(data, dict_constructor=lambda *args, **kwargs: defaultdict(list, *args, **kwargs))
        #                     # add parent ITM
        #
        #                     # add parent id to ADF
        #
        #     # should we just a build a big map of id to xml?
    # boundwith.xml is a collection - record repeating file with xmlns

    #  delete the itemized set once we're done with it
