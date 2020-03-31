"""Tests suite for XML parsing function primarily used by Catalog DAGs."""
import unittest
import boto3
from cob_datapipeline import sc_xml_parse
from lxml import etree
from moto import mock_s3
from tulflow import process
import logging
import pandas
import io

NS = {
    "marc21": "http://www.loc.gov/MARC21/slim"
}

class TestBWXMLProcessIntegration(unittest.TestCase):
    """Test Class for XML processing functions."""
    kwargs = {
        "AWS_ACCESS_KEY_ID": "test_access_id",
        "AWS_SECRET_ACCESS_KEY": "test_access_secret",
        "BUCKET": "test_bucket",
        "DEST_FOLDER": "almasftp/test/2019/lookup.tsv",
        "S3_KEYS": "['almasftp/alma_bibs__new_1.xml.tar.gz', 'almasftp/alma_bibs__boundwith_new_1.xml.tar.gz']",
        "SOURCE_FOLDER": "almasftp/alma_bibs__boundwith"
    }

    @mock_s3
    def test_prepare_boundwiths(self):
        """Test creating Boundwith Lookup CSV file."""
        self.maxDiff = None
        conn = boto3.client(
            "s3",
            aws_access_key_id=self.kwargs.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=self.kwargs.get("AWS_SECRET_ACCESS_KEY")
        )
        conn.create_bucket(Bucket=self.kwargs.get("BUCKET"))
        conn.put_object(
            Bucket=self.kwargs.get("BUCKET"),
            Key="almasftp/alma_bibs__new_1.xml.tar.gz",
            Body=open("tests/fixtures/alma_bibs__new_1.xml.tar.gz", "rb").read()
        )
        conn.put_object(
            Bucket=self.kwargs.get("BUCKET"),
            Key="almasftp/alma_bibs__boundwith_new_1.xml.tar.gz",
            Body=open("tests/fixtures/alma_bibs__boundwith_new_1.xml.tar.gz", "rb").read()
        )

        sc_xml_parse.prepare_boundwiths(**self.kwargs)
        test_result = conn.get_object(
            Bucket=self.kwargs.get("BUCKET"),
            Key=self.kwargs.get("DEST_FOLDER")
        )
        test_csv = pandas.read_csv(io.BytesIO(test_result["Body"].read()), header=0)
        self.assertEqual(
            test_csv.loc[test_csv.child_id == 991006932409703811, "parent_xml"].values[0],
            """<datafield xmlns="http://www.loc.gov/MARC21/slim" tag="ITM" ind1=" " ind2=" "><subfield code="r">22457827260003811</subfield><subfield code="b">1</subfield><subfield code="h">0</subfield><subfield code="g">ASRS</subfield><subfield code="t">BOOK</subfield><subfield code="9">39074016052185</subfield><subfield code="e">ASRS</subfield><subfield code="8">23326254130003811</subfield><subfield code="a">0</subfield><subfield code="q">2017-06-20 13:38:20</subfield><subfield code="i">HD72 .O38 v.7 no.53</subfield><subfield code="d">ASRS</subfield><subfield code="f">ASRS</subfield></datafield>||<datafield xmlns="http://www.loc.gov/MARC21/slim" tag="HLD" ind1="0" ind2=" "><subfield code="b">ASRS</subfield><subfield code="c">ASRS</subfield><subfield code="h"> HD72 </subfield><subfield code="i"> .O38 v.7 no.53 </subfield><subfield code="8">22457827260003811</subfield><subfield code="updated">2019-04-19 19:36:45</subfield><subfield code="created">2017-06-20 13:38:20</subfield></datafield>||<marc21:datafield xmlns:marc21="http://www.loc.gov/MARC21/slim" ind1=" " ind2=" " tag="ADF"><marc21:subfield code="a">9910367273103811</marc21:subfield></marc21:datafield>"""
        )
        self.assertEqual(test_csv.shape, (9, 3))
        self.assertEqual(
            list(test_csv.loc[test_csv.parent_id == 9910367273003811, "child_id"].values),
            [991031640129703811, 991012069669703811]
        )


class TestAlmaXMLProcessIntegration(unittest.TestCase):
    """Test Class for XML processing functions."""
    kwargs = {
        "AWS_ACCESS_KEY_ID": "test_access_id",
        "AWS_SECRET_ACCESS_KEY": "test_access_secret",
        "BUCKET": "test_bucket_2",
        "DEST_PREFIX": "almasftp/test/2019",
        "LOOKUP_KEY": "almasftp/test/2019/lookup.tsv",
        "S3_KEYS": "['almasftp/alma_bibs__new_1.xml.tar.gz', 'almasftp/alma_bibs__boundwith_new_1.xml.tar.gz']",
        "SOURCE_PREFIX": "almasftp/alma_bibs__",
        "SOURCE_SUFFIX": ".tar.gz"
    }

    @mock_s3
    def test_prepare_alma_data(self):
        """Test preparing Alma Data (expand, add namespace, add bw parent info) for indexing."""
        self.maxDiff = None
        conn = boto3.client(
            "s3",
            aws_access_key_id=self.kwargs.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=self.kwargs.get("AWS_SECRET_ACCESS_KEY")
        )
        conn.create_bucket(Bucket=self.kwargs.get("BUCKET"))
        conn.put_object(
            Bucket=self.kwargs.get("BUCKET"),
            Key="almasftp/alma_bibs__new_1.xml.tar.gz",
            Body=open("tests/fixtures/alma_bibs__new_1.xml.tar.gz", "rb").read()
        )
        conn.put_object(
            Bucket=self.kwargs.get("BUCKET"),
            Key="almasftp/alma_bibs__boundwith_new_1.xml.tar.gz",
            Body=open("tests/fixtures/alma_bibs__boundwith_new_1.xml.tar.gz", "rb").read()
        )
        conn.put_object(
            Bucket=self.kwargs.get("BUCKET"),
            Key="almasftp/test/2019/lookup.tsv",
            Body=open("tests/fixtures/desired_lookup.tsv", "rb").read()
        )

        sc_xml_parse.prepare_alma_data(**self.kwargs)
        test_result = conn.list_objects(
            Bucket=self.kwargs.get("BUCKET"),
            Prefix=self.kwargs.get("DEST_PREFIX")
        )
        self.assertEqual(
            [key["Key"] for key in test_result["Contents"]],
            [
                "almasftp/test/2019/alma_bibs__boundwith_new_1.xml",
                "almasftp/test/2019/alma_bibs__new_1.xml",
                "almasftp/test/2019/lookup.tsv"
            ]
        )
        test_object = conn.get_object(
            Bucket=self.kwargs.get("BUCKET"),
            Key="almasftp/test/2019/alma_bibs__new_1.xml"
        )
        test_xml = etree.fromstring(test_object["Body"].read())
        for record in test_xml:
            if process.get_record_001(record) == "991022063789703811":
                test_record = record
        parent_id_xpath = "marc21:datafield[@tag='ADF']//marc21:subfield[@code='a']"
        parent_hld_xpath = "marc21:datafield[@tag='HLD']"
        parent_itm_xpath = "marc21:datafield[@tag='ITM']"

        self.assertEqual(
            test_record.xpath(parent_id_xpath, namespaces=NS)[0].text,
            "9910367272903811"
        )
        self.assertEqual(
            etree.tostring(test_record.xpath(parent_hld_xpath, namespaces=NS)[0]),
            b'<datafield xmlns="http://www.loc.gov/MARC21/slim" tag="HLD" ind1="0" ind2=" "><subfield code="b">KARDON</subfield><subfield code="c">p_remote</subfield><subfield code="h">PQ2105.A2B4</subfield><subfield code="8">22326269950003811</subfield><subfield code="updated">2017-06-20 13:38:41</subfield><subfield code="created">2017-06-20 13:38:22</subfield></datafield>'
        )
        self.assertEqual(
            etree.tostring(test_record.xpath(parent_itm_xpath, namespaces=NS)[0]),
            b'<datafield xmlns="http://www.loc.gov/MARC21/slim" tag="ITM" ind1=" " ind2=" "><subfield code="r">22326269950003811</subfield><subfield code="b">1</subfield><subfield code="h">0</subfield><subfield code="g">p_remote</subfield><subfield code="t">BOOK</subfield><subfield code="9">39074004700811</subfield><subfield code="e">p_remote</subfield><subfield code="8">23326269940003811</subfield><subfield code="c">v.248</subfield><subfield code="a">0</subfield><subfield code="q">2017-06-20 13:38:22</subfield><subfield code="i">PQ2105.A2B4</subfield><subfield code="d">KARDON</subfield><subfield code="f">KARDON</subfield></datafield>'
        )
