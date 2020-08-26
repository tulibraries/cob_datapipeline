# pylint: disable=missing-docstring,line-too-long

import unittest
from cob_datapipeline.operators import SolrApiBaseOperator
from cob_datapipeline.exceptions import SafetyCheckException

class DeleteAliasTest(unittest.TestCase):

    def test_safety_check(self):
        """
        Checks that if a skip_included is set it is never empty or None.  This
        is an extra safety measure against the possiblity of meaning to skip
        deleting something but not configuring it properly.
        """
        with self.assertRaises(SafetyCheckException):
            SolrApiBaseOperator(
                task_id='test_task',
                name='foo',
                data={},
                skip_included=['bar', None],
                solr_conn_id='solr_conn_id')

        with self.assertRaises(SafetyCheckException):
            SolrApiBaseOperator(
                task_id='test_task',
                name='foo',
                data={},
                skip_included=['None'],
                solr_conn_id='solr_conn_id')

        with self.assertRaises(SafetyCheckException):
            SolrApiBaseOperator(
                task_id='test_task',
                name='foo',
                data={},
                skip_included=[''],
                solr_conn_id='solr_conn_id')
