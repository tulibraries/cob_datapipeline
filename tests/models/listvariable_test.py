# pylint: disable=missing-docstring

import unittest
from cob_datapipeline.models import ListVariable

class ListVariableTest(unittest.TestCase):

    def setUp(self):
        ListVariable.delete('bizz')

    def test_push_no_value_skip_blank_true(self):
        log_name = 'cob_datapipeline.models.list_variable'
        with self.assertLogs(log_name) as log:
            ListVariable.push('bizz', None, skip_blank=True)
            msg = "INFO:cob_datapipeline.models.list_variable:Skipping empty value push."
        self.assertIn(msg, log.output)

    def test_push_no_value_default(self):
        ListVariable.push('bizz', None)
        self.assertEqual(ListVariable.get('bizz'), [None])

    def test_get_non_existant_list_varabile(self):
        self.assertEqual(ListVariable.get('bizz'), [])

    def test_push_with_value(self):
        ListVariable.push('bizz', 1)
        self.assertEqual(ListVariable.get('bizz'), [1])

    def test_push_mulitple_times(self):
        ListVariable.push('bizz', 1)
        ListVariable.push('bizz', 2)
        ListVariable.push('bizz', 3)
        ListVariable.push('bizz', 1)
        self.assertEqual(ListVariable.get('bizz'), [1, 2, 3, 1])

    def test_push_mulitple_times_unique_true(self):
        ListVariable.push('bizz', 1, unique=True)
        ListVariable.push('bizz', 2, unique=True)
        ListVariable.push('bizz', 3, unique=True)
        ListVariable.push('bizz', 1, unique=True)
        self.assertEqual(ListVariable.get('bizz'), [1, 2, 3])
