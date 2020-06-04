import unittest
from cob_datapipeline import helpers


class TestDetermineMostRecentDate(unittest.TestCase):
    """Primary Class for Testing Logic for Picking Most Recent Alma SFTP files"""

    def test_determine_most_recent_date(self):
        files_list = ["alma_bibs__1234_test_1", "alma_bibs__1235_test_1", "alma_bibs__1235_test_2", "alma_bibs__12340_test_1"]
        self.assertEqual(helpers.determine_most_recent_date(files_list), 12340)