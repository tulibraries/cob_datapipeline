import unittest
from airflow.models import DagBag

class TestDagIntegrity(unittest.TestCase):

    LOAD_SECOND_THRESHOLD = 2

    def setUp(self):
        self.dagbag = DagBag()

    @unittest.skip("Dags need to be refactored to be testable first.")
    def test_import_dags(self):
        """
        Unfortunately most of these dags error out in test context because
        they are dependent on global variables that are saved to DB.

        I wrote cob_datapipeline.globals specifically to allow getting default
        values without need of a DB (as is the case in the unit test context)
        """
        self.assertFalse(
            len(self.dagbag.import_errors),
            'DAG import failures. Errors: {}'.format(
                self.dagbag.import_errors
            )
        )
