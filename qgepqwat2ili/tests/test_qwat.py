import logging
import os
import sys
import tempfile
import unittest

from qgepqwat2ili import main

# Display logging in unittest output
logger = logging.getLogger()
logger.setLevel(logging.WARNING)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.WARNING)
logger.addHandler(handler)


class TestQGEPUseCases(unittest.TestCase):
    def test_case_b_export_complete_qwat_to_xtf_with_validation(self):
        """
        # B. export the whole QWAT model to interlis

        This currently is expected to fail until we have completed matching (incl. value lists)
        """

        # Prepare db
        main(["setupdb", "full"])

        path = os.path.join(tempfile.mkdtemp(), "export.xtf")
        main(["qwat", "export", path, "--recreate_schema"])
