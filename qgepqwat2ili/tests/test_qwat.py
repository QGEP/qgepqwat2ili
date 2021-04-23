import logging
import os
import sys
import tempfile
import unittest

from qgepqwat2ili import main, utils

# Display logging in unittest output
logger = logging.getLogger()
logger.setLevel(logging.WARNING)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.WARNING)
logger.addHandler(handler)


class TestQGEPUseCases(unittest.TestCase):
    def test_case_b_export_complete_qwat_to_xtf(self):
        """
        # B. export the whole QWAT model to interlis
        """

        # Prepare db
        main(["setupdb", "full"])

        path = os.path.join(tempfile.mkdtemp(), "export.xtf")
        main(["qwat", "export", path, "--recreate_schema"])

        # Validate the outgoing XTF
        print(f"Saved to {path}")
        utils.ili2db.validate_xtf_data(path)
