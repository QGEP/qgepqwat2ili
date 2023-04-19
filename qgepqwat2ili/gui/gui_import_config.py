import os
#from collections import OrderedDict

from qgis.core import Qgis, QgsSettings
from qgis.PyQt.QtWidgets import QCheckBox, QDialog
from qgis.PyQt.uic import loadUi

#from ....utils.qgeplayermanager import QgepLayerManager
#from ..processing_algs.extractlabels_interlis import ExtractlabelsInterlisAlgorithm

#7.7.2022 
from qgis.PyQt.QtCore import pyqtSlot


class GuiImportConfig(QDialog):
    def __init__(self, parent):
        super().__init__(parent)
        
        loadUi(os.path.join(os.path.dirname(__file__), "gui_import_config.ui"), self)

        self.finished.connect(self.on_finish)
        print("GuiImportConfig init")
        # Execute the dialog
        # self.resize(iface.mainWindow().size() * 0.75)

        # Remember save skipvalidation_import_checkbox - to do maybe later
        # s = QgsSettings().value("qgep_plugin/skipvalidation_import", False)
        # self.skipvalidation_import_checkbox.setChecked(s == True or s == "true")

        # Remember save skipimport_wizard_checkbox - to do maybe later
        # s = QgsSettings().value("qgep_plugin/skipimport_wizard", False)
        # self.skipimport_wizard_checkbox.setChecked(s == True or s == "true")

    def on_finish(self):
        # Remember skipvalidation_import checkbox
        # QgsSettings().setValue("qgep_plugin/skipvalidation_import", self.skipvalidation_import)

        # Remember skipimport_wizard checkbox
        # QgsSettings().setValue("qgep_plugin/skipimport_wizard", self.skipimport_wizard)
        print("finished")

    @property
    def skipvalidation_import(self):
        return self.skipvalidation_import.isChecked()

    @property
    def skipimport_wizard(self):
        return self.skipimport_wizard_checkbox.isChecked()
