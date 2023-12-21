import os
# neu 25.4.2023 / 26.7.2023 wieder eingefügt
import sys
# neu 25.4.2023
from collections import defaultdict
from collections import OrderedDict


from qgis.core import Qgis, QgsSettings
#from qgis.PyQt.QtCore import Qt
#from qgis.PyQt.QtGui import QFont
from qgis.PyQt.QtWidgets import QCheckBox, QDialog, QHeaderView, QTreeWidgetItem
from qgis.PyQt.uic import loadUi
from qgis.utils import iface

#from ....utils.qgeplayermanager import QgepLayerManager
#from ..processing_algs.extractlabels_interlis import ExtractlabelsInterlisAlgorithm

#7.7.2022 
from qgis.PyQt.QtCore import pyqtSlot


import time
import logging

# Required for loadUi to find the custom widget
# 26.7.2023 wieder eingefügt
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

class GuiImportc(QDialog):
    def __init__(self, parent):

        super().__init__(parent)
        
        print("GuiImportconfig init")
        iface.messageBar().pushMessage("GuiImportconfig", level=Qgis.Info)

        datei = os.path.join(os.path.dirname(__file__), "gui_importc.ui")
        #breakpoint()
        iface.messageBar().pushMessage("Info Datei", str(datei), level=Qgis.Info)

        iface.messageBar().pushMessage("Info self", str(self), level=Qgis.Info)
        iface.messageBar().pushMessage("Info parent", str(parent), level=Qgis.Info)
        logging.info(str(datei))
        logging.info(str(self))
        logging.info(str(parent))
        # delays the execution for 5.5 secs.
        time.sleep(5.5)
        iface.messageBar().pushMessage("Info parent", str(parent), level=Qgis.Info)
        #breakpoint()
        # 26.7.2023 Aufruf schlägt fehl - Absturz - warum?
        loadUi(os.path.join(os.path.dirname(__file__), "gui_importc.ui"), self)
        
        #geht nicht mit \
        #loadUi("C:\Users\Stefan\AppData\Roaming\QGIS\QGIS3\profiles\default\python\plugins\qgepplugin\qgepqwat2ili\qgepqwat2ili\gui\gui_importc.ui", self)
        #loadUi("C:/Users/Stefan/AppData/Roaming/QGIS/QGIS3/profiles/default/python/plugins/qgepplugin/qgepqwat2ili/qgepqwat2ili/gui/gui_importc.ui", self)
        #loadUi(os.path.join(os.path.dirname(__file__), "gui_import.ui"), self)

        #Test ob file ok
        #loadUi(os.path.join(os.path.dirname(__file__), "untitled.ui"), self)


        # 26.7.2023 neu
        self.accepted.connect(self.on_finish)
        # noch nicht klar was tun hier
        #self.rejected.connect(self.xxx)


        self.finished.connect(self.on_finish)
        print("GuiImportconfig init2")

        #breakpoint()
        
        # header = self.treeWidget.header()
        # header.setSectionResizeMode(QHeaderView.ResizeToContents)
        # header.setSectionResizeMode(0, QHeaderView.Stretch)
        
        # Execute the dialog
        # self.resize(iface.mainWindow().size() * 0.75)

        # Remember save skipvalidation_import_checkbox - to do maybe later
        # s = QgsSettings().value("qgep_plugin/skipvalidation_import", False)
        # self.skipvalidation_import_checkbox.setChecked(s == True or s == "true")

        # Remember save skipimport_wizard_checkbox - to do maybe later
        # s = QgsSettings().value("qgep_plugin/skipimport_wizard", False)
        # self.skipimport_wizard_checkbox.setChecked(s == True or s == "true")
        
        # Execute the dialog
        self.resize(iface.mainWindow().size() * 0.75)
        self.show()

    def init_without_session(self):
        """
        Shows the configuration dialog
        """

        # Execute the dialog
        self.resize(iface.mainWindow().size() * 0.75)
        self.show()

    # 27.4.2023 
    def on_finish(self):
        # Remember skipvalidation_import checkbox
        #QgsSettings().setValue("qgep_plugin/skipvalidation_import", self.skipvalidation_import)

        # Remember skipimport_wizard checkbox
        #QgsSettings().setValue("qgep_plugin/skipimport_wizard", self.skipimport_wizard)
        print("finished")

    @property
    def skipvalidation_import(self):
        return self.skipvalidation_import.isChecked()

    @property
    def skipimport_wizard(self):
        return self.skipimport_wizard_checkbox.isChecked()
