import os
import sys

from qgis.core import QgsProject
from qgis.PyQt.QtWidgets import QDialog
from qgis.PyQt.uic import loadUi
from qgis.utils import iface

# Required for loadUi to find the custom widget
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))


class GuiExport(QDialog):
    def __init__(self, parent):
        super().__init__(parent)
        loadUi(os.path.join(os.path.dirname(__file__), "gui_export.ui"), self)

        # Execute the dialog
        # self.resize(iface.mainWindow().size() * 0.75)

        self.structures_layer = QgsProject.instance().mapLayersByName("vw_qgep_wastewater_structure")[0]
        self.upstream_widget.set_layer(self.structures_layer)
        self.upstream_widget.set_canvas(iface.mapCanvas())
        self.downstream_widget.set_layer(self.structures_layer)
        self.downstream_widget.set_canvas(iface.mapCanvas())

    @property
    def upstream_id(self):
        try:
            return self.upstream_widget.feature["wn_obj_id"]
        except KeyError:
            return None

    @property
    def downstream_id(self):
        try:
            return self.downstream_widget.feature["wn_obj_id"]
        except KeyError:
            return None
