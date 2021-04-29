import os

from qgis.core import QgsProject
from qgis.PyQt.QtWidgets import QDialog
from qgis.PyQt.uic import loadUi


class GuiExport(QDialog):
    def __init__(self, parent):
        super().__init__(parent)
        loadUi(os.path.join(os.path.dirname(__file__), "gui_export.ui"), self)

        # Execute the dialog
        # self.resize(iface.mainWindow().size() * 0.75)

        structures_layer = QgsProject.instance().mapLayersByName("vw_qgep_wastewater_structure")[0]
        reaches_layer = QgsProject.instance().mapLayersByName("vw_qgep_reach")[0]

        QgsProject.instance().mapLayersByName("vw_qgep_reach")[0]

        self.structures = structures_layer.selectedFeatures()
        self.reaches = reaches_layer.selectedFeatures()

        self.limit_checkbox.setText(
            f"Limit to selection ({len(self.structures)} structures and {len(self.reaches)} reaches)"
        )

    @property
    def selected_ids(self):
        if self.limit_checkbox.isChecked():
            ids = []
            for struct in self.structures:
                ids.append(str(struct["wn_obj_id"]))
            for reach in self.reaches:
                ids.append(str(reach["obj_id"]))
                ids.append(str(reach["rp_from_fk_wastewater_networkelement"]))
                ids.append(str(reach["rp_to_fk_wastewater_networkelement"]))
            return ids
        else:
            return None
