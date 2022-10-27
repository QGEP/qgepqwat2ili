import os

from qgis.core import QgsProject, QgsSettings
from qgis.PyQt.QtWidgets import QDialog
from qgis.PyQt.uic import loadUi


class GuiExport(QDialog):
    def __init__(self, parent):
        super().__init__(parent)
        loadUi(os.path.join(os.path.dirname(__file__), "gui_export.ui"), self)

        self.accepted.connect(self.on_accepted)

        # Execute the dialog
        # self.resize(iface.mainWindow().size() * 0.75)

        structures_layers = QgsProject.instance().mapLayersByName("vw_qgep_wastewater_structure")
        if structures_layers:
            self.structures = structures_layers[0].selectedFeatures()
        else:
            self.structures = []

        reaches_layers = QgsProject.instance().mapLayersByName("vw_qgep_reach")
        if reaches_layers:
            self.reaches = reaches_layers[0].selectedFeatures()
        else:
            self.reaches = []

        self.limit_checkbox.setText(
            f"Limit to selection ({len(self.structures)} structures and {len(self.reaches)} reaches)"
        )

        s = QgsSettings().value("qgep_plugin/logs_next_to_file", False)
        self.logs_next_to_file = s == True or s == "true"
        self.save_logs_next_to_file_checkbox.setChecked(self.logs_next_to_file)

    def on_accepted(self):
        self.logs_next_to_file = self.save_logs_next_to_file_checkbox.isChecked()
        QgsSettings().setValue("qgep_plugin/logs_next_to_file", self.logs_next_to_file)

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
