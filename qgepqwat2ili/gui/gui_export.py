import os

from qgis.core import QgsProject
from qgis.PyQt.QtCore import Qt
from qgis.PyQt.QtWidgets import QDialog, QListWidgetItem
from qgis.PyQt.uic import loadUi

from ..processing_algs.extractlabels_interlis import ExtractlabelsInterlisAlgorithm


class GuiExport(QDialog):
    def __init__(self, parent):
        super().__init__(parent)
        loadUi(os.path.join(os.path.dirname(__file__), "gui_export.ui"), self)

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

        # Populate the labels list
        self.labels_scale_list.clear()
        for i, scale_conf in enumerate(ExtractlabelsInterlisAlgorithm.AVAILABLE_SCALES):
            scale_key, scale_disp, scale_val = scale_conf
            item = QListWidgetItem(f"{scale_disp} [1:{scale_val}]")
            item.setData(Qt.UserRole, i)
            self.labels_scale_list.addItem(item)
        self.labels_scale_list.selectAll()
        self.labels_scale_list.setMinimumWidth(self.labels_scale_list.sizeHintForColumn(0) + 10)

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

    @property
    def limit_to_selection(self):
        return self.limit_checkbox.isChecked()

    @property
    def selected_labels_scales_indices(self):
        scales = []
        for item in self.labels_scale_list.selectedItems():
            scales.append(item.data(Qt.UserRole))
        return scales
