import os

from qgis.core import QgsProject
from qgis.PyQt.QtWidgets import QDialog
# neu 28.6.2022
#from qgis.PyQt.QtWidgets import QDialog, QComboBox
# from PyQt5.QtWidgets import QComboBox

from qgis.PyQt.uic import loadUi

from qgis.PyQt.QtCore import pyqtSlot

#7.7.2022 
exportmodell = ""

class GuiExport(QDialog):
    def __init__(self, parent):
        super().__init__(parent)
        
        # test mit neuem gui 16.5.2022
        # loadUi(os.path.join(os.path.dirname(__file__), "gui_export.ui"), self)
        loadUi(os.path.join(os.path.dirname(__file__), "gui_export_model_select.ui"), self)
        
        #7.7.2022 / neu in class
        exportmodellclassguiexport = ""
        
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
        
        # neu 16.5.2022 populating QcomboBox
        
        # neu 28.6.2022 https://www.pythonguis.com/docs/qcombobox/
        # Keep a reference to combobox on self, so we can access it in our methods.
        #self.comboBox_modelselection = QComboBox()
        
        self.comboBox_modelselection.clear()
        self.comboBox_modelselection.addItem("DSS_2015_LV95", "qgepdss")
        self.comboBox_modelselection.addItem("SIA405_ABWASSER_2015_LV95", "qgepsia405")
        self.comboBox_modelselection.addItem("VSA_KEK_2019_LV95", "qgepkek" )
        self.comboBox_modelselection.addItem("VSA_KEK_2019_LV95_current", "qgep" )

        # neu 27.5.2022
        self.comboBox_modelselection.currentIndexChanged.connect(self.modelChanged)
  
        #27.6.2022
        self.comboBox_modelselection.activated.connect(self.handleActivated)
        
        #28.6.2022 https://www.pythonguis.com/docs/qcombobox/
        self.comboBox_modelselection.activated.connect(self.current_model)
  
    # neu 27.5.2022
    @pyqtSlot()
    # def modelChanged(self, index): 
    def modelChanged(self):  
        #self.comboBox_modelselection.currentIndexChanged.connect(self.showId)
        if self.comboBox_modelselection.itemData(self.comboBox_modelselection.currentIndex()) == 'DSS_2015_LV95':
            self.labelmodelshortcut.setText("qgepdss")
            print ("Model qgepdss")
        elif self.comboBox_modelselection.itemData(self.comboBox_modelselection.currentIndex()) == 'SIA405_ABWASSER_2015_LV95':
            self.labelmodelshortcut.setText("qgepsia405abwasser")
            print ("Model qgepsia405abwasser")
        elif self.comboBox_modelselection.itemData(self.comboBox_modelselection.currentIndex()) == 'VSA_KEK_2019_LV95':
            self.labelmodelshortcut.setText("qgepkek")
            print ("Model qgepkek")
        
    #neu 28.6.2022 https://www.pythonguis.com/docs/qcombobox/
    def current_model(self, _): # We receive the index, but don't use it.
        cmodel = self.comboBox_modelselection.currentText()
        print("Current model", cmodel)
        self.label_2.setText(cmodel + "----")
        
        # 29.08.2022 hide
        # self.label_2.show
        #self.label_2.setVisible(false);

    #neu 27.6.2022 sb 
    def handleActivated(self, index):
        print(self.comboBox_modelselection.itemText(index))
        print(self.comboBox_modelselection.itemData(index))

    # neu 7.7.2022 - analog wie in qgepdatamodeldialog.py Zeile 218
    @property
    def selected_model(self):
        exportmodell = self.releaseVersionComboBox.currentText()
        print("Exportmodell = " + exportmodell)
        return self.releaseVersionComboBox.currentText()

 #   def showId(self):
 #       id_us = self.comboBox_modelselection.itemData(self.comboBox_modelselection.currentIndex())
#        print('VAL ',id_us)
        
        # self.labelmodelshortcut.setText = id_us
 #       self.labelmodelshortcut.setText = "hallo"


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
