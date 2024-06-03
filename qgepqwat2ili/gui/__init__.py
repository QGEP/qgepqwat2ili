import logging
import os
import tempfile
import webbrowser
from types import SimpleNamespace

from pkg_resources import parse_version
from qgis import processing
from qgis.core import Qgis, QgsProject, QgsSettings
from qgis.PyQt.QtWidgets import QApplication, QFileDialog, QProgressDialog, QPushButton
from qgis.utils import iface, plugins
from QgisModelBaker.libili2db import globals, ili2dbconfig, ili2dbutils

from ....utils.qgeplayermanager import QgepLayerManager
from .. import config
from ..qgep.export import qgep_export
from ..qgep.import_ import qgep_import

# 12.7.2022 additional models
from ..qgepdss.export import qgep_export as qgepdss_export
from ..qgepdss.import_ import qgep_import as qgepdss_import

# 28.3.2023 additional import for sia405, export to be discussed further
from ..qgepsia405.export import qgep_export as qgepsia405_export
from ..qgepsia405.import_ import qgep_import as qgepsia405_import

from ..utils.ili2db import (
    create_ili_schema,
    export_xtf_data,
    import_xtf_data,
    validate_xtf_data,
    # neu 22.7.2022
    #get_xtf_model,
    get_xtf_model2,
    # neu 31.3.2023
    check_organisation_subclass_data,
    # neu 12.4.2023
    check_wastewater_structure_subclass_data,
    check_identifier_null,
)
from ..utils.various import CmdException, LoggingHandlerContext, logger, make_log_path
from .gui_export import GuiExport
from .gui_import import GuiImport

# 19.4.2023 / 25.4.2023 ohne Bindestrich / neu aus gui_import - Gui
from .gui_importc import GuiImportc

# 12.7.2022 for testing import time
import time


from qgis.PyQt.QtWidgets import QDialog


def _show_results(title, message, log_path, level):
    widget = iface.messageBar().createMessage(title, message)
    button = QPushButton(widget)
    button.setText("Show logs")
    button.pressed.connect(lambda p=log_path: webbrowser.open(p))
    widget.layout().addWidget(button)
    iface.messageBar().pushWidget(widget, level)


def show_failure(title, message, log_path):
    return _show_results(title, message, log_path, Qgis.Warning)

def show_hint(title, message, log_path):
    return _show_results(title, message, log_path, Qgis.Info)
    
def show_success(title, message, log_path):
    return _show_results(title, message, log_path, Qgis.Success)



import_dialog = None


flagskipvalidation_import = False

# 19.4.2023 / 26.7.2023 wieder gesetzt
importc_dialog = None


def action_importc(plugin):

    #neu 26.7.2023 analog action_import
    global importc_dialog  # avoid garbage collection

    flagskipvalidation_import = False
    print("set flagskipvalidation_import")

    
    iface.messageBar().pushMessage("Info", "action import", level=Qgis.Info)

    importc_dialog = GuiImportc(plugin.iface.mainWindow())
    
    
    
    # # 19.4.2023 add option for additional import configuration
    def action_do_importc():
        print("Open import dialog config")
        if importc_dialog.skipvalidation_import:
            flagskipvalidation_import = importc_dialog.skipvalidation_import

        progress_dialog = QProgressDialog("", "", 0, 100, plugin.iface.mainWindow())
        progress_dialog.setCancelButton(None)
        progress_dialog.setModal(True)
        progress_dialog.show()
        progress_dialog.setLabelText("waiting...")
        # delays the execution for 5.5 secs.
        time.sleep(5.5)
        progress_dialog.close
        # end action_do_importc

    importc_dialog.accepted.connect(action_do_importc)
    importc_dialog.adjustSize()
    importc_dialog.show()

    
#def action_import(plugin):
def action_import(plugin):
    """
    Is executed when the user clicks the importAction tool
    """
    if not configure_from_modelbaker(plugin.iface):
        return

    global import_dialog  # avoid garbage collection

    default_folder = QgsSettings().value("qgep_pluging/last_interlis_path", QgsProject.instance().absolutePath())
    file_name, _ = QFileDialog.getOpenFileName(
        None, plugin.tr("Import file"), default_folder, plugin.tr("Interlis transfer files (*.xtf)")
    )
    if not file_name:
        # Operation canceled
        return
    QgsSettings().setValue("qgep_pluging/last_interlis_path", os.path.dirname(file_name))

    # Configure logging
    s = QgsSettings().value("qgep_plugin/logs_next_to_file", False)
    logs_next_to_file = s == True or s == "true"
    if logs_next_to_file:
        base_log_path = file_name
    else:
        base_log_path = None

    progress_dialog = QProgressDialog("", "", 0, 100, plugin.iface.mainWindow())
    progress_dialog.setCancelButton(None)
    progress_dialog.setModal(True)
    progress_dialog.show()

    # Check if validating is selected
    if flagskipvalidation_import:
        print("Validation will be skipped!")
        progress_dialog.setLabelText("No validation of input file...")
    else:
        # Validating the input file
        progress_dialog.setLabelText("Validating the input file...")
        QApplication.processEvents()
        log_path = make_log_path(base_log_path, "ilivalidator")
        try:
            validate_xtf_data(
                file_name,
                log_path,
            )
        except CmdException:
            progress_dialog.close()
            show_failure(
                "Invalid file",
                "The input file is not a valid XTF file. Open the logs for more details on the error.",
                log_path,
            )
            return

    # von unten hierherauf genommen, da sonst nicht ansprechbar
    import_dialog = GuiImport(plugin.iface.mainWindow())
    
    # new 23.7.2022
    global imodel 
    imodel = "nothing"

    
    # 22.7.2022 xtf file checken and get model name as imodel
    try:
        # neu 23.7.2022 imodel mit return aus get_xtf_model
        # new version imodel = get_xtf_model(file_name)
        imodel = get_xtf_model2(file_name)
        
    except CmdException:
        progress_dialog.close()
        show_failure(
            "Could not read modelname from xtf file",
            "Open the logs for more details on the error.",
            log_path,
        )
        return

    # Prepare the temporary ili2pg model
    # 12.7.2022 

    #progress_dialog.setLabelText("Creating ili schema...")
    # 23.7.2022 rausgenommen - da in ili2db gesetzt
    # imodel = import_dialog.label_importmodelname.text()
    tmplabeltext = "Creating ili schema..." + imodel
    print(tmplabeltext)
    # breakpoint()
    progress_dialog.setLabelText(tmplabeltext)
    
    progress_dialog.setValue(5)

    # delays the execution for 5.5 secs.
    # time.sleep(5.5)

    QApplication.processEvents()
    log_path = make_log_path(base_log_path, "ili2pg-schemaimport")
    try:
        # 22.7.2022 create_ili_schema depending of imodel
        if imodel == "VSA_KEK_2019_LV95":
            create_ili_schema(
                config.ABWASSER_SCHEMA,
                config.ABWASSER_ILI_MODEL,
                log_path,
                recreate_schema=True,
                )
        elif imodel == "SIA405_ABWASSER_2015_LV95":
            create_ili_schema(
                config.ABWASSER_SIA405_SCHEMA,
                config.ABWASSER_SIA405_ILI_MODEL,
                log_path,
                recreate_schema=True,
                )
        elif imodel == "DSS_2015_LV95":
            create_ili_schema(
                config.ABWASSER_DSS_SCHEMA,
                config.ABWASSER_DSS_ILI_MODEL,
                log_path,
                recreate_schema=True,
                )
        else: 
            # print(imodel)
            # breakpoint()
            progress_dialog.close()
            show_failure(
                 "MODEL " + imodel + " schema creation failed: Not yet supported for INTERLIS import - no configuration available in config.py / _init_.py",
                 "Open the logs for more details on the error.",
                 log_path,
            )

    except CmdException:
        progress_dialog.close()
        show_failure(
            "Could not create the ili2pg schema" + imodel,
            "Open the logs for more details on the error.",
            log_path,
        )
        return
    progress_dialog.setValue(33)

    # Export from ili2pg model to file
    progress_dialog.setLabelText("Importing XTF data...")

    #time.sleep(6.5)

    QApplication.processEvents()
    log_path = make_log_path(base_log_path, "ili2pg-import")
    try:
#        import_xtf_data(
#            config.ABWASSER_SCHEMA,
#            file_name,
#            log_path,
#        )
        #27.6.2022 to do dependant on Model Selection
        if imodel == "VSA_KEK_2019_LV95":
            import_xtf_data(
                config.ABWASSER_SCHEMA,
                file_name,
                log_path,
                #imodel,
                #"VSA_KEK_2019_LV95",
            )
        elif imodel == "SIA405_ABWASSER_2015_LV95":
            import_xtf_data(
                config.ABWASSER_SIA405_SCHEMA,
                file_name,
                log_path,
                #imodel,
            )
        elif imodel == "DSS_2015_LV95":
            import_xtf_data(
                config.ABWASSER_DSS_SCHEMA,
                file_name,
                log_path,
                #imodel,
            )
        else: 
            # print(imodel)
            # breakpoint()
            progress_dialog.close()
            show_failure(
                 "Import xtf in MODEL " + imodel + " not yet supported for INTERLIS import - no configuration available in config.py / _init_.py",
                 "Open the logs for more details on the error.",
                 log_path,
            )

    except CmdException:
        progress_dialog.close()
        show_failure(
            "Could not import data",
            "Open the logs for more details on the error.",
            log_path,
        )
        return


    # Export to the temporary ili2pg model
    progress_dialog.setLabelText("Converting to QGEP...")
    QApplication.processEvents()

    # 22.7.2022 nach oben verschoben
    # import_dialog = GuiImport(plugin.iface.mainWindow())
    # 25.7.2022 new 80 instead of 100 (to show that still something is happening
    #progress_dialog.setValue(100)
    progress_dialog.setValue(66)



    log_handler = logging.FileHandler(make_log_path(base_log_path, "qgepqwat2ili-import"), mode="w", encoding="utf-8")
    log_handler.setLevel(logging.INFO)
    log_handler.setFormatter(logging.Formatter("%(levelname)-8s %(message)s"))
    with LoggingHandlerContext(log_handler):
#        qgep_import(
#        precommit_callback=import_dialog.init_with_session,
#        )

        progress_dialog.setLabelText("Loading import wizard - please be patient...")
# 24.3.2023 added model dependency
        if imodel == "VSA_KEK_2019_LV95":
            qgep_import(
            precommit_callback=import_dialog.init_with_session,
            )
        elif imodel == "SIA405_ABWASSER_2015_LV95":
            qgepsia405_import(
            precommit_callback=import_dialog.init_with_session,
            )
        elif imodel == "DSS_2015_LV95":
            qgepdss_import(
            precommit_callback=import_dialog.init_with_session,
            )
        else:
            progress_dialog.close()
            show_failure(
                 "Import xtf in qgep with " + imodel + " not yet supported for INTERLIS import - no configuration available in config.py / _init_.py",
                 "Open the logs for more details on the error.",
                 log_path,
            )

    progress_dialog.setLabelText("Set main_cover manually after import if vw_qgep_wastewater_structure does not display correctly!")
    
    time.sleep(2)
    # to add option to run main_cover.sql manually
    
    # 24.7.2022 / moved to end
    progress_dialog.setValue(100)


def action_export(plugin):
    """
    Is executed when the user clicks the exportAction tool
    """

    if not configure_from_modelbaker(plugin.iface):
        return

    export_dialog = GuiExport(plugin.iface.mainWindow())

    def action_do_export():

        # neu 12.7.2022
        emodel = export_dialog.comboBox_modelselection.currentText()
        print (emodel)

        # neu 3.4.2023 added float()
        eorientation = float(export_dialog.comboBox_orientation.currentText())
        print (eorientation)
        
        # Prepare file dialog 
        default_folder = QgsSettings().value("qgep_pluging/last_interlis_path", QgsProject.instance().absolutePath())
        file_name, _ = QFileDialog.getSaveFileName(
            None,
            plugin.tr("Export to file"),
            os.path.join(default_folder, "qgep-export.xtf"),
            plugin.tr("Interlis transfer files (*.xtf)"),
        )
        if not file_name:
            # Operation canceled
            return
        QgsSettings().setValue("qgep_pluging/last_interlis_path", os.path.dirname(file_name))

        # File name without extension (used later for export)
        file_name_base, _ = os.path.splitext(file_name)

        # Configure logging
        if export_dialog.logs_next_to_file:
            base_log_path = file_name
        else:
            base_log_path = None

        progress_dialog = QProgressDialog("", "", 0, 100, plugin.iface.mainWindow())
        progress_dialog.setCancelButton(None)
        progress_dialog.setModal(True)
        progress_dialog.show()

        # 31.3.2023 Integrity checks before starting export
        progress_dialog.setLabelText("Integrity checks for export...")

        # 1. check organisation only for VSA-DSS export
        if emodel == "DSS_2015_LV95":

            check_organisation = False
            check_organisation = check_organisation_subclass_data()
            if check_organisation:
                print("OK: Integrity checks organisation")
                show_success(
                        "Sucess",
                        f"OK: Integrity checks organisation",
                        None,
                    )
            else:
                progress_dialog.close()
                print("number of subclass elements of organisation NOT CORRECT")
                show_failure(
                    "ERROR: number of subclass elements of organisation NOT CORRECT in schmea qgep_od",
                    f"Add missing obj_id in organisation subclasses so that number of subclass elements match organisation elements. See qgep logs tab for details.",
                    None,
                )
                return

        # 2. check wastewater_structure for all data models
        flag_test = False
        if flag_test:
            check_wastewater_structure = False
            check_wastewater_structure = check_wastewater_structure_subclass_data()
            if check_wastewater_structure:
                print("OK: Integrity checks wastewater_structure")
                show_success(
                        "Sucess",
                        f"OK: Integrity checks wastewater_structure",
                        None,
                    )
            else:
                progress_dialog.close()
                print("ERROR: number of subclass elements of wastewater_structure NOT CORRECT")
                show_failure(
                    "ERROR: number of subclass elements of wastewater_structure NOT CORRECT in schmea qgep_od",
                    f"Add missing obj_id in wastewater_structure subclasses so that number of subclass elements match wastewater_structure elements. See qgep logs tab for details.",
                    None,
                )
                return

        # 3. identifier check check_identifier_null
        check_identifier = False
        check_identifier = check_identifier_null()
        if check_identifier:
            print("OK: Integrity checks identifiers not isNull")
            show_success(
                    "Sucess",
                    f"OK: Integrity checks identifiers not isNull",
                    None,
                )

        else:
            progress_dialog.close()
            print("INFO: missing identifiers")
            show_hint(
                "INFO: Missing identifiers in schema qgep_od",
                f"Add missing identifiers to get a valid INTERLIS export file. See qgep logs tab for details.",
                None,
            )
            # just show hint, but continue
            # return


        # Prepare the temporary ili2pg model
        progress_dialog.setLabelText("Creating ili schema..." + emodel)

        QApplication.processEvents()
        log_path = make_log_path(base_log_path, "ili2pg-schemaimport")

# 28.3.2023 replaced by else        try:

        # 28.6.2022 https://pythontect.com/python-configparser-tutorial/
        if emodel == "VSA_KEK_2019_LV95":
        # alte Konfiguration behalten
            create_ili_schema(
                config.ABWASSER_SCHEMA,
                config.ABWASSER_ILI_MODEL,
                log_path,
                recreate_schema=True,
                )
        elif emodel == "SIA405_ABWASSER_2015_LV95":
            create_ili_schema(
                config.ABWASSER_SIA405_SCHEMA,
                config.ABWASSER_SIA405_ILI_MODEL,
                log_path,
                recreate_schema=True,
                )
        elif emodel == "DSS_2015_LV95":
            create_ili_schema(
                config.ABWASSER_DSS_SCHEMA,
                config.ABWASSER_DSS_ILI_MODEL,
                log_path,
                recreate_schema=True,
                )

        #to do 27.3.2023 else instead of except? discuss with OD
        else:
            progress_dialog.close()
            show_failure(
                "Could not create the ili2pg schema - selected model not supported",
                emodel,
                None,
            )
            return
        
# 28.3.2023 replaced by else            
        # except CmdException:
            # progress_dialog.close()
            # show_failure(
                # "Could not create the ili2pg schema",
                # "Open the logs for more details on the error.",
                # log_path,
            # )
            # return

#        progress_dialog.setValue(25)
        progress_dialog.setValue(5)

        # neu 12.7.2022
        progress_dialog.setLabelText(emodel)
        
        #print("GFG printed immediately.")
        #time.sleep(5.5)
          
        # delays the execution
        # for 5.5 secs.
        #print("GFG printed after 5.5 secs.")

        progress_dialog.setValue(25)
        
        # Export the labels file
        tempdir = tempfile.TemporaryDirectory()
        labels_file_path = None

        if len(export_dialog.selected_labels_scales_indices):
            labels_file_path = os.path.join(tempdir.name, "labels.geojson")

            progress_dialog.setLabelText("Extracting labels...")

            structures_lyr = QgepLayerManager.layer("vw_qgep_wastewater_structure")
            reaches_lyr = QgepLayerManager.layer("vw_qgep_reach")
            #4.4.2023 add catchment_area 
            catchment_area_lyr = QgepLayerManager.layer("catchment_area")
            #if not structures_lyr or not reaches_lyr:
            if not structures_lyr or not reaches_lyr or not catchment_area_lyr:
                progress_dialog.close()
                show_failure(
                    "Could not find the vw_qgep_wastewater_structure and/or the vw_qgep_reach and/or catchment_area layers.",
                    "Make sure your QGEP project is open.",
                    None,
                )
                return

            QApplication.processEvents()
            processing.run(
                "qgep:extractlabels_interlis",
                {
                    "OUTPUT": labels_file_path,
                    "RESTRICT_TO_SELECTION": export_dialog.limit_to_selection,
                    "STRUCTURE_VIEW_LAYER": structures_lyr,
                    "REACH_VIEW_LAYER": reaches_lyr,
                    "CATCHMENT_AREA_VIEW_LAYER": catchment_area_lyr,
                    "SCALES": export_dialog.selected_labels_scales_indices,
                },
            )
            progress_dialog.setValue(35)

        # Export to the temporary ili2pg model
        progress_dialog.setLabelText("Converting from QGEP...")
        QApplication.processEvents()

        log_handler = logging.FileHandler(make_log_path(file_name, "qgepqwat2ili-export"), mode="w", encoding="utf-8")
        log_handler.setLevel(logging.INFO)
        log_handler.setFormatter(logging.Formatter("%(levelname)-8s %(message)s"))
        with LoggingHandlerContext(log_handler):
#18.3.2023
# 22.3.2023 added try - seems not to work
#            try:
                if emodel == "VSA_KEK_2019_LV95":
                    logger.info("Start Exporting VSA_KEK_2019_LV95 - qgep_export")
                    #qgep_export(selection=export_dialog.selected_ids, labels_file=labels_file_path) 
                    # 3.4.2023 neu mit eorientation
                    qgep_export(selection=export_dialog.selected_ids, labels_file=labels_file_path, orientation=eorientation) 
                # 22.3.2023 / 28.3.2023 adjusted to qgepsia405_export
                elif emodel == "SIA405_ABWASSER_2015_LV95":
                    logger.info("Start Exporting SIA405_ABWASSER_2015_LV95 - qgepsia405_export")
                    #qgepsia405_export(selection=export_dialog.selected_ids, labels_file=labels_file_path)
                    # 3.4.2023 neu mit eorientation
                    qgepsia405_export(selection=export_dialog.selected_ids, labels_file=labels_file_path, orientation=eorientation)
                elif emodel == "DSS_2015_LV95":
                    logger.info("Start Exporting DSS_2015_LV95 - qgepdss_export")
                    #qgepdss_export(selection=export_dialog.selected_ids, labels_file=labels_file_path)
                    # 3.4.2023 neu mit eorientation
                    qgepdss_export(selection=export_dialog.selected_ids, labels_file=labels_file_path, orientation=eorientation)
                else:
                    progress_dialog.close()
                    show_failure(
                         "Could not export data for model " + emodel,
                         "Model not yet supported on export!",
                         log_path,
                    )
                    return
 
        progress_dialog.setValue(51)

        # Cleanup
        tempdir.cleanup()


        #12.7.2022 to do dependant on Model Selection
        if emodel == "VSA_KEK_2019_LV95":
            for model_name, export_model_name, progress in [
                (config.ABWASSER_ILI_MODEL_NAME, None, 50),
                (config.ABWASSER_ILI_MODEL_NAME_SIA405, config.ABWASSER_ILI_MODEL_NAME_SIA405, 70),
            ]:

                export_file_name = f"{file_name_base}_{model_name}.xtf"

                # Export from ili2pg model to file
                progress_dialog.setLabelText(f"Saving XTF file [{model_name}]...")
                QApplication.processEvents()
                log_path = make_log_path(base_log_path, f"ili2pg-export-{model_name}")
                try:
                    export_xtf_data(
                        config.ABWASSER_SCHEMA,
                        model_name,
                        export_model_name,
                        export_file_name,
                        log_path,
                    )
                except CmdException:
                    progress_dialog.close()
                    show_failure(
                        "Could not export the ili2pg schema " + config.ABWASSER_SCHEMA,
                        "Open the logs for more details on the error.",
                        log_path,
                    )
                    continue
                progress_dialog.setValue(progress + 10)

                progress_dialog.setLabelText(f"Validating the network output file [{model_name}]...")
                QApplication.processEvents()
                log_path = make_log_path(base_log_path, f"ilivalidator-{model_name}")
                try:
                    validate_xtf_data(
                        export_file_name,
                        log_path,
                    )
                    
                    #24.3.2023 moved up here
                    show_success(
                        "Sucess",
                        #f"Data successfully exported to {file_name_base}",
                        f"Data successfully exported to {export_file_name}",
                        os.path.dirname(log_path),
                    )
                except CmdException:
                    progress_dialog.close()
                    show_failure(
                        "Invalid file",
                        f"The created file is not a valid {model_name} XTF file. Open the logs for more details on the error.",
                        log_path,
                    )
                    continue

                progress_dialog.setValue(progress + 20)

        elif emodel == "DSS_2015_LV95":
            for model_name, export_model_name, progress in [
                (config.ABWASSER_DSS_ILI_MODEL_NAME, None, 50),
            ]:

                export_file_name = f"{file_name_base}_{model_name}.xtf"

                # Export from ili2pg model to file
                progress_dialog.setLabelText(f"Saving XTF file [{model_name}]...")
                QApplication.processEvents()
                log_path = make_log_path(base_log_path, f"ili2pg-export-{model_name}")
                try:
                    export_xtf_data(
                        config.ABWASSER_DSS_SCHEMA,
                        model_name,
                        export_model_name,
                        export_file_name,
                        log_path,
                    )
                except CmdException:
                    progress_dialog.close()
                    show_failure(
                        "Could not export the ili2pg schema " + config.ABWASSER_DSS_SCHEMA,
                        "Open the logs for more details on the error.",
                        log_path,
                    )
                    continue

                progress_dialog.setValue(progress + 10)

                progress_dialog.setLabelText(f"Validating the GEP output file [{model_name}]...")
                QApplication.processEvents()
                log_path = make_log_path(base_log_path, f"ilivalidator-{model_name}")
                try:
                    validate_xtf_data(
                        export_file_name,
                        log_path,
                    )
                    
                    #24.3.2023 moved up here
                    show_success(
                        "Sucess",
                        f"Data successfully exported to {file_name_base}",
                        os.path.dirname(log_path),
                    )
                    
                except CmdException:
                    progress_dialog.close()
                    show_failure(
                        "Invalid file",
                        f"The created file is not a valid {model_name} XTF file. Open the logs for more details on the error.",
                        log_path,
                    )
                    continue

                progress_dialog.setValue(progress + 20)

        # 29.3.2023 SIA405_ABWASSER_2015_LV95
        elif emodel == "SIA405_ABWASSER_2015_LV95":
            for model_name, export_model_name, progress in [
                # (config.ABWASSER_DSS_ILI_MODEL_NAME, None, 50),
                (config.ABWASSER_SIA405_ILI_MODEL_NAME, None, 50),
            ]:

                export_file_name = f"{file_name_base}_{model_name}.xtf"

                # Export from ili2pg model to file
                progress_dialog.setLabelText(f"Saving XTF file [{model_name}]...")
                QApplication.processEvents()
                log_path = make_log_path(base_log_path, f"ili2pg-export-{model_name}")
                try:
                    export_xtf_data(
                        # config.ABWASSER_DSS_SCHEMA,
                        config.ABWASSER_SIA405_SCHEMA,
                        model_name,
                        export_model_name,
                        export_file_name,
                        log_path,
                    )
                except CmdException:
                    progress_dialog.close()
                    show_failure(
                        "Could not export the ili2pg schema " + config.ABWASSER_SIA405_SCHEMA,
                        "Open the logs for more details on the error.",
                        log_path,
                    )
                    continue

                progress_dialog.setValue(progress + 10)

                progress_dialog.setLabelText(f"Validating the GEP output file [{model_name}]...")
                QApplication.processEvents()
                log_path = make_log_path(base_log_path, f"ilivalidator-{model_name}")
                try:
                    validate_xtf_data(
                        export_file_name,
                        log_path,
                    )
                    
                    #24.3.2023 moved up here
                    show_success(
                        "Sucess",
                        f"Data successfully exported to {file_name_base}",
                        os.path.dirname(log_path),
                    )
                    
                except CmdException:
                    progress_dialog.close()
                    show_failure(
                        "Invalid file",
                        f"The created file is not a valid {model_name} XTF file. Open the logs for more details on the error.",
                        log_path,
                    )
                    continue

                progress_dialog.setValue(progress + 20)

        else:
           progress_dialog.close()
           show_failure(
              "No supported model",
              f"The selected {emodel} is not supported yet.",
              log_path,
           )
           return
            
        progress_dialog.setValue(100)

        # show_success(
            # "Sucess",
            # f"Data successfully exported to {file_name_base}",
            # os.path.dirname(log_path),
        # )

    export_dialog.accepted.connect(action_do_export)
    export_dialog.adjustSize()
    export_dialog.show()


def configure_from_modelbaker(iface):
    """
    Configures config.JAVA/ILI2PG paths using modelbaker.
    Returns whether modelbaker is available, and displays instructions if not.
    """
    REQUIRED_VERSION = "v6.5.2"
    modelbaker = plugins.get("QgisModelBaker")
    if modelbaker is None:
        iface.messageBar().pushMessage(
            "Error",
            "This feature requires the ModelBaker plugin. Please install and activate it from the plugin manager.",
            level=Qgis.Critical,
        )
        return False

    elif modelbaker.__version__ != "dev" and parse_version(modelbaker.__version__) < parse_version(REQUIRED_VERSION):
        iface.messageBar().pushMessage(
            "Error",
            f"This feature requires a more recent version of the ModelBaker plugin (currently : {modelbaker.__version__}). Please install and activate version {REQUIRED_VERSION} or newer from the plugin manager.",
            level=Qgis.Critical,
        )
        return False

    # We reuse modelbaker's logic to get the java path and ili2pg executables from withing QGIS
    # Maybe we could reuse even more (IliExecutable...) ?

    stdout = SimpleNamespace()
    stdout.emit = logger.info
    stderr = SimpleNamespace()
    stderr.emit = logger.error

    config.JAVA = ili2dbutils.get_java_path(ili2dbconfig.BaseConfiguration())
    config.ILI2PG = ili2dbutils.get_ili2db_bin(globals.DbIliMode.ili2pg, 4, stdout, stderr)

    return True
