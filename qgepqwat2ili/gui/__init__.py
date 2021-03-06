import logging
import os
import webbrowser
from types import SimpleNamespace

from pkg_resources import parse_version
from qgis.core import Qgis, QgsProject, QgsSettings
from qgis.PyQt.QtWidgets import QApplication, QFileDialog, QProgressDialog, QPushButton
from qgis.utils import iface, plugins
from QgisModelBaker.libili2db import globals, ili2dbconfig, ili2dbutils

from .. import config
from ..qgep.export import qgep_export
from ..qgep.import_ import qgep_import
from ..utils.ili2db import (
    create_ili_schema,
    export_xtf_data,
    import_xtf_data,
    validate_xtf_data,
)
from ..utils.various import CmdException, logger, make_log_path
from .gui_export import GuiExport
from .gui_import import GuiImport

# Always log to temp dir
filename = make_log_path(None, "qgepqwat2ili")
file_handler = logging.FileHandler(filename, mode="w", encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(levelname)-8s %(message)s"))
logger.addHandler(file_handler)


def _show_results(title, message, log_path, level):
    widget = iface.messageBar().createMessage(title, message)
    button = QPushButton(widget)
    button.setText("Show logs")
    button.pressed.connect(lambda p=log_path: webbrowser.open(p))
    widget.layout().addWidget(button)
    iface.messageBar().pushWidget(widget, level)


def show_failure(title, message, log_path):
    return _show_results(title, message, log_path, Qgis.Warning)


def show_success(title, message, log_path):
    return _show_results(title, message, log_path, Qgis.Success)


import_dialog = None


def action_import(plugin, pgservice=None):
    """
    Is executed when the user clicks the importAction tool
    """
    global import_dialog  # avoid garbage collection

    if not configure_from_modelbaker(plugin.iface):
        return

    if pgservice:
        config.PGSERVICE = pgservice

    default_folder = QgsSettings().value("qgep_pluging/last_interlis_path", QgsProject.instance().absolutePath())
    file_name, _ = QFileDialog.getOpenFileName(
        None, plugin.tr("Import file"), default_folder, plugin.tr("Interlis transfer files (*.xtf)")
    )
    if not file_name:
        # Operation canceled
        return
    QgsSettings().setValue("qgep_pluging/last_interlis_path", os.path.dirname(file_name))

    progress_dialog = QProgressDialog("", "", 0, 100, plugin.iface.mainWindow())
    progress_dialog.setCancelButton(None)
    progress_dialog.setModal(True)
    progress_dialog.show()

    # Validating the input file
    progress_dialog.setLabelText("Validating the input file...")
    QApplication.processEvents()
    log_path = make_log_path(None, "validate")
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

    # Prepare the temporary ili2pg model
    progress_dialog.setLabelText("Creating ili schema...")
    QApplication.processEvents()
    log_path = make_log_path(None, "create")
    try:
        create_ili_schema(
            config.ABWASSER_SCHEMA,
            config.ABWASSER_ILI_MODEL,
            log_path,
            recreate_schema=True,
        )
    except CmdException:
        progress_dialog.close()
        show_failure(
            "Could not create the ili2pg schema",
            "Open the logs for more details on the error.",
            log_path,
        )
        return
    progress_dialog.setValue(33)

    # Export from ili2pg model to file
    progress_dialog.setLabelText("Importing XTF data...")
    QApplication.processEvents()
    log_path = make_log_path(None, "import")
    try:
        import_xtf_data(
            config.ABWASSER_SCHEMA,
            file_name,
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
    progress_dialog.setValue(66)

    # Export to the temporary ili2pg model
    progress_dialog.setLabelText("Converting to QGEP...")
    QApplication.processEvents()
    import_dialog = GuiImport(plugin.iface.mainWindow())
    progress_dialog.setValue(100)
    qgep_import(
        precommit_callback=import_dialog.init_with_session,
    )


def action_export(plugin, pgservice=None):
    """
    Is executed when the user clicks the exportAction tool
    """

    if not configure_from_modelbaker(plugin.iface):
        return

    if pgservice:
        config.PGSERVICE = pgservice

    export_dialog = GuiExport(plugin.iface.mainWindow())

    def action_do_export():

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

        progress_dialog = QProgressDialog("", "", 0, 100, plugin.iface.mainWindow())
        progress_dialog.setCancelButton(None)
        progress_dialog.setModal(True)
        progress_dialog.show()

        # Prepare the temporary ili2pg model
        progress_dialog.setLabelText("Creating ili schema...")
        QApplication.processEvents()
        log_path = make_log_path(None, "create")
        try:
            create_ili_schema(
                config.ABWASSER_SCHEMA,
                config.ABWASSER_ILI_MODEL,
                log_path,
                recreate_schema=True,
            )
        except CmdException:
            progress_dialog.close()
            show_failure(
                "Could not create the ili2pg schema",
                "Open the logs for more details on the error.",
                log_path,
            )
            return
        progress_dialog.setValue(25)

        # Export to the temporary ili2pg model
        progress_dialog.setLabelText("Converting from QGEP...")
        QApplication.processEvents()
        qgep_export(selection=export_dialog.selected_ids)
        progress_dialog.setValue(50)

        # Export from ili2pg model to file
        progress_dialog.setLabelText("Saving XTF file...")
        QApplication.processEvents()
        log_path = make_log_path(None, "export")
        try:
            export_xtf_data(
                config.ABWASSER_SCHEMA,
                config.ABWASSER_ILI_MODEL_NAME,
                file_name,
                log_path,
            )
        except CmdException:
            progress_dialog.close()
            show_failure(
                "Could not export the ili2pg schema",
                "Open the logs for more details on the error.",
                log_path,
            )
            return
        progress_dialog.setValue(75)

        progress_dialog.setLabelText("Validating the output file...")
        QApplication.processEvents()
        log_path = make_log_path(None, "validate")
        try:
            validate_xtf_data(
                file_name,
                log_path,
            )
        except CmdException:
            progress_dialog.close()
            show_failure(
                "Invalid file",
                "The created file is not a valid XTF file.",
                log_path,
            )
            return
        progress_dialog.setValue(100)

        show_success(
            "Sucess",
            f"Data successfully exported to {file_name}",
            os.path.dirname(log_path),
        )

    export_dialog.accepted.connect(action_do_export)
    export_dialog.show()


def configure_from_modelbaker(iface):
    """
    Configures config.JAVA/ILI2PG paths using modelbaker.
    Returns whether modelbaker is available, and displays instructions if not.
    """
    REQUIRED_VERSION = "v6.4.0"  # TODO : update once https://github.com/opengisch/QgisModelBaker/pull/473 is released
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
