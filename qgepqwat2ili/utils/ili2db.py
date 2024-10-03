import collections

# 11.4.2023
import xml.etree.ElementTree as ET

import psycopg2
from sqlalchemy.ext.automap import AutomapBase

from .. import config
from .various import exec_, get_pgconf_as_ili_args, get_pgconf_as_psycopg2_dsn, logger


# Checking if subclass entries of organisation are set and match number of organisation entries
def check_organisation_subclass_data():

    logger.info("INTEGRITY CHECK organisations subclass data...")
    print("INTEGRITY CHECK organisations subclass data...")

    connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
    connection.set_session(autocommit=True)
    cursor = connection.cursor()

    cursor.execute("SELECT obj_id FROM qgep_od.organisation;")
    if cursor.rowcount > 0:
        organisation_count = cursor.rowcount
        logger.info(f"Number of organisation datasets: {organisation_count}")
        for subclass in [
            ("administrative_office"),
            ("waste_water_association"),
            ("municipality"),
            ("canton"),
            ("cooperative"),
            ("private"),
            ("waste_water_treatment_plant"),
        ]:
            cursor.execute(f"SELECT obj_id FROM qgep_od.{subclass};")
            logger.info(f"Number of {subclass} datasets: {cursor.rowcount}")
            organisation_count = organisation_count - cursor.rowcount

        if organisation_count == 0:
            organisation_subclass_check = True
            logger.info(
                "OK: number of subclass elements of class organisation OK in schema qgep_od!"
            )
        else:
            organisation_subclass_check = False
            logger.info(
                f"ERROR: number of subclass elements of organisation NOT CORRECT in schema qgep_od: checksum = {organisation_count} (positiv number means missing entries, negativ means too many subclass entries)"
            )
            print(
                f"ERROR: number of subclass elements of organisation NOT CORRECT in schema qgep_od: checksum = {organisation_count} (positiv number means missing entries, negativ means too many subclass entries)"
            )

    return organisation_subclass_check


# Checking if subclass entries of wastewater_structure are set and match number of wastewater_structure entries
def check_wastewater_structure_subclass_data():

    logger.info("INTEGRITY CHECK wastewater_structures subclass data...")
    print("INTEGRITY CHECK wastewater_structures subclass data...")

    connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
    connection.set_session(autocommit=True)
    cursor = connection.cursor()

    cursor.execute("SELECT obj_id FROM qgep_od.wastewater_structure;")
    if cursor.rowcount > 0:
        wastewater_structure_count = cursor.rowcount
        logger.info(f"Number of wastewater_structure datasets: {wastewater_structure_count}")
        for subclass in [
            ("manhole"),
            ("channel"),
            ("special_structure"),
            ("infiltration_installation"),
            ("discharge_point"),
            ("wwtp_structure"),
        ]:
            cursor.execute(f"SELECT obj_id FROM qgep_od.{subclass};")
            logger.info(f"Number of {subclass} datasets: {cursor.rowcount}")
            wastewater_structure_count = wastewater_structure_count - cursor.rowcount

        if wastewater_structure_count == 0:
            wastewater_structure_subclass_check = True
            logger.info(
                "OK: number of subclass elements of class wastewater_structure OK in schema qgep_od!"
            )
        else:
            wastewater_structure_subclass_check = False
            logger.info(
                f"ERROR: number of subclass elements of wastewater_structure NOT CORRECT in schema qgep_od: checksum = {wastewater_structure_count} (positiv number means missing entries, negativ means too many subclass entries)"
            )
            print(
                f"ERROR: number of subclass elements of wastewater_structure NOT CORRECT in schema qgep_od: checksum = {wastewater_structure_count} (positiv number means missing entries, negativ means too many subclass entries)"
            )

    return wastewater_structure_subclass_check


# Checking if attribute identifier is Null
def check_identifier_null():

    logger.info("INTEGRITY CHECK missing idientifiers...")
    print("INTEGRITY CHECK missing idientifiers...")

    connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
    connection.set_session(autocommit=True)
    cursor = connection.cursor()

    missing_identifier_count = 0
    for notsubclass in [
        # VSA-KEK
        ("file"),
        ("data_media"),
        ("maintenance_event"),
        # SIA405 Abwasser
        ("organisation"),
        ("wastewater_structure"),
        ("wastewater_networkelement"),
        ("structure_part"),
        ("reach_point"),
        ("pipe_profile"),
        # VSA-DSS
        ("catchment_area"),
        ("connection_object"),
        ("control_center"),
        ("hazard_source"),
        ("hydr_geometry"),
        ("hydraulic_char_data"),
        ("measurement_result"),
        ("measurement_series"),
        ("measuring_device"),
        ("measuring_point"),
        ("mechanical_pretreatment"),
        ("overflow"),
        ("overflow_char"),
        ("retention_body"),
        ("river_bank"),
        ("river_bed"),
        ("sector_water_body"),
        ("substance"),
        ("surface_runoff_parameters"),
        ("surface_water_bodies"),
        ("throttle_shut_off_unit"),
        ("waste_water_treatment"),
        ("water_catchment"),
        ("water_control_structure"),
        ("water_course_segment"),
        ("wwtp_energy_use"),
        ("zone"),
    ]:
        cursor.execute(
            f"SELECT COUNT(obj_id) FROM qgep_od.{notsubclass} WHERE identifier is null;"
        )
        # use cursor.fetchone()[0] instead of cursor.rowcount
        logger.info(
            f"Number of datasets in {notsubclass} without identifier : {cursor.fetchone()[0]}"
        )

        if cursor.fetchone() is None:
            missing_identifier_count = missing_identifier_count
        else:
            missing_identifier_count = missing_identifier_count + int(cursor.fetchone()[0])

    if missing_identifier_count == 0:
        identifier_null_check = True
        logger.info("OK: all identifiers set in qgep_od!")
    else:
        identifier_null_check = False
        logger.info(f"ERROR: Missing identifiers in qgep_od: {missing_identifier_count}")
        print(f"ERROR: Missing identifiers: {missing_identifier_count}")

    return identifier_null_check

# Checking if MAMDATORY eigentuemerref not is Null
def check_fk_owner_null():

    logger.info("INTEGRITY CHECK missing MAMDATORY owner references fk_owner...")
    print("INTEGRITY CHECK missing MAMDATORY owner references fk_owner...")

    connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
    connection.set_session(autocommit=True)
    cursor = connection.cursor()

    missing_fk_owner_count = 0
    for notsubclass in [
        # SIA405 Abwasser
        ("wastewater_structure"),
    ]:
        cursor.execute(
            f"SELECT COUNT(obj_id) FROM qgep_od.{missing_fk_owner_count} WHERE fk_owner is null;"
        )
        # use cursor.fetchone()[0] instead of cursor.rowcount
        logger.info(
            f"Number of datasets in {notsubclass} without fk_owner : {cursor.fetchone()[0]}"
        )

        if cursor.fetchone() is None:
            missing_fk_owner_count = missing_fk_owner_count
        else:
            missing_fk_owner_count = missing_fk_owner_count + int(cursor.fetchone()[0])

    if missing_fk_owner_count == 0:
        check_fk_owner_null = True
        logger.info("OK: all mandatory fk_owner set in qgep_od!")
    else:
        missing_fk_owner_count = False
        logger.info(f"ERROR: Missing mandatory fk_owner in qgep_od: {missing_fk_owner_count}")
        print(f"ERROR: Missing mandatory fk_owner: {missing_fk_owner_count}")

    return check_fk_owner_null

# Checking if MAMDATORY eigentuemerref not is Null
def check_fk_operator_null():

    logger.info("INTEGRITY CHECK missing MAMDATORY operator references fk_operator...")
    print("INTEGRITY CHECK missing MAMDATORY operator references fk_operator...")

    connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
    connection.set_session(autocommit=True)
    cursor = connection.cursor()

    missing_fk_operator_count = 0
    for notsubclass in [
        # SIA405 Abwasser
        ("wastewater_structure"),
    ]:
        cursor.execute(
            f"SELECT COUNT(obj_id) FROM qgep_od.{missing_fk_operator_count} WHERE fk_operator is null;"
        )
        # use cursor.fetchone()[0] instead of cursor.rowcount
        logger.info(
            f"Number of datasets in {notsubclass} without fk_operator : {cursor.fetchone()[0]}"
        )

        if cursor.fetchone() is None:
            missing_fk_operator_count = missing_fk_operator_count
        else:
            missing_fk_operator_count = missing_fk_operator_count + int(cursor.fetchone()[0])

    if missing_fk_operator_count == 0:
        check_fk_operator_null = True
        logger.info("OK: all mandatory fk_operator set in qgep_od!")
    else:
        missing_fk_operator_count = False
        logger.info(f"ERROR: Missing mandatory fk_operator in qgep_od: {missing_fk_operator_count}")
        print(f"ERROR: Missing mandatory fk_operator: {missing_fk_operator_count}")

    return check_fk_operator_null

# Checking if MAMDATORY datenherrref not is Null
def check_fk_dataowner_null():

    logger.info("INTEGRITY CHECK missing dataowner references fk_dataowner...")
    print("INTEGRITY CHECK missing dataowner references fk_dataowner...")

    connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
    connection.set_session(autocommit=True)
    cursor = connection.cursor()

    missing_fk_dataowner_count = 0
    for notsubclass in [
        # VSA-KEK
        ("file"),
        ("data_media"),
        ("maintenance_event"),
        # SIA405 Abwasser
        ("organisation"),
        ("wastewater_structure"),
        ("wastewater_networkelement"),
        ("structure_part"),
        ("reach_point"),
        ("pipe_profile"),
        # VSA-DSS
        ("catchment_area"),
        ("connection_object"),
        ("control_center"),
        ("hazard_source"),
        ("hydr_geometry"),
        ("hydraulic_char_data"),
        ("measurement_result"),
        ("measurement_series"),
        ("measuring_device"),
        ("measuring_point"),
        ("mechanical_pretreatment"),
        ("overflow"),
        ("overflow_char"),
        ("retention_body"),
        ("river_bank"),
        ("river_bed"),
        ("sector_water_body"),
        ("substance"),
        ("surface_runoff_parameters"),
        ("surface_water_bodies"),
        ("throttle_shut_off_unit"),
        ("waste_water_treatment"),
        ("water_catchment"),
        ("water_control_structure"),
        ("water_course_segment"),
        ("wwtp_energy_use"),
        ("zone"),
    ]:
        cursor.execute(
            f"SELECT COUNT(obj_id) FROM qgep_od.{missing_fk_dataowner_count} WHERE fk_dataowner is null;"
        )
        # use cursor.fetchone()[0] instead of cursor.rowcount
        logger.info(
            f"Number of datasets in {notsubclass} without fk_dataowner : {cursor.fetchone()[0]}"
        )

        if cursor.fetchone() is None:
            missing_fk_dataowner_count = missing_fk_dataowner_count
        else:
            missing_fk_dataowner_count = missing_fk_dataowner_count + int(cursor.fetchone()[0])

    if missing_fk_dataowner_count == 0:
        check_fk_dataowner_null = True
        logger.info("OK: all mandatory fk_dataowner set in qgep_od!")
    else:
        missing_fk_dataowner_count = False
        logger.info(f"ERROR: Missing mandatory fk_dataowner in qgep_od: {missing_fk_dataowner_count}")
        print(f"ERROR: Missing mandatory fk_dataowner: {missing_fk_dataowner_count}")

    return check_fk_dataowner_null



def create_ili_schema(schema, model, log_path, recreate_schema=False):
    logger.info("CONNECTING TO DATABASE...")

    connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
    connection.set_session(autocommit=True)
    cursor = connection.cursor()

    if not recreate_schema:
        # If the schema already exists, we just truncate all tables
        cursor.execute(
            f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema}';"
        )
        if cursor.rowcount > 0:
            logger.info(f"Schema {schema} already exists, we truncate instead")
            cursor.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}';"
            )
            for row in cursor.fetchall():
                cursor.execute(f"TRUNCATE TABLE {schema}.{row[0]} CASCADE;")
            return

    logger.info(f"DROPPING THE SCHEMA {schema}...")
    cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE ;')
    logger.info(f"CREATING THE SCHEMA {schema}...")
    cursor.execute(f'CREATE SCHEMA "{schema}";')
    connection.commit()
    connection.close()

    logger.info(f"ILIDB SCHEMAIMPORT INTO {schema}...")
    exec_(
        " ".join(
            [
                f'"{config.JAVA}"',
                "-jar",
                f'"{config.ILI2PG}"',
                "--schemaimport",
                *get_pgconf_as_ili_args(),
                "--dbschema",
                f"{schema}",
                "--setupPgExt",
                "--createGeomIdx",
                "--createFk",
                "--createFkIdx",
                "--createTidCol",
                "--importTid",
                "--noSmartMapping",
                "--defaultSrsCode",
                "2056",
                "--log",
                f'"{log_path}"',
                "--nameLang",
                "de",
                f"{model}",
            ]
        )
    )


def validate_xtf_data(xtf_file, log_path):
    logger.info("VALIDATING XTF DATA...")
    exec_(
        f'"{config.JAVA}" -jar "{config.ILIVALIDATOR}" --modeldir "{config.ILI_FOLDER}" --log "{log_path}" "{xtf_file}"'
    )


# 22.7.2022 sb
def get_xtf_model(xtf_file):
    logger.info("GET XTF MODEL... ")
    print("xtf_file: " + xtf_file)
    # logger.info("vorher" + imodel)
    # funktioniert nicht
    # global imodel # define imodel as global variable for import model name
    # impmodel = ""

    # open and read xtf / xml file line by line until <DATASECTION>
    # <DATASECTION>
    # <VSA_KEK_2019_LV95.KEK BID="VSA_KEK_2019_LV95.KEK">
    # read string between < and . -> eg. VSA_KEK_2019_LV95
    # impmodel
    from io import open

    model_list = []

    # checkdatasection = -1
    checkmodelssection = -1
    impmodel = "not found"

    with open(xtf_file, encoding="utf-8") as f:
        while True:
            # if checkdatasection == -1:
            if checkmodelssection == -1:

                line = f.readline()
                if not line:
                    break
                else:
                    # checkdatasection = line.find('<DATASECTION>')
                    # logger.info(str(checkdatasection))
                    # print("checkdatasection (ili2db): " + str(checkdatasection))
                    checkmodelssection = line.find("<MODELS>")
                    logger.info(str(checkmodelssection))
                    print("checkmodelssection (ili2db): " + str(checkmodelssection))
                    logger.info(str(line))
                    print(line)
            else:
                line2 = f.readline()
                if not line2:
                    break
                else:
                    logger.info(str(line2))
                    logger.info("line2: ", str(line2))
                    print(line2)
                    # logger.info(str(checkdatasection))
                    # print("checkdatasection (ili2db): " + str(checkdatasection))
                    logger.info("checkmodelssection2 " + str(checkmodelssection))
                    print("checkmodelssection2 (ili2db): " + str(checkmodelssection))
                    # strmodel = str(line2.strip())
                    strmodel = str(line2)
                    strmodel = strmodel.strip()
                    print("strmodel (ili2db): " + strmodel)
                    print(f"strmodel (ili2db): {strmodel}")
                    logger.info("strmodel: " + strmodel)
                    logger.info("strmodel: ", strmodel)
                    logger.info(f"strmodel: {strmodel}")
                    a = strmodel.find("</MODELS>")
                    logger.info("strmodel.find a </MODELS>: " + str(a))
                    print("strmodel.find a </MODELS>: " + str(a))
                    # if strmodel.find("</MODELS>") > -1:
                    if a == -1:
                        b = strmodel.find("<MODEL>")
                        logger.info(r"strmodel.find b \<MODEL: " + str(b))
                        print(r"strmodel.find b \<MODEL: " + str(b))
                        if strmodel.find("<MODEL") > -1:
                            print("strmodel (ili2db): " + strmodel)
                            logger.info("MODELS definition found in xtf: " + strmodel)
                            # <VSA_KEK_2019_LV95.KEK BID="VSA_KEK_2019_LV95.KEK">
                            # read string between < and . -> eg. VSA_KEK_2019_LV95

                            # result = re.search('<(.*).',strmodel)
                            # result = str(result.group(1))
                            # result2 = result.split('.',1)
                            # result3 = str(result2[0])
                            # result4 = result3.strip('<')
                            # impmodel = str(result4)
                            # Search MODELNAME in MODEL entry:     # <MODEL NAME="VSA_KEK_2019_LV95" VERSION="20.01.2021" URI="http://www.vsa.ch/models" />
                            char1 = "="
                            char2 = "VERSION="
                            result = strmodel[strmodel.find(char1) + 1 : strmodel.find(char2)]
                            # result = re.search('<(.*).',strmodel)
                            # result = str(result.group(1))
                            # result2 = result.split('.',1)
                            # result3 = str(result2[0])
                            # result4 = result3.strip('<')
                            # impmodel = str(result4)
                            # strip spaces
                            result = result.strip()
                            # strip "
                            result = result.strip('"')
                            logger.info("MODEL found: " + str(result))
                            logger.info(result)
                            print("MODEL found: " + str(result) + "*")
                            model_list.append(result)
                        else:
                            print("goto next line")
                    else:
                        print(r"<\/MODEL> found - stop checking!")
                        logger.info("</MODEL> found - stop checking!")
                        break
    print(model_list)
    logger.info("model_list:")
    logger.info(str(model_list))

    if len(model_list) > 0:
        # if impmodel == "not found":
        # # write that MODEL was not found
        # logger.info("MODEL was " + impmodel + " was not found!")
        # else:
        if "VSA_KEK_2019_LV95" in model_list:
            impmodel = "VSA_KEK_2019_LV95"
        elif "SIA405_ABWASSER_2015_LV95" in model_list:
            impmodel = "SIA405_ABWASSER_2015_LV95"
        elif "DSS_2015_LV95" in model_list:
            impmodel = "DSS_2015_LV95"
        elif "SIA405_WASSER_LV95" in model_list:
            impmodel = "SIA405_WASSER_LV95"
        else:
            logger.info("None of the supported models was found!")
    else:
        # write that MODEL was not found
        logger.info("MODEL information was " + impmodel + "!")

    # close xtf file to avoid conflicts
    f.close()

    logger.info("MODEL found: " + str(impmodel))
    print("MODEL found: ", str(impmodel))

    # neu 23.7.2022 return imodel from get_xtf_model so it can be called in _init_.py
    return impmodel


def get_xtf_model2(xtf_file):
    logger.info("GET XTF MODEL xml version... ")
    print("xtf_file: " + xtf_file)
    # logger.info("vorher" + imodel)
    # funktioniert nicht
    # global imodel # define imodel as global variable for import model name
    # impmodel = ""

    # open and read xtf / xml file line by line until <DATASECTION>
    # <DATASECTION>
    # <VSA_KEK_2019_LV95.KEK BID="VSA_KEK_2019_LV95.KEK">
    # read string between < and . -> eg. VSA_KEK_2019_LV95
    # impmodel

    model_list = []

    # checkdatasection = -1
    impmodel = "not found"

    # from xml file
    tree = ET.parse(xtf_file)
    rootinterlis = tree.getroot()
    print("rootinterlis.findall:", rootinterlis.findall("."))
    logger.info("rootinterlis.findall:", rootinterlis.findall("."))

    i = 0
    model_found = False

    while i < 15:
        try:
            j = i
            i = i + 1
            model_list.append(rootinterlis[0][0][j].get("NAME"))
            model_found = True
        # except utils.various.CmdException:
        except Exception:
            if model_found:
                logger.info(f"{i - 1} times MODEL information was found!")
                break
            else:
                logger.info("No MODEL information was found!")
                break

    print(model_list)
    logger.info("model_list:")
    logger.info(str(model_list))

    if len(model_list) > 0:
        # if impmodel == "not found":
        # # write that MODEL was not found
        # logger.info("MODEL was " + impmodel + " was not found!")
        # else:
        if "VSA_KEK_2019_LV95" in model_list:
            impmodel = "VSA_KEK_2019_LV95"
        elif "SIA405_ABWASSER_2015_LV95" in model_list:
            impmodel = "SIA405_ABWASSER_2015_LV95"
        elif "DSS_2015_LV95" in model_list:
            impmodel = "DSS_2015_LV95"
        elif "SIA405_WASSER_LV95" in model_list:
            impmodel = "SIA405_WASSER_LV95"
        else:
            logger.info("None of the supported models was found!")
    else:
        # write that MODEL was not found
        logger.info("MODEL information was " + impmodel + "!")

    logger.info("MODEL found: " + str(impmodel))
    print("MODEL found: ", str(impmodel))

    # neu 23.7.2022 return imodel from get_xtf_model so it can be called in _init_.py
    return impmodel


def import_xtf_data(schema, xtf_file, log_path):
    logger.info("IMPORTING XTF DATA...")
    exec_(
        " ".join(
            [
                f'"{config.JAVA}"',
                "-jar",
                f'"{config.ILI2PG}"',
                "--import",
                "--deleteData",
                *get_pgconf_as_ili_args(),
                "--dbschema",
                f'"{schema}"',
                "--modeldir",
                f'"{config.ILI_FOLDER}"',
                "--disableValidation",
                "--skipReferenceErrors",
                "--createTidCol",
                "--noSmartMapping",
                "--defaultSrsCode",
                "2056",
                "--log",
                f'"{log_path}"',
                f'"{xtf_file}"',
            ]
        )
    )


def export_xtf_data(schema, model_name, export_model_name, xtf_file, log_path):
    logger.info("EXPORT ILIDB...")

    # if optional export_model_name is set, add it to the args
    if export_model_name:
        export_model_name_args = ["--exportModels", export_model_name]
    else:
        export_model_name_args = []

    exec_(
        " ".join(
            [
                f'"{config.JAVA}"',
                "-jar",
                f'"{config.ILI2PG}"',
                "--export",
                "--models",
                f"{model_name}",
                *export_model_name_args,
                *get_pgconf_as_ili_args(),
                "--dbschema",
                f"{schema}",
                "--modeldir",
                f'"{config.ILI_FOLDER}"',
                "--disableValidation",
                "--skipReferenceErrors",
                "--createTidCol",
                "--noSmartMapping",
                "--defaultSrsCode",
                "2056",
                "--log",
                f'"{log_path}"',
                "--trace",
                f'"{xtf_file}"',
            ]
        )
    )


class TidMaker:
    """
    Helper class that creates globally unique integer primary key forili2pg class (t_id)
    from a a QGEP/QWAT id (obj_id or id).
    """

    def __init__(self, id_attribute="id"):
        self._id_attr = id_attribute
        self._autoincrementer = collections.defaultdict(lambda: len(self._autoincrementer))

    def tid_for_row(self, row, for_class=None):
        # tid are globally unique, while ids are only guaranteed unique per table,
        # so include the base table in the key
        # this finds the base class (the first parent class before sqlalchemy.ext.automap.Base)
        class_for_id = row.__class__.__mro__[row.__class__.__mro__.index(AutomapBase) - 2]
        key = (class_for_id, getattr(row, self._id_attr), for_class)
        # was_created = key not in self._autoincrementer  # just for debugging
        tid = self._autoincrementer[key]
        # if was_created:
        #     # just for debugging
        #     logger.info(f"created tid {tid} for {key}")
        return tid

    def next_tid(self):
        """Get an arbitrary unused tid"""
        key = len(self._autoincrementer)
        return self._autoincrementer[key]
