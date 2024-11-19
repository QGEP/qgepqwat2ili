import collections

# 11.4.2023
import xml.etree.ElementTree as ET

import psycopg2
from sqlalchemy.ext.automap import AutomapBase

from .. import config
from .various import exec_, get_pgconf_as_ili_args, get_pgconf_as_psycopg2_dsn, logger


def check_organisation_subclass_data():
    """
    Check if subclass entries of organisation are set and match number of organisation entries
    """
    logger.info("INTEGRITY CHECK organisations subclass data...")

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

    return organisation_subclass_check


def check_wastewater_structure_subclass_data():
    """
    Check if subclass entries of wastewater_structure are set and match number of wastewater_structure entries
    """
    logger.info("INTEGRITY CHECK wastewater_structures subclass data...")

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

    return wastewater_structure_subclass_check


def check_identifier_null():
    """
    Check if attribute identifier is Null
    """
    logger.info("INTEGRITY CHECK missing identifiers...")

    connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
    connection.set_session(autocommit=True)
    cursor = connection.cursor()

    missing_identifier_count = 0
    # add classes to be checked
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
        # add variable and store result of cursor.fetchone()[0] as the next call will give None value instead of count https://pynative.com/python-cursor-fetchall-fetchmany-fetchone-to-read-rows-from-table/

        try:
            class_identifier_count = int(cursor.fetchone()[0])
        except Exception:
            class_identifier_count = 0
            logger.debug(
                f"Number of datasets in class '{notsubclass}' without identifier could not be identified (TypeError: 'NoneType' object is not subscriptable). Automatically set class_identifier_count = 0"
            )
        else:
            logger.info(
                f"Number of datasets in class '{notsubclass}' without identifier : {class_identifier_count}"
            )

        # if cursor.fetchone() is None:
        if class_identifier_count == 0:
            missing_identifier_count = missing_identifier_count
        else:
            # missing_identifier_count = missing_identifier_count + int(cursor.fetchone()[0])
            missing_identifier_count = missing_identifier_count + class_identifier_count

        # add for testing
        logger.info(f"missing_identifier_count : {missing_identifier_count}")

    if missing_identifier_count == 0:
        identifier_null_check = True
        logger.info("OK: all identifiers set in qgep_od!")
    else:
        identifier_null_check = False
        logger.info(f"ERROR: Missing identifiers in qgep_od: {missing_identifier_count}")
    return identifier_null_check


def check_fk_owner_null():
    """
    Check if MAMDATORY fk_owner is Null
    """
    logger.info("INTEGRITY CHECK missing MAMDATORY owner references fk_owner...")

    connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
    connection.set_session(autocommit=True)
    cursor = connection.cursor()

    missing_fk_owner_count = 0
    # add MANDATORY classes to be checked
    for notsubclass in [
        # SIA405 Abwasser
        ("wastewater_structure"),
    ]:
        cursor.execute(f"SELECT COUNT(obj_id) FROM qgep_od.{notsubclass} WHERE fk_owner is null;")
        # use cursor.fetchone()[0] instead of cursor.rowcount
        # add variable and store result of cursor.fetchone()[0] as the next call will give None value instead of count https://pynative.com/python-cursor-fetchall-fetchmany-fetchone-to-read-rows-from-table/
        class_fk_owner_count = int(cursor.fetchone()[0])
        # logger.info(
        #    f"Number of datasets in class '{notsubclass}' without fk_owner : {cursor.fetchone()[0]}"
        # )
        logger.info(
            f"Number of datasets in class '{notsubclass}' without fk_owner : {class_fk_owner_count}"
        )

        # if cursor.fetchone() is None:
        if class_fk_owner_count == 0:
            missing_fk_owner_count = missing_fk_owner_count
        else:
            # missing_fk_owner_count = missing_fk_owner_count + int(cursor.fetchone()[0])
            missing_fk_owner_count = missing_fk_owner_count + class_fk_owner_count

        # add for testing
        logger.info(f"missing_fk_owner_count : {missing_fk_owner_count}")

    if missing_fk_owner_count == 0:
        check_fk_owner_null = True
        logger.info("OK: all mandatory fk_owner set in qgep_od!")
    else:
        check_fk_owner_null = False
        logger.info(f"ERROR: Missing mandatory fk_owner in qgep_od: {missing_fk_owner_count}")
    return check_fk_owner_null


def check_fk_operator_null():
    """
    Check if MAMDATORY fk_operator is Null
    """
    logger.info("INTEGRITY CHECK missing MAMDATORY operator references fk_operator...")

    connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
    connection.set_session(autocommit=True)
    cursor = connection.cursor()

    missing_fk_operator_count = 0

    # add MANDATORY classes to be checked
    for notsubclass in [
        # SIA405 Abwasser
        ("wastewater_structure"),
    ]:
        cursor.execute(
            f"SELECT COUNT(obj_id) FROM qgep_od.{notsubclass} WHERE fk_operator is null;"
        )
        # use cursor.fetchone()[0] instead of cursor.rowcount
        logger.info(
            f"Number of datasets in class '{notsubclass}' without fk_operator : {cursor.fetchone()[0]}"
        )

        if cursor.fetchone() is None:
            missing_fk_operator_count = missing_fk_operator_count
        else:
            missing_fk_operator_count = missing_fk_operator_count + int(cursor.fetchone()[0])
        # add for testing
        logger.info(f"missing_fk_operator_count : {missing_fk_operator_count}")

    if missing_fk_operator_count == 0:
        check_fk_operator_null = True
        logger.info("OK: all mandatory fk_operator set in qgep_od!")
    else:
        check_fk_operator_null = False
        logger.info(
            f"ERROR: Missing mandatory fk_operator in qgep_od: {missing_fk_operator_count}"
        )

    return check_fk_operator_null


def check_fk_dataowner_null():
    """
    Check if MAMDATORY fk_dataowner is Null
    """
    logger.info("INTEGRITY CHECK missing dataowner references fk_dataowner...")

    connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
    connection.set_session(autocommit=True)
    cursor = connection.cursor()

    missing_fk_dataowner_count = 0
    # add MANDATORY classes to be checked
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
            f"SELECT COUNT(obj_id) FROM qgep_od.{notsubclass} WHERE fk_dataowner is null;"
        )
        # use cursor.fetchone()[0] instead of cursor.rowcount
        # add variable and store result of cursor.fetchone()[0] as the next call will give None value instead of count https://pynative.com/python-cursor-fetchall-fetchmany-fetchone-to-read-rows-from-table/
        class_fk_dataowner_count = int(cursor.fetchone()[0])

        # logger.info(
        #    f"Number of datasets in class '{notsubclass}' without fk_dataowner : {cursor.fetchone()[0]}"
        # )
        logger.info(
            f"Number of datasets in class '{notsubclass}' without fk_dataowner : {class_fk_dataowner_count}"
        )

        # if cursor.fetchone() is None:
        if class_fk_dataowner_count == 0:
            missing_fk_dataowner_count = missing_fk_dataowner_count
        else:
            # missing_fk_dataowner_count = missing_fk_dataowner_count + int(cursor.fetchone()[0])
            missing_fk_dataowner_count = missing_fk_dataowner_count + class_fk_dataowner_count

        # add for testing
        logger.info(f"missing_fk_dataowner_count : {missing_fk_dataowner_count}")

    if missing_fk_dataowner_count == 0:
        check_fk_dataowner_null = True
        logger.info("OK: all mandatory fk_dataowner set in qgep_od!")
    else:
        check_fk_dataowner_null = False
        logger.info(
            f"ERROR: Missing mandatory fk_dataowner in qgep_od: {missing_fk_dataowner_count}"
        )

    return check_fk_dataowner_null


def check_fk_provider_null():
    """
    Check if MAMDATORY fk_provider is Null
    """
    logger.info("INTEGRITY CHECK missing provider references fk_provider...")

    connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
    connection.set_session(autocommit=True)
    cursor = connection.cursor()

    missing_fk_provider_count = 0
    # add MANDATORY classes to be checked
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
            f"SELECT COUNT(obj_id) FROM qgep_od.{notsubclass} WHERE fk_provider is null;"
        )
        # use cursor.fetchone()[0] instead of cursor.rowcount
        # add variable and store result of cursor.fetchone()[0] as the next call will give None value instead of count https://pynative.com/python-cursor-fetchall-fetchmany-fetchone-to-read-rows-from-table/
        class_fk_provider_count = int(cursor.fetchone()[0])
        # logger.info(
        #    f"Number of datasets in class '{notsubclass}' without fk_provider : {cursor.fetchone()[0]}"
        # )
        logger.info(
            f"Number of datasets in class '{notsubclass}' without fk_dataowner : {class_fk_provider_count}"
        )

        # if cursor.fetchone() is None:
        if class_fk_provider_count == 0:
            missing_fk_provider_count = missing_fk_provider_count
        else:
            # missing_fk_provider_count = missing_fk_provider_count + int(cursor.fetchone()[0])
            missing_fk_provider_count = missing_fk_provider_count + class_fk_provider_count

        # add for testing
        logger.info(f"missing_fk_provider_count : {missing_fk_provider_count}")

    if missing_fk_provider_count == 0:
        check_fk_provider_null = True
        logger.info("OK: all mandatory fk_provider set in qgep_od!")
    else:
        check_fk_provider_null = False
        logger.info(
            f"ERROR: Missing mandatory fk_provider in qgep_od: {missing_fk_provider_count}"
        )

    return check_fk_provider_null


def skip_wwtp_structure_ids_old():
    """
    Get list of id's of class wastewater_structure without wwtp_structure (ARABauwerk)
    """
    logger.info("get list of id's of class wwtp_structure (ARABauwerk)...")

    connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
    connection.set_session(autocommit=True)
    cursor = connection.cursor()

    not_wwtp_structure_ids = []

    # select all obj_id from wastewater_structure that are not in wwtp_structure
    cursor.execute(
        "SELECT * FROM qgep_od.wastewater_structure WHERE obj_id NOT IN (SELECT obj_id FROM qgep_od.wwtp_structure);"
    )

    # cursor.fetchall() - see https://pynative.com/python-cursor-fetchall-fetchmany-fetchone-to-read-rows-from-table/
    # wwtp_structure_count = int(cursor.fetchone()[0])
    # if wwtp_structure_count == 0:
    if cursor.fetchone() is None:
        not_wwtp_structure_ids = None
    else:
        records = cursor.fetchall()
        for row in records:
            logger.debug(f" row[0] = {row[0]}")
            # https://www.pythontutorial.net/python-string-methods/python-string-concatenation/
            # not_wwtp_structure_ids = not_wwtp_structure_ids + str(row[0]) + ","
            strrow = str(row[0])
            # not_wwtp_structure_ids = ','.join([not_wwtp_structure_ids, strrow])
            # not_wwtp_structure_ids = not_wwtp_structure_ids + row[0]
            not_wwtp_structure_ids.append(strrow)
            logger.debug(f" building up '{not_wwtp_structure_ids}' ...")

    return not_wwtp_structure_ids


# 12.11.2024 to clean up - get_ws_wn_ids kann das auch
def get_cl_re_ids(classname):
    """
    Get list of id's of reaches of the channels provided
    """

    # define classes that this is allowed to use - adapt for TWW to include model changes
    if classname == "channel":
        logger.info(f"get list of id's of wastewater_nodes of {classname} ...")

        connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
        connection.set_session(autocommit=True)
        cursor = connection.cursor()

        cl_re_ids = []

        # select all obj_id of the wastewater_nodes of wwtp_structure
        cursor.execute(
            "SELECT wn.obj_id FROM qgep_od.channel LEFT JOIN qgep_od.wastewater_networkelement wn ON wn.fk_wastewater_structure = channel.obj_id WHERE wn.obj_id is not NULL;"
        )

        # cursor.fetchall() - see https://pynative.com/python-cursor-fetchall-fetchmany-fetchone-to-read-rows-from-table/
        # cl_re_ids_count = int(cursor.fetchone()[0])
        # if cl_re_ids_count == 0:
        if cursor.fetchone() is None:
            cl_re_ids = None
        else:
            records = cursor.fetchall()
            for row in records:
                logger.debug(f" row[0] = {row[0]}")
                # https://www.pythontutorial.net/python-string-methods/python-string-concatenation/
                strrow = str(row[0])
                cl_re_ids.append(strrow)
                logger.debug(f" building up '{cl_re_ids}' ...")

        return cl_re_ids
    else:
        logger.warning(f"Do not use this function with {classname} !")
        return None


def get_ws_wn_ids(classname):
    """
    Get list of id's of wastewater_nodes of the wastewater_structure (sub)class provided, eg. wwtp_structure (ARABauwerk, does also work for channel (give reaches then)
    """

    logger.info(f"get list of id's of wastewater_nodes of {classname} ...")
    connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
    connection.set_session(autocommit=True)
    cursor = connection.cursor()

    ws_wn_ids = []

    # select all obj_id of the wastewater_nodes of wwtp_structure
    cursor.execute(
        f"SELECT wn.obj_id FROM qgep_od.{classname} LEFT JOIN qgep_od.wastewater_networkelement wn ON wn.fk_wastewater_structure = {classname}.obj_id WHERE wn.obj_id is not NULL;"
    )

    # cursor.fetchall() - see https://pynative.com/python-cursor-fetchall-fetchmany-fetchone-to-read-rows-from-table/
    # ws_wn_ids_count = int(cursor.fetchone()[0])
    # if ws_wn_ids_count == 0:
    if cursor.fetchone() is None:
        ws_wn_ids = None
    else:
        # added cursor.execute again to see if with this all records will be available
        # 15.11.2024 added - see https://stackoverflow.com/questions/58101874/cursor-fetchall-or-other-method-fetchone-is-not-working
        cursor.execute(
            f"SELECT wn.obj_id FROM qgep_od.{classname} LEFT JOIN qgep_od.wastewater_networkelement wn ON wn.fk_wastewater_structure = {classname}.obj_id WHERE wn.obj_id is not NULL;"
        )
        records = cursor.fetchall()

        # 15.11.2024 - does not get all records, but only n-1
        for row in records:
            logger.debug(f" row[0] = {row[0]}")
            # https://www.pythontutorial.net/python-string-methods/python-string-concatenation/
            strrow = str(row[0])
            if strrow is not None:
                ws_wn_ids.append(strrow)
                # logger.debug(f" building up '{ws_wn_ids}' ...")

    return ws_wn_ids


def get_ws_selected_ww_networkelements(selected_wwn):
    """
    Get list of id's of wastewater_structure from selected wastewater_network_elements
    """

    logger.debug(
        f"get list of id's of wastewater_structure of selected wastewater_network_elements {selected_wwn} ..."
    )
    connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
    connection.set_session(autocommit=True)
    cursor = connection.cursor()

    selection_text = ""

    for list_item in selected_wwn:
        selection_text += "'"
        selection_text += list_item
        selection_text += "',"

    # remove last komma to make it a correct IN statement
    selection_text = selection_text[:-1]

    logger.debug(f"selection_text = {selection_text} ...")

    ws_ids = []

    # select all obj_id of the wastewater_nodes of wwtp_structure
    cursor.execute(
        f"SELECT ws.obj_id FROM qgep_od.wastewater_structure ws LEFT JOIN qgep_od.wastewater_networkelement wn ON wn.fk_wastewater_structure = ws.obj_id WHERE wn.obj_id IN ({selection_text});"
    )

    # cursor.fetchall() - see https://pynative.com/python-cursor-fetchall-fetchmany-fetchone-to-read-rows-from-table/
    # ws_wn_ids_count = int(cursor.fetchone()[0])
    # if ws_wn_ids_count == 0:
    if cursor.fetchone() is None:
        ws_ids = None
    else:
        records = cursor.fetchall()
        for row in records:
            logger.debug(f" row[0] = {row[0]}")
            # https://www.pythontutorial.net/python-string-methods/python-string-concatenation/
            strrow = str(row[0])
            if strrow is not None:
                ws_ids.append(strrow)
                # logger.debug(f" building up '{ws_wn_ids}' ...")

    return ws_ids


def remove_from_selection(selected_ids, remove_ids):
    """
    Remove ids from selected_ids if they are in selected_ids
    """

    for list_item in remove_ids:
        # selected_ids = selected_ids.remove(list_item)
        try:
            selected_ids.remove(list_item)
        except Exception:
            logger.debug logger.debug(f" remove_from_selection: '{list_item}' not in selected_ids - could not be removed!")

    return selected_ids


def add_to_selection(selected_ids, add_ids):
    """
    Append ids to selected_ids
    """

    if selected_ids is None:
        selected_ids = []

    for list_item in add_ids:
        # selected_ids = selected_ids.append(list_item)
        selected_ids.append(list_item)

    return selected_ids


def create_ili_schema(schema, model, log_path, recreate_schema=False):
    """
    Create schema for INTERLIS import
    """
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
    """
    Run XTF validation using ilivalidator
    """
    logger.info("VALIDATING XTF DATA...")
    exec_(
        f'"{config.JAVA}" -jar "{config.ILIVALIDATOR}" --modeldir "{config.ILI_FOLDER}" --log "{log_path}" "{xtf_file}"'
    )


# 22.7.2022 sb
def get_xtf_model(xtf_file):
    """
    Get XTF model from file
    """
    logger.info(f"GET XTF MODEL {xtf_file} ... ")
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
                    checkmodelssection = line.find("<MODELS>")
                    logger.info("checkmodelssection " + str(checkmodelssection))
                    logger.info(str(line))
            else:
                line2 = f.readline()
                if not line2:
                    break
                else:
                    logger.info(str(line2))
                    logger.info("line2: ", str(line2))
                    # logger.info(str(checkdatasection))
                    logger.info("checkmodelssection2 " + str(checkmodelssection))
                    # strmodel = str(line2.strip())
                    strmodel = str(line2)
                    strmodel = strmodel.strip()
                    logger.info("strmodel: " + strmodel)
                    logger.info("strmodel: ", strmodel)
                    logger.info(f"strmodel: {strmodel}")
                    a = strmodel.find("</MODELS>")
                    logger.info("strmodel.find a </MODELS>: " + str(a))
                    # if strmodel.find("</MODELS>") > -1:
                    if a == -1:
                        b = strmodel.find("<MODEL>")
                        logger.info(r"strmodel.find b \<MODEL: " + str(b))
                        if strmodel.find("<MODEL") > -1:
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
                            model_list.append(result)
                        else:
                            logger.info("goto next line")
                    else:
                        logger.info("</MODEL> found - stop checking!")
                        break
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

    # neu 23.7.2022 return imodel from get_xtf_model so it can be called in _init_.py
    return impmodel


def get_xtf_model2(xtf_file):
    logger.info("GET XTF MODEL xml version... ")
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
