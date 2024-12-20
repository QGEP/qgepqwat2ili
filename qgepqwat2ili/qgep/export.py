import json

from geoalchemy2.functions import ST_Force2D
from sqlalchemy import or_
from sqlalchemy.orm import Session
from sqlalchemy.sql import text

from .. import utils
from ..utils.basket_utils import BasketUtils
from ..utils.qgep_export_utils import (
    QgepExportUtils,
    add_to_selection,
    filter_reaches,
    get_connected_overflow_to_wn_ids,
    get_connected_we_from_re,
    get_connected_we_to_re,
    get_ws_ids,
    get_ws_selected_ww_networkelements,
    remove_from_selection,
)
from ..utils.various import logger
from .model_abwasser import get_abwasser_model
from .model_qgep import get_qgep_model


def qgep_export_kek(selection=None, labels_file=None, orientation=None, basket_enabled=False):
    """
    Export data from the QGEP model into the ili2pg model.

    Args:
        selection:      if provided, limits the export to networkelements that are provided in the selection
    """

    qgep_model = get_qgep_model()
    abwasser_model = get_abwasser_model()

    # Logging disabled (very slow)
    # qgep_session = Session(utils.sqlalchemy.create_engine(logger_name="qgep"), autocommit=False, autoflush=False)
    # abwasser_session = Session(utils.sqlalchemy.create_engine(logger_name="abwasser"), autocommit=False, autoflush=False)
    qgep_session = Session(utils.sqlalchemy.create_engine(), autocommit=False, autoflush=False)
    abwasser_session = Session(utils.sqlalchemy.create_engine(), autocommit=False, autoflush=False)
    tid_maker = utils.ili2db.TidMaker(id_attribute="obj_id")

    # backport from tww https://github.com/teksi/wastewater/blob/3acfba249866d299f8a22e249d9f1e475fe7b88d/plugin/teksi_wastewater/interlis/interlis_model_mapping/interlis_exporter_to_intermediate_schema.py#L83
    abwasser_session.execute(text("SET CONSTRAINTS ALL DEFERRED;"))

    basket_utils = None
    current_basket = None
    if basket_enabled:
        basket_utils = BasketUtils(abwasser_model, abwasser_session)
        basket_utils.create_basket()

        current_basket = basket_utils.basket_topic_sia405_abwasser

    # 0. Initialize ws_off_sia405abwasser
    ws_off_sia405abwasser = False
    # 1. Filtering - check if selection
    filtered = selection is not None

    # Logging for debugging
    logger.debug(f"print filtered '{str(filtered)}'")

    subset_ids = selection if selection is not None else []
    # Logging for debugging
    logger.debug(f"print subset_ids: '{str(subset_ids)}'")

    # make a backup copy of subset_id - as it is beeing changed - don't know why

    subset_ids_original = selection if selection is not None else []
    logger.debug(f"print subset_ids_original: '{str(subset_ids_original)}'")

    subset_wws_ids = []

    if filtered:
        # 2. Get all connected from wastewater_nodes of selected reaches
        connected_from_wn_ids = get_connected_we_from_re(subset_ids)
        # 3. Get all connected to wastewater_nodes of selected reaches
        connected_to_wn_ids = get_connected_we_to_re(subset_ids)
        # 4. Get all connected wastewater_nodes from overflows.fk_overflow_to
        connected_overflow_to_wn_ids = get_connected_overflow_to_wn_ids(subset_ids)
        # 5. Add results from 2., 3. and 4. to subset_ids -> adapted_subset_ids
        adapted_subset_ids = []
        adapted_subset_ids = add_to_selection(subset_ids, connected_from_wn_ids)
        logger.debug(
            f"5 + 2 adapted_subset_ids: {adapted_subset_ids}",
        )
        adapted_subset_ids = add_to_selection(adapted_subset_ids, connected_to_wn_ids)
        logger.debug(
            f"5 + 2 + 3 adapted_subset_ids: {adapted_subset_ids}",
        )
        adapted_subset_ids = add_to_selection(adapted_subset_ids, connected_overflow_to_wn_ids)
        logger.debug(
            f"5 + 2 + 3 + 4 adapted_subset_ids: {adapted_subset_ids}",
        )
        # 6. check blind connections - are there reaches in adapted_subset_ids that have not been in subset_ids

        logger.debug(
            f"reprint subset_ids_original: {subset_ids_original}",
        )
        subset_ids = subset_ids_original
        logger.debug(
            f"reprint subset_ids: {subset_ids}",
        )

        subset_ids_reaches = []
        subset_ids_reaches = filter_reaches(subset_ids)
        logger.debug(
            f"6. subset_ids_reaches: {subset_ids_reaches}",
        )
        adapted_subset_ids_reaches = []
        adapted_subset_ids_reaches = filter_reaches(adapted_subset_ids)
        logger.debug(
            f"6. adapted_subset_ids_reaches: {adapted_subset_ids_reaches}",
        )
        if adapted_subset_ids_reaches is None:
            extra_reaches_ids = []
            if not adapted_subset_ids_reaches:
                logger.debug(
                    "no adapted_subset_ids_reaches - so nothing to remove",
                )
            else:
                logger.debug(
                    f"adapted_subset_ids_reaches: {adapted_subset_ids_reaches}",
                )
                # https://www.geeksforgeeks.org/python-difference-two-lists/
                # First convert lists to sets
                # https://www.w3schools.com/python/ref_set_difference.asp
                # x = {"apple", "banana", "cherry"}
                # y = {"google", "microsoft", "apple"}
                # z = x.difference(y)
                # replaced with code that first converts to sets
                # extra_reaches_ids = subset_ids_reaches.difference(adapted_subset_ids_reaches)
                # Convert lists to sets and use the difference method
                # c = list(set(a) - set(b))
                extra_reaches_ids = list(set(subset_ids_reaches) - set(adapted_subset_ids_reaches))
            # 7. If extra_reaches then remove from adapted_subset_ids
            if extra_reaches_ids is None:
                if not extra_reaches_ids:
                    # list is empty - no need for adaption
                    logger.debug(
                        "no extra reaches - so nothing to remove from adapted_subset_ids",
                    )
                else:
                    logger.debug(
                        f"extra_reaches_ids: {extra_reaches_ids} found!",
                    )
                    # if len(extra_reaches_ids) > 0:
                    adapted_subset_ids = remove_from_selection(
                        adapted_subset_ids, extra_reaches_ids
                    )
        # 8. get all id's of connected wastewater_structures
        subset_wws_ids = get_ws_selected_ww_networkelements(adapted_subset_ids)
        logger.info(
            f"8. subset_wws_ids: {subset_wws_ids}",
        )
        # 9. if sia405 export: check if wastewater_structures exist that are not part of SIA 405 Abwasser (in Release 2015 this is the class wwtp_structures, in Release 2020 it will be more - to be extended in tww)
        ws_off_sia405abwasser_list = None
        ws_off_sia405abwasser_list = get_ws_ids("wwtp_structure")

        # set flag if there are wwtp_structures
        ws_off_sia405abwasser = ws_off_sia405abwasser_list is not None
        logger.info(
            f"9. ws_off_sia405abwasser = {ws_off_sia405abwasser}",
        )
        # 10. Show ws_off_sia405abwasser_list
        logger.info(
            f"10. ws_off_sia405abwasser_list : {ws_off_sia405abwasser_list}",
        )
        # 11. take out ws_off_sia405abwasser_list from subset_wws_ids
        subset_wws_ids = remove_from_selection(subset_wws_ids, ws_off_sia405abwasser_list)
        logger.info(
            f"11. subset_ids of all wws minus ws_off_sia405abwasser_list: {subset_wws_ids}",
        )

    # also if not filtered we have to take out references to wwtp_structures
    else:
        # 20. if sia405 export: check if wastewater_structures exist that are not part of SIA 405 Abwasser (in Release 2015 this is the class wwtp_structures, in Release 2020 it will be more - to be extended in tww)
        ws_off_sia405abwasser_list = None
        ws_off_sia405abwasser_list = get_ws_ids("wwtp_structure")

        # set flag if there are wwtp_structures
        ws_off_sia405abwasser = ws_off_sia405abwasser_list is not None
        logger.info(
            f"20. ws_off_sia405abwasser (non filtered) = {ws_off_sia405abwasser}",
        )
        # 21. Show ws_off_sia405abwasser_list
        logger.info(
            f"21. ws_off_sia405abwasser_list (non filtered) : {ws_off_sia405abwasser_list}",
        )

        # 22. Get list of all wastewater_structures
        subset_wws_ids = get_ws_ids("wastewater_structure")
        logger.info(
            f"22. subset_wws_ids (non filtered) : {subset_wws_ids}",
        )
        # 23. take out ws_off_sia405abwasser_list from subset_wws_ids
        subset_wws_ids = remove_from_selection(subset_wws_ids, ws_off_sia405abwasser_list)
        logger.info(
            f"23. subset_ids of all wws minus ws_off_sia405abwasser_list (non filtered): {subset_wws_ids}",
        )

    # Orientation
    oriented = orientation is not None
    if oriented:
        labelorientation = orientation
    else:
        labelorientation = 0

    qgep_export_utils = QgepExportUtils(
        tid_maker=tid_maker,
        current_basket=current_basket,
        abwasser_session=abwasser_session,
        abwasser_model=abwasser_model,
        qgep_session=qgep_session,
        qgep_model=qgep_model,
        labelorientation=labelorientation,
        filtered=filtered,
        subset_ids=subset_ids,
        subset_wws_ids=subset_wws_ids,
        ws_off_sia405abwasser=ws_off_sia405abwasser,
    )

    # ADAPTED FROM 052a_sia405_abwasser_2015_2_d_interlisexport2.sql
    logger.info("Exporting QGEP.organisation -> ABWASSER.organisation, ABWASSER.metaattribute")
    qgep_export_utils.export_organisation()

    logger.info("Exporting QGEP.channel -> ABWASSER.kanal, ABWASSER.metaattribute")
    qgep_export_utils.export_channel()

    logger.info("Exporting QGEP.manhole -> ABWASSER.normschacht, ABWASSER.metaattribute")
    qgep_export_utils.export_manhole()

    logger.info("Exporting QGEP.discharge_point -> ABWASSER.einleitstelle, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.discharge_point)
    if filtered:
        query = query.join(qgep_model.wastewater_networkelement).filter(
            qgep_model.wastewater_networkelement.obj_id.in_(subset_ids)
        )
    for row in query:
        # AVAILABLE FIELDS IN QGEP.discharge_point

        # --- wastewater_structure ---
        # to do attributeslist of superclass
        # --- discharge_point ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        einleitstelle = abwasser_model.einleitstelle(
            # FIELDS TO MAP TO ABWASSER.einleitstelle
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "einleitstelle"),
            # --- abwasserbauwerk ---
            **qgep_export_utils.wastewater_structure_common(row),
            # --- einleitstelle ---
            hochwasserkote=row.highwater_level,
            # -- attribute 3D ---
            # maechtigkeit=row.depth,
            relevanz=qgep_export_utils.get_vl(row.relevance__REL),
            terrainkote=row.terrain_level,
            wasserspiegel_hydraulik=row.waterlevel_hydraulic,
        )
        abwasser_session.add(einleitstelle)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.special_structure -> ABWASSER.spezialbauwerk, ABWASSER.metaattribute"
    )
    qgep_export_utils.export_special_structure()

    logger.info(
        "Exporting QGEP.infiltration_installation -> ABWASSER.versickerungsanlage, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.infiltration_installation)
    if filtered:
        query = query.join(qgep_model.wastewater_networkelement).filter(
            qgep_model.wastewater_networkelement.obj_id.in_(subset_ids)
        )
    for row in query:
        # AVAILABLE FIELDS IN QGEP.infiltration_installation

        # --- wastewater_structure ---
        # _bottom_label, _cover_label, _depth, _function_hierarchic, _input_label, _label, _output_label, _usage_current, accessibility, contract_section, detail_geometry_geometry, financing, fk_dataowner, fk_main_cover, fk_main_wastewater_node, fk_operator, fk_owner, fk_provider, gross_costs, identifier, inspection_interval, last_modification, location_name, records, remark, renovation_necessity, replacement_value, rv_base_year, rv_construction_type, status, structure_condition, subsidies, year_of_construction, year_of_replacement

        # --- infiltration_installation ---
        # absorption_capacity, defects, dimension1, dimension2, distance_to_aquifer, effective_area, emergency_spillway, fk_aquifier, kind, labeling, obj_id, seepage_utilization, upper_elevation, vehicle_access, watertightness

        # --- _bwrel_ ---
        # measuring_point__BWREL_fk_wastewater_structure, mechanical_pretreatment__BWREL_fk_infiltration_installation, mechanical_pretreatment__BWREL_fk_wastewater_structure, re_maintenance_event_wastewater_structure__BWREL_fk_wastewater_structure, retention_body__BWREL_fk_infiltration_installation, structure_part__BWREL_fk_wastewater_structure, txt_symbol__BWREL_fk_wastewater_structure, txt_text__BWREL_fk_wastewater_structure, wastewater_networkelement__BWREL_fk_wastewater_structure, wastewater_structure_symbol__BWREL_fk_wastewater_structure, wastewater_structure_text__BWREL_fk_wastewater_structure, wwtp_structure_kind__BWREL_obj_id

        # --- _rel_ ---
        # accessibility__REL, defects__REL, emergency_spillway__REL, financing__REL, fk_aquifier__REL, fk_dataowner__REL, fk_main_cover__REL, fk_main_wastewater_node__REL, fk_operator__REL, fk_owner__REL, fk_provider__REL, kind__REL, labeling__REL, renovation_necessity__REL, rv_construction_type__REL, seepage_utilization__REL, status__REL, structure_condition__REL, vehicle_access__REL, watertightness__REL

        logger.info(
            "QGEP field infiltration_installation.upper_elevation is part of 3D extension. It will be ignored."
        )
        versickerungsanlage = abwasser_model.versickerungsanlage(
            # FIELDS TO MAP TO ABWASSER.versickerungsanlage
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "versickerungsanlage"),
            # --- abwasserbauwerk ---
            **qgep_export_utils.wastewater_structure_common(row),
            # --- versickerungsanlage ---
            # TODO : NOT MAPPED : upper_elevation
            art=qgep_export_utils.get_vl(row.kind__REL),
            beschriftung=qgep_export_utils.get_vl(row.labeling__REL),
            dimension1=row.dimension1,
            dimension2=row.dimension2,
            gwdistanz=row.distance_to_aquifer,
            maengel=qgep_export_utils.get_vl(row.defects__REL),
            notueberlauf=qgep_export_utils.get_vl(row.emergency_spillway__REL),
            saugwagen=qgep_export_utils.get_vl(row.vehicle_access__REL),
            schluckvermoegen=row.absorption_capacity,
            versickerungswasser=qgep_export_utils.get_vl(row.seepage_utilization__REL),
            wasserdichtheit=qgep_export_utils.get_vl(row.watertightness__REL),
            wirksameflaeche=row.effective_area,
        )
        abwasser_session.add(versickerungsanlage)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.pipe_profile -> ABWASSER.rohrprofil, ABWASSER.metaattribute")
    qgep_export_utils.export_pipe_profile()

    # with or without check_fk_in_subset
    # if filtered
    if filtered or ws_off_sia405abwasser:
        logger.info(
            "Exporting QGEP.reach_point (check_fk_in_subset) -> ABWASSER.haltungspunkt, ABWASSER.metaattribute"
        )
        qgep_export_utils.export_reach_point_check_fk_in_subset()

        logger.info(
            "Exporting QGEP.wastewater_node (check_fk_in_subset) -> ABWASSER.abwasserknoten, ABWASSER.metaattribute"
        )
        # cannot be moved to qgep_export_utils because fk_hydr_geometry is only in VSA-DSS but not in SIA405 Abwasser and KEK
        # qgep_export_utils.export_wastewater_node_check_fk_in_subset()

        query = qgep_session.query(qgep_model.wastewater_node)
        if filtered:
            query = query.filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
            # add sql statement to logger
            statement = query.statement
            logger.debug(f" selection query = {statement}")
        for row in query:
            # AVAILABLE FIELDS IN QGEP.wastewater_node

            # --- wastewater_networkelement ---
            # fk_dataowner, fk_provider, fk_wastewater_structure, identifier, last_modification, remark

            # --- wastewater_node ---

            # --- _bwrel_ ---
            # catchment_area__BWREL_fk_wastewater_networkelement_rw_current, catchment_area__BWREL_fk_wastewater_networkelement_rw_planned, catchment_area__BWREL_fk_wastewater_networkelement_ww_current, catchment_area__BWREL_fk_wastewater_networkelement_ww_planned, connection_object__BWREL_fk_wastewater_networkelement, hydraulic_char_data__BWREL_fk_wastewater_node, overflow__BWREL_fk_overflow_to, overflow__BWREL_fk_wastewater_node, reach_point__BWREL_fk_wastewater_networkelement, throttle_shut_off_unit__BWREL_fk_wastewater_node, wastewater_structure__BWREL_fk_main_wastewater_node

            # --- _rel_ ---
            # fk_dataowner__REL, fk_hydr_geometry__REL, fk_provider__REL, fk_wastewater_structure__REL

            # QGEP field wastewater_node.fk_hydr_geometry has no equivalent in the interlis model. It will be ignored.

            abwasserknoten = abwasser_model.abwasserknoten(
                # FIELDS TO MAP TO ABWASSER.abwasserknoten
                # --- baseclass ---
                # --- sia405_baseclass ---
                **qgep_export_utils.base_common(row, "abwasserknoten"),
                # --- abwassernetzelement ---
                # **qgep_export_utils.wastewater_networkelement_common(row),
                **qgep_export_utils.wastewater_networkelement_common_check_fk_in_subset(row),
                # --- abwasserknoten ---
                # TODO : WARNING : fk_hydr_geometry is not mapped
                lage=ST_Force2D(row.situation_geometry),
                rueckstaukote=row.backflow_level,
                sohlenkote=row.bottom_level,
            )
            abwasser_session.add(abwasserknoten)
            qgep_export_utils.create_metaattributes(row)
            print(".", end="")
        logger.info("done")
        abwasser_session.flush()

        logger.info(
            "Exporting QGEP.reach (check_fk_in_subset) -> ABWASSER.haltung, ABWASSER.metaattribute"
        )
        qgep_export_utils.export_reach_check_fk_in_subset()

    # not filtered and not ws_off_sia405abwasser
    else:
        logger.info("Exporting QGEP.reach_point -> ABWASSER.haltungspunkt, ABWASSER.metaattribute")
        qgep_export_utils.export_reach_point()

        logger.info(
            "Exporting QGEP.wastewater_node -> ABWASSER.abwasserknoten, ABWASSER.metaattribute"
        )
        # qgep_export_utils.export_wastewater_node()

        query = qgep_session.query(qgep_model.wastewater_node)
        if filtered:
            query = query.filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
            # add sql statement to logger
            statement = query.statement
            logger.debug(f" selection query = {statement}")
        for row in query:
            # AVAILABLE FIELDS IN QGEP.wastewater_node

            # --- wastewater_networkelement ---
            # fk_dataowner, fk_provider, fk_wastewater_structure, identifier, last_modification, remark

            # --- wastewater_node ---

            # --- _bwrel_ ---
            # catchment_area__BWREL_fk_wastewater_networkelement_rw_current, catchment_area__BWREL_fk_wastewater_networkelement_rw_planned, catchment_area__BWREL_fk_wastewater_networkelement_ww_current, catchment_area__BWREL_fk_wastewater_networkelement_ww_planned, connection_object__BWREL_fk_wastewater_networkelement, hydraulic_char_data__BWREL_fk_wastewater_node, overflow__BWREL_fk_overflow_to, overflow__BWREL_fk_wastewater_node, reach_point__BWREL_fk_wastewater_networkelement, throttle_shut_off_unit__BWREL_fk_wastewater_node, wastewater_structure__BWREL_fk_main_wastewater_node

            # --- _rel_ ---
            # fk_dataowner__REL, fk_hydr_geometry__REL, fk_provider__REL, fk_wastewater_structure__REL

            # QGEP field wastewater_node.fk_hydr_geometry has no equivalent in the interlis model. It will be ignored.

            abwasserknoten = abwasser_model.abwasserknoten(
                # FIELDS TO MAP TO ABWASSER.abwasserknoten
                # --- baseclass ---
                # --- sia405_baseclass ---
                **qgep_export_utils.base_common(row, "abwasserknoten"),
                # --- abwassernetzelement ---
                **qgep_export_utils.wastewater_networkelement_common(row),
                # --- abwasserknoten ---
                # TODO : WARNING : fk_hydr_geometry is not mapped
                lage=ST_Force2D(row.situation_geometry),
                rueckstaukote=row.backflow_level,
                sohlenkote=row.bottom_level,
            )
            abwasser_session.add(abwasserknoten)
            qgep_export_utils.create_metaattributes(row)
            print(".", end="")
        logger.info("done")
        abwasser_session.flush()

        logger.info("Exporting QGEP.reach -> ABWASSER.haltung, ABWASSER.metaattribute")
        qgep_export_utils.export_reach()

    if ws_off_sia405abwasser:
        logger.info(
            "Exporting QGEP.dryweather_downspout (ws_off_sia405abwasser) -> ABWASSER.trockenwetterfallrohr, ABWASSER.metaattribute"
        )
        qgep_export_utils.export_dryweather_downspout_ws_off_sia405abwasser()

        logger.info(
            "Exporting QGEP.access_aid (ws_off_sia405abwasser) -> ABWASSER.einstiegshilfe, ABWASSER.metaattribute"
        )
        qgep_export_utils.export_access_aid_ws_off_sia405abwasser()
        logger.info(
            "Exporting QGEP.dryweather_flume (ws_off_sia405abwasser)-> ABWASSER.trockenwetterrinne, ABWASSER.metaattribute"
        )
        qgep_export_utils.export_dryweather_flume_ws_off_sia405abwasser()

        logger.info(
            "Exporting QGEP.cover (ws_off_sia405abwasser) (  -> ABWASSER.deckel, ABWASSER.metaattribute"
        )
        qgep_export_utils.export_cover_ws_off_sia405abwasser()

        logger.info(
            "Exporting QGEP.benching (ws_off_sia405abwasser) -> ABWASSER.bankett, ABWASSER.metaattribute"
        )
        qgep_export_utils.export_benching_ws_off_sia405abwasser()

    else:
        logger.info(
            "Exporting QGEP.dryweather_downspout -> ABWASSER.trockenwetterfallrohr, ABWASSER.metaattribute"
        )
        qgep_export_utils.export_dryweather_downspout()

        logger.info("Exporting QGEP.access_aid -> ABWASSER.einstiegshilfe, ABWASSER.metaattribute")
        qgep_export_utils.export_access_aid()

        logger.info(
            "Exporting QGEP.dryweather_flume -> ABWASSER.trockenwetterrinne, ABWASSER.metaattribute"
        )
        qgep_export_utils.export_dryweather_flume()

        logger.info("Exporting QGEP.cover -> ABWASSER.deckel, ABWASSER.metaattribute")
        qgep_export_utils.export_cover()

        logger.info("Exporting QGEP.benching -> ABWASSER.bankett, ABWASSER.metaattribute")
        qgep_export_utils.export_benching()

    # From here on its about KEK -> change current basket
    current_basket = basket_utils.basket_topic_kek
    qgep_export_utils.current_basket = current_basket

    logger.info("Exporting QGEP.examination -> ABWASSER.untersuchung, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.examination)
    if filtered:
        query = (
            query.join(qgep_model.re_maintenance_event_wastewater_structure)
            .join(qgep_model.wastewater_structure)
            .join(qgep_model.wastewater_networkelement)
            .filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        )

    for row in query:

        # AVAILABLE FIELDS IN QGEP.examination

        # --- maintenance_event ---
        # --- examination ---
        # equipment, fk_reach_point, from_point_identifier, inspected_length, obj_id, recording_type, to_point_identifier, vehicle, videonumber, weather

        # --- _bwrel_ ---
        # damage__BWREL_fk_examination, re_maintenance_event_wastewater_structure__BWREL_fk_maintenance_event

        # --- _rel_ ---
        # fk_dataowner__REL, fk_operating_company__REL, fk_provider__REL, fk_reach_point__REL, kind__REL, recording_type__REL, status__REL, weather__REL
        logger.warning(
            "QGEP field maintenance_event.active_zone has no equivalent in the interlis model. It will be ignored."
        )

        untersuchung = abwasser_model.untersuchung(
            # FIELDS TO MAP TO ABWASSER.untersuchung
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "untersuchung"),
            # --- erhaltungsereignis ---
            # abwasserbauwerkref=row.REPLACE_ME,  # TODO : convert this to M2N relation through re_maintenance_event_wastewater_structure
            art=qgep_export_utils.get_vl(row.kind__REL),
            astatus=qgep_export_utils.get_vl(row.status__REL),
            ausfuehrende_firmaref=qgep_export_utils.get_tid(row.fk_operating_company__REL),
            ausfuehrender=row.operator,
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            # model difference qgep (unlimited text) and vsa-dss 2015 / 2020 / vsa-kek 2019 / 2020 TEXT*50
            # datengrundlage=row.base_data,
            datengrundlage=qgep_export_utils.truncate(row.base_data, 50),
            dauer=row.duration,
            detaildaten=row.data_details,
            ergebnis=row.result,
            grund=row.reason,
            kosten=row.cost,
            zeitpunkt=row.time_point,
            # --- untersuchung ---
            bispunktbezeichnung=row.to_point_identifier,
            erfassungsart=qgep_export_utils.get_vl(row.recording_type__REL),
            fahrzeug=row.vehicle,
            geraet=row.equipment,
            haltungspunktref=qgep_export_utils.get_tid(row.fk_reach_point__REL),
            inspizierte_laenge=row.inspected_length,
            videonummer=row.videonumber,
            vonpunktbezeichnung=row.from_point_identifier,
            witterung=qgep_export_utils.get_vl(row.weather__REL),
        )
        abwasser_session.add(untersuchung)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.damage_manhole -> ABWASSER.normschachtschaden, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.damage_manhole)
    if filtered:
        query = (
            query.join(qgep_model.examination)
            .join(qgep_model.re_maintenance_event_wastewater_structure)
            .join(qgep_model.wastewater_structure)
            .join(qgep_model.wastewater_networkelement)
            .filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        )
    for row in query:

        # AVAILABLE FIELDS IN QGEP.damage_manhole

        # --- damage ---

        # --- damage_manhole ---
        # manhole_damage_code, manhole_shaft_area, obj_id

        # --- _bwrel_ ---
        # damage_channel_channel_damage_code__BWREL_obj_id

        # --- _rel_ ---
        # connection__REL, fk_dataowner__REL, fk_examination__REL, fk_provider__REL, manhole_damage_code__REL, manhole_shaft_area__REL, single_damage_class__REL

        normschachtschaden = abwasser_model.normschachtschaden(
            # FIELDS TO MAP TO ABWASSER.normschachtschaden
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "normschachtschaden"),
            # --- schaden ---
            anmerkung=row.comments,
            ansichtsparameter=row.view_parameters,
            einzelschadenklasse=qgep_export_utils.get_vl(row.single_damage_class__REL),
            streckenschaden=row.damage_reach,
            untersuchungref=qgep_export_utils.get_tid(row.fk_examination__REL),
            verbindung=qgep_export_utils.get_vl(row.connection__REL),
            videozaehlerstand=row.video_counter,
            # --- normschachtschaden ---
            distanz=row.distance,
            quantifizierung1=row.quantification1,
            quantifizierung2=row.quantification2,
            schachtbereich=qgep_export_utils.get_vl(row.manhole_shaft_area__REL),
            schachtschadencode=qgep_export_utils.get_vl(row.manhole_damage_code__REL),
            schadenlageanfang=row.damage_begin,
            schadenlageende=row.damage_end,
        )
        abwasser_session.add(normschachtschaden)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.damage_channel -> ABWASSER.kanalschaden, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.damage_channel)
    if filtered:
        query = (
            query.join(qgep_model.examination)
            .join(qgep_model.re_maintenance_event_wastewater_structure)
            .join(qgep_model.wastewater_structure)
            .join(qgep_model.wastewater_networkelement)
            .filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        )
    for row in query:

        # AVAILABLE FIELDS IN QGEP.damage_channel

        # --- damage ---
        # comments, connection, damage_begin, damage_end, damage_reach, distance, fk_dataowner, fk_examination, fk_provider, last_modification, quantification1, quantification2, single_damage_class, video_counter, view_parameters

        # --- damage_channel ---
        # , obj_id

        # --- _bwrel_ ---
        # damage_channel_channel_damage_code__BWREL_obj_id

        # --- _rel_ ---
        # channel_damage_code__REL, connection__REL, fk_dataowner__REL, fk_examination__REL, fk_provider__REL, single_damage_class__REL

        kanalschaden = abwasser_model.kanalschaden(
            # FIELDS TO MAP TO ABWASSER.kanalschaden
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "kanalschaden"),
            # --- schaden ---
            anmerkung=row.comments,
            ansichtsparameter=row.view_parameters,
            einzelschadenklasse=qgep_export_utils.get_vl(row.single_damage_class__REL),
            streckenschaden=row.damage_reach,
            untersuchungref=qgep_export_utils.get_tid(row.fk_examination__REL),
            verbindung=qgep_export_utils.get_vl(row.connection__REL),
            videozaehlerstand=row.video_counter,
            # --- kanalschaden ---
            distanz=row.distance,
            kanalschadencode=qgep_export_utils.get_vl(row.channel_damage_code__REL),
            quantifizierung1=row.quantification1,
            quantifizierung2=row.quantification2,
            schadenlageanfang=row.damage_begin,
            schadenlageende=row.damage_end,
        )

        print(f"Kanalschaden: {kanalschaden.t_basket}")

        abwasser_session.add(kanalschaden)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.data_media -> ABWASSER.datentraeger, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.data_media)
    for row in query:

        # AVAILABLE FIELDS IN QGEP.data_media

        # --- data_media ---
        # fk_dataowner, fk_provider, identifier, kind, last_modification, location, obj_id, path, remark

        # --- _rel_ ---
        # fk_dataowner__REL, fk_provider__REL, kind__REL

        datentraeger = abwasser_model.datentraeger(
            # FIELDS TO MAP TO ABWASSER.datentraeger
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "datentraeger"),
            # --- datentraeger ---
            art=qgep_export_utils.get_vl(row.kind__REL),
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            pfad=row.path,
            standort=row.location,
        )
        abwasser_session.add(datentraeger)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.file -> ABWASSER.datei, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.file)
    if filtered:
        query = (
            query.outerjoin(qgep_model.damage, qgep_model.file.object == qgep_model.damage.obj_id)
            .join(
                qgep_model.examination,
                or_(
                    qgep_model.file.object == qgep_model.damage.obj_id,
                    qgep_model.file.object == qgep_model.examination.obj_id,
                ),
            )
            .join(qgep_model.re_maintenance_event_wastewater_structure)
            .join(qgep_model.wastewater_structure)
            .join(qgep_model.wastewater_networkelement)
            .filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        )
    for row in query:

        # AVAILABLE FIELDS IN QGEP.file

        # --- file ---
        # class, fk_data_media, fk_dataowner, fk_provider, identifier, kind, last_modification, obj_id, object, path_relative, remark

        # --- _rel_ ---
        # class__REL, fk_dataowner__REL, fk_provider__REL, kind__REL

        datei = abwasser_model.datei(
            # FIELDS TO MAP TO ABWASSER.datei
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "datei"),
            # --- datei ---
            art=qgep_export_utils.get_vl(row.kind__REL) or "andere",
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            datentraegerref=qgep_export_utils.get_tid(row.fk_data_media__REL),
            klasse=qgep_export_utils.get_vl(row.class__REL),
            # model difference qgep TEXT*41 and vsa-kek 2019 / 2020 TEXT*16 (length of obj_id)
            # objekt=qgep_export_utils.null_to_emptystr(row.object),
            objekt=qgep_export_utils.truncate(qgep_export_utils.null_to_emptystr(row.object), 16),
            relativpfad=row.path_relative,
        )
        abwasser_session.add(datei)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    current_basket = basket_utils.basket_topic_sia405_abwasser
    qgep_export_utils.current_basket = current_basket

    # Labels
    # Note: these are extracted from the optional labels file (not exported from the QGEP database)
    if labels_file:
        logger.info(f"Exporting label positions from {labels_file}")

        # Get t_id by obj_name to create the reference on the labels below
        tid_for_obj_id = {
            "haltung": {},
            "abwasserbauwerk": {},
        }
        for row in abwasser_session.query(abwasser_model.haltung):
            tid_for_obj_id["haltung"][row.obj_id] = row.t_id
        for row in abwasser_session.query(abwasser_model.abwasserbauwerk):
            tid_for_obj_id["abwasserbauwerk"][row.obj_id] = row.t_id

        with open(labels_file) as labels_file_handle:
            labels = json.load(labels_file_handle)

        geojson_crs_def = labels["crs"]

        for label in labels["features"]:
            layer_name = label["properties"]["Layer"]
            obj_id = label["properties"]["qgep_obj_id"]

            print(f"label[properties]: {label['properties']}")

            if not label["properties"]["LabelText"]:
                logger.warning(
                    f"Label of object '{obj_id}' from layer '{layer_name}' is empty and will not be exported"
                )
                continue

            if layer_name == "vw_qgep_reach":
                if obj_id not in tid_for_obj_id["haltung"]:
                    logger.warning(
                        f"Label for haltung `{obj_id}` exists, but that object is not part of the export"
                    )
                    continue
                ili_label = abwasser_model.haltung_text(
                    **qgep_export_utils.textpos_common(label, "haltung_text", geojson_crs_def),
                    haltungref=tid_for_obj_id["haltung"][obj_id],
                )

            elif layer_name == "vw_qgep_wastewater_structure":
                if obj_id not in tid_for_obj_id["abwasserbauwerk"]:
                    logger.warning(
                        f"Label for abwasserbauwerk `{obj_id}` exists, but that object is not part of the export"
                    )
                    continue
                ili_label = abwasser_model.abwasserbauwerk_text(
                    **qgep_export_utils.textpos_common(
                        label, "abwasserbauwerk_text", geojson_crs_def
                    ),
                    abwasserbauwerkref=tid_for_obj_id["abwasserbauwerk"][obj_id],
                )

            else:
                logger.warning(
                    f"Unknown layer for label `{layer_name}`. Label will be ignored",
                )
                continue

            abwasser_session.add(ili_label)
            print(".", end="")
        logger.info("done")
        abwasser_session.flush()

    abwasser_session.commit()

    qgep_session.close()
    abwasser_session.close()
