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
    get_ws_selected_ww_networkelements,
    remove_from_selection,
)
from ..utils.various import logger
from .model_abwasser import get_abwasser_model
from .model_qgep import get_qgep_model


def qgep_export_dss(selection=None, labels_file=None, orientation=None, basket_enabled=False):
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
    logger.debug(f"print filtered '{filtered}'")

    subset_ids = selection if selection is not None else []

    # Logging for debugging
    logger.debug(f"print subset_ids '{subset_ids}'")

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
        # ws_off_sia405abwasser_list = None
        # ws_off_sia405abwasser_list = get_ws_ids("wwtp_structure")

        # set flag if there are wwtp_structures
        # ws_off_sia405abwasser = ws_off_sia405abwasser_list is not None
        # logger.info(
        # f"9. ws_off_sia405abwasser = {ws_off_sia405abwasser}",
        # )
        # 10. Show ws_off_sia405abwasser_list
        # logger.info(
        # f"10. ws_off_sia405abwasser_list : {ws_off_sia405abwasser_list}",
        # )
        # 11. take out ws_off_sia405abwasser_list from subset_wws_ids
        # subset_wws_ids = remove_from_selection(subset_wws_ids, ws_off_sia405abwasser_list)
        # logger.info(
        # f"11. subset_ids of all wws minus ws_off_sia405abwasser_list: {subset_wws_ids}",
        # )

    # also if not filtered we have to take out references to wwtp_structures
    else:
        # 20. if sia405 export: check if wastewater_structures exist that are not part of SIA 405 Abwasser (in Release 2015 this is the class wwtp_structures, in Release 2020 it will be more - to be extended in tww)
        # ws_off_sia405abwasser_list = None
        # ws_off_sia405abwasser_list = get_ws_ids("wwtp_structure")

        # set flag if there are wwtp_structures - with dss always false
        ws_off_sia405abwasser = False
        logger.info(
            f"20. ws_off_sia405abwasser (non filtered) = {ws_off_sia405abwasser}",
        )
        # 21. Show ws_off_sia405abwasser_list
        # logger.info(
        # f"21. ws_off_sia405abwasser_list (non filtered) : {ws_off_sia405abwasser_list}",
        # )

        # 22. Get list of all wastewater_structures
        # subset_wws_ids = get_ws_ids("wastewater_structure")
        # logger.info(
        # f"22. subset_wws_ids (non filtered) : {subset_wws_ids}",
        # )
        # 23. take out ws_off_sia405abwasser_list from subset_wws_ids
        # subset_wws_ids = remove_from_selection(subset_wws_ids, ws_off_sia405abwasser_list)
        # logger.info(
        # f"23. subset_ids of all wws minus ws_off_sia405abwasser_list (non filtered): {subset_wws_ids}",
        # )
        logger.debug("Handling of wwtp_structures not needed with VSA-DSS")

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

    def organisation_common(row):
        """
        Returns common attributes for organisation
        """
        return {
            "bemerkung": qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            "bezeichnung": row.identifier,
            # attribute organisation.gemeindenummer will be added with release 2020
            # "gemeindenummer": row.municipality_number,
            # not supported in model qgep 2015
            # "teil_vonref": qgep_export_utils.get_tid(row.fk_part_of__REL),
            "auid": row.uid,
        }

    def surface_water_bodies_common(row):
        """
        Returns common attributes for surface_water_bodies
        """
        return {
            "bemerkung": qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            "bezeichnung": qgep_export_utils.null_to_emptystr(row.identifier),
        }

    def zone_common(row):
        """
        Returns common attributes for zone
        """
        return {
            "bemerkung": qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            "bezeichnung": qgep_export_utils.null_to_emptystr(row.identifier),
        }

    def water_control_structure_common(row):
        """
        Returns common attributes for water_control_structure
        """
        return {
            "bemerkung": qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            "bezeichnung": qgep_export_utils.null_to_emptystr(row.identifier),
            "gewaesserabschnittref": qgep_export_utils.get_tid(row.fk_water_course_segment__REL),
            "lage": ST_Force2D(row.situation_geometry),
        }

    def connection_object_common(row):
        """
        Returns common attributes for connection_object
        """
        return {
            "abwassernetzelementref": qgep_export_utils.get_tid(
                row.fk_wastewater_networkelement__REL
            ),
            "bemerkung": qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            "betreiberref": qgep_export_utils.get_tid(row.fk_operator__REL),
            "bezeichnung": qgep_export_utils.null_to_emptystr(row.identifier),
            "eigentuemerref": qgep_export_utils.get_tid(row.fk_owner__REL),
            "fremdwasseranfall": row.sewer_infiltration_water_production,
        }

    def connection_object_common_check_fk_in_subset(row):
        """
        Returns common attributes for connection_object
        """
        return {
            # "abwassernetzelementref": qgep_export_utils.get_tid(
            # row.fk_wastewater_networkelement__REL
            # ),
            "abwassernetzelementref": qgep_export_utils.check_fk_in_subsetid(
                subset_ids, row.fk_wastewater_networkelement__REL
            ),
            "bemerkung": qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            "betreiberref": qgep_export_utils.get_tid(row.fk_operator__REL),
            "bezeichnung": qgep_export_utils.null_to_emptystr(row.identifier),
            "eigentuemerref": qgep_export_utils.get_tid(row.fk_owner__REL),
            "fremdwasseranfall": row.sewer_infiltration_water_production,
        }

    def surface_runoff_parameters_common(row):
        """
        Returns common attributes for surface_runoff_parameters
        """
        return {
            "bemerkung": qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            "benetzungsverlust": row.wetting_loss,
            "bezeichnung": qgep_export_utils.null_to_emptystr(row.identifier),
            "einzugsgebietref": qgep_export_utils.get_tid(row.fk_catchment_area__REL),
            "muldenverlust": row.surface_storage,
            "verdunstungsverlust": row.evaporation_loss,
            "versickerungsverlust": row.infiltration_loss,
        }

    def overflow_common(row):
        """
        Returns common attributes for overflow
        """
        return {
            "abwasserknotenref": qgep_export_utils.get_tid(row.fk_wastewater_node__REL),
            "antrieb": qgep_export_utils.get_vl(row.actuation__REL),
            "bemerkung": qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            "bezeichnung": qgep_export_utils.null_to_emptystr(row.identifier),
            "bruttokosten": row.gross_costs,
            "einleitstelle": qgep_export_utils.null_to_emptystr(row.discharge_point),
            "fabrikat": row.brand,
            "funktion": qgep_export_utils.get_vl(row.function__REL),
            "qan_dim": row.qon_dim,
            "signaluebermittlung": qgep_export_utils.get_vl(row.signal_transmission__REL),
            "steuerung": qgep_export_utils.get_vl(row.control__REL),
            "steuerungszentraleref": qgep_export_utils.get_tid(row.fk_control_center__REL),
            "subventionen": row.subsidies,
            "ueberlaufcharakteristikref": qgep_export_utils.get_tid(row.fk_overflow_char__REL),
            "ueberlaufnachref": qgep_export_utils.get_tid(row.fk_overflow_to__REL),
            "verstellbarkeit": qgep_export_utils.get_vl(row.adjustability__REL),
        }

    # re_maintenance_event_wastewater_structure moved to end, as wastewater_structure and maintenance_event are not yet added

    logger.info("Exporting QGEP.mutation -> ABWASSER.mutation, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.mutation)
    # only export explicitly specified mutation objects if filtered
    if filtered:
        query = query.filter(qgep_model.mutation.obj_id.in_(subset_ids))
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.mutation

        # --- mutation ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        mutation = abwasser_model.mutation(
            # FIELDS TO MAP TO ABWASSER.mutation
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "mutation"),
            # --- mutation ---
            art=qgep_export_utils.get_vl(row.kind__REL),
            attribut=row.attribute,
            aufnahmedatum=row.date_time,
            aufnehmer=row.recorded_by,
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            # Model adapted with delta/delta_1.5.8_dss_upddate_attributes_class.sql
            # klasse=row.class,
            klasse=row.classname,
            letzter_wert=row.last_value,
            mutationsdatum=row.date_mutation,
            objekt=qgep_export_utils.null_to_emptystr(row.object),
            systembenutzer=qgep_export_utils.null_to_emptystr(row.user_system),
        )
        abwasser_session.add(mutation)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.aquifier -> ABWASSER.grundwasserleiter, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.aquifier)
    # always export all aquifier
    if filtered:
        # add sql statement to logger
        statement = query.statement
        logger.info(f" always export all aquifier datasets query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.aquifier

        # --- aquifier ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        grundwasserleiter = abwasser_model.grundwasserleiter(
            # FIELDS TO MAP TO ABWASSER.grundwasserleiter
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "grundwasserleiter"),
            # --- grundwasserleiter ---
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            maxgwspiegel=row.maximal_groundwater_level,
            mingwspiegel=row.minimal_groundwater_level,
            mittlerergwspiegel=row.average_groundwater_level,
            perimeter=ST_Force2D(row.perimeter_geometry),
        )
        abwasser_session.add(grundwasserleiter)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.river -> ABWASSER.fliessgewaesser, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.river)
    # always export all river
    if filtered:
        # add sql statement to logger
        statement = query.statement
        logger.info(f" always export all river datasets query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.river

        # --- surface_water_bodies ---
        # to do attributeslist of superclass
        # --- river ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        fliessgewaesser = abwasser_model.fliessgewaesser(
            # FIELDS TO MAP TO ABWASSER.fliessgewaesser
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "fliessgewaesser"),
            # --- oberflaechengewaesser ---
            **surface_water_bodies_common(row),
            # --- fliessgewaesser ---
            art=qgep_export_utils.get_vl(row.kind__REL),
        )
        abwasser_session.add(fliessgewaesser)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.lake -> ABWASSER.see, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.lake)
    # always export all lake
    if filtered:
        # add sql statement to logger
        statement = query.statement
        logger.info(f" always export all lake datasets query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.lake

        # --- surface_water_bodies ---
        # to do attributeslist of superclass
        # --- lake ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        see = abwasser_model.see(
            # FIELDS TO MAP TO ABWASSER.see
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "see"),
            # --- oberflaechengewaesser ---
            **surface_water_bodies_common(row),
            # --- see ---
            perimeter=ST_Force2D(row.perimeter_geometry),
        )
        abwasser_session.add(see)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.water_course_segment -> ABWASSER.gewaesserabschnitt, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.water_course_segment)
    # always export all water_course_segment
    if filtered:
        # add sql statement to logger
        statement = query.statement
        logger.info(f" always export all water_course_segment datasets query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.water_course_segment

        # --- water_course_segment ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        gewaesserabschnitt = abwasser_model.gewaesserabschnitt(
            # FIELDS TO MAP TO ABWASSER.gewaesserabschnitt
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "gewaesserabschnitt"),
            # --- gewaesserabschnitt ---
            abflussregime=qgep_export_utils.get_vl(row.discharge_regime__REL),
            algenbewuchs=qgep_export_utils.get_vl(row.algae_growth__REL),
            art=qgep_export_utils.get_vl(row.kind__REL),
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            bis=ST_Force2D(row.to_geometry),
            breitenvariabilitaet=qgep_export_utils.get_vl(row.width_variability__REL),
            fliessgewaesserref=qgep_export_utils.get_tid(row.fk_watercourse__REL),
            gefaelle=qgep_export_utils.get_vl(row.slope__REL),
            groesse=row.size,
            hoehenstufe=qgep_export_utils.get_vl(row.altitudinal_zone__REL),
            laengsprofil=qgep_export_utils.get_vl(row.length_profile__REL),
            linienfuehrung=qgep_export_utils.get_vl(row.section_morphology__REL),
            makrophytenbewuchs=qgep_export_utils.get_vl(row.macrophyte_coverage__REL),
            nutzung=qgep_export_utils.get_vl(row.utilisation__REL),
            oekom_klassifizierung=qgep_export_utils.get_vl(row.ecom_classification__REL),
            sohlenbreite=row.bed_with,
            tiefenvariabilitaet=qgep_export_utils.get_vl(row.depth_variability__REL),
            totholz=qgep_export_utils.get_vl(row.dead_wood__REL),
            von=ST_Force2D(row.from_geometry),
            wasserhaerte=qgep_export_utils.get_vl(row.water_hardness__REL),
        )
        abwasser_session.add(gewaesserabschnitt)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.water_catchment -> ABWASSER.wasserfassung, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.water_catchment)
    # always export all water_catchment
    if filtered:
        # add sql statement to logger
        statement = query.statement
        logger.info(f" always export all water_catchment datasets query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.water_catchment

        # --- water_catchment ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        wasserfassung = abwasser_model.wasserfassung(
            # FIELDS TO MAP TO ABWASSER.wasserfassung
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "wasserfassung"),
            # --- wasserfassung ---
            art=qgep_export_utils.get_vl(row.kind__REL),
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            grundwasserleiterref=qgep_export_utils.get_tid(row.fk_aquifier__REL),
            lage=ST_Force2D(row.situation_geometry),
            oberflaechengewaesserref=qgep_export_utils.get_tid(row.fk_surface_water_bodies__REL),
        )
        abwasser_session.add(wasserfassung)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.river_bank -> ABWASSER.ufer, ABWASSER.metaattribute")
    # always export all river_bank
    if filtered:
        # add sql statement to logger
        statement = query.statement
        logger.info(f" always export all river_bank datasets query = {statement}")
    query = qgep_session.query(qgep_model.river_bank)
    for row in query:

        # AVAILABLE FIELDS IN QGEP.river_bank

        # --- river_bank ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        ufer = abwasser_model.ufer(
            # FIELDS TO MAP TO ABWASSER.ufer
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "ufer"),
            # --- ufer ---
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            breite=row.width,
            gewaesserabschnittref=qgep_export_utils.get_tid(row.fk_water_course_segment__REL),
            seite=qgep_export_utils.get_vl(row.side__REL),
            uferbereich=qgep_export_utils.get_vl(row.shores__REL),
            umlandnutzung=qgep_export_utils.get_vl(row.utilisation_of_shore_surroundings__REL),
            vegetation=qgep_export_utils.get_vl(row.vegetation__REL),
            verbauungsart=qgep_export_utils.get_vl(row.river_control_type__REL),
            verbauungsgrad=qgep_export_utils.get_vl(row.control_grade_of_river__REL),
        )
        abwasser_session.add(ufer)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.river_bed -> ABWASSER.gewaessersohle, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.river_bed)
    # always export all river_bed
    if filtered:
        # add sql statement to logger
        statement = query.statement
        logger.info(f" always export all river_bed datasets query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.river_bed

        # --- river_bed ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        gewaessersohle = abwasser_model.gewaessersohle(
            # FIELDS TO MAP TO ABWASSER.gewaessersohle
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "gewaessersohle"),
            # --- gewaessersohle ---
            art=qgep_export_utils.get_vl(row.kind__REL),
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            breite=row.width,
            gewaesserabschnittref=qgep_export_utils.get_tid(row.fk_water_course_segment__REL),
            verbauungsart=qgep_export_utils.get_vl(row.river_control_type__REL),
            verbauungsgrad=qgep_export_utils.get_vl(row.control_grade_of_river__REL),
        )
        abwasser_session.add(gewaessersohle)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.sector_water_body -> ABWASSER.gewaessersektor, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.sector_water_body)
    # always export all sector_water_body
    if filtered:
        # add sql statement to logger
        statement = query.statement
        logger.info(f" always export all sector_water_body datasets query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.sector_water_body

        # --- sector_water_body ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        gewaessersektor = abwasser_model.gewaessersektor(
            # FIELDS TO MAP TO ABWASSER.gewaessersektor
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "gewaessersektor"),
            # --- gewaessersektor ---
            art=qgep_export_utils.get_vl(row.kind__REL),
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            bwg_code=row.code_bwg,
            kilomo=row.km_down,
            kilomu=row.km_up,
            oberflaechengewaesserref=qgep_export_utils.get_tid(row.fk_surface_water_bodies__REL),
            reflaenge=row.ref_length,
            verlauf=ST_Force2D(row.progression_geometry),
            # reference to own class not supported in qgep
            # vorherigersektorref=qgep_export_utils.get_tid(row.fk_sector_previous__REL),
        )
        abwasser_session.add(gewaessersektor)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.administrative_office -> ABWASSER.amt, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.administrative_office)
    # always export all administrative_office
    if filtered:
        # add sql statement to logger
        statement = query.statement
        logger.info(f" always export all administrative_office datasets query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.administrative_office

        # --- organisation ---
        # to do attributeslist of superclass
        # --- administrative_office ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        amt = abwasser_model.amt(
            # FIELDS TO MAP TO ABWASSER.amt
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "amt"),
            # --- organisation ---
            **organisation_common(row),
            # --- amt ---
        )
        abwasser_session.add(amt)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.cooperative -> ABWASSER.genossenschaft_korporation, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.cooperative)
    # always export all cooperative
    if filtered:
        # add sql statement to logger
        statement = query.statement
        logger.info(f" always export all cooperative datasets query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.cooperative

        # --- organisation ---
        # to do attributeslist of superclass
        # --- cooperative ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        genossenschaft_korporation = abwasser_model.genossenschaft_korporation(
            # FIELDS TO MAP TO ABWASSER.genossenschaft_korporation
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "genossenschaft_korporation"),
            # --- organisation ---
            **organisation_common(row),
            # --- genossenschaft_korporation ---
        )
        abwasser_session.add(genossenschaft_korporation)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.canton -> ABWASSER.kanton, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.canton)
    # always export all canton
    if filtered:
        # add sql statement to logger
        statement = query.statement
        logger.info(f" always export all canton datasets query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.canton

        # --- organisation ---
        # to do attributeslist of superclass
        # --- canton ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        kanton = abwasser_model.kanton(
            # FIELDS TO MAP TO ABWASSER.kanton
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "kanton"),
            # --- organisation ---
            **organisation_common(row),
            # --- kanton ---
            perimeter=ST_Force2D(row.perimeter_geometry),
        )
        abwasser_session.add(kanton)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.waste_water_association -> ABWASSER.abwasserverband, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.waste_water_association)
    # always export all waste_water_association
    if filtered:
        # add sql statement to logger
        statement = query.statement
        logger.info(f" always export all waste_water_association datasets query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.waste_water_association

        # --- organisation ---
        # to do attributeslist of superclass
        # --- waste_water_association ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        abwasserverband = abwasser_model.abwasserverband(
            # FIELDS TO MAP TO ABWASSER.abwasserverband
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "abwasserverband"),
            # --- organisation ---
            **organisation_common(row),
            # --- abwasserverband ---
        )
        abwasser_session.add(abwasserverband)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.municipality -> ABWASSER.gemeinde, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.municipality)
    # always export all municipality
    if filtered:
        # add sql statement to logger
        statement = query.statement
        logger.info(f" always export all municipality datasets query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.municipality

        # --- organisation ---
        # to do attributeslist of superclass
        # --- municipality ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        gemeinde = abwasser_model.gemeinde(
            # FIELDS TO MAP TO ABWASSER.gemeinde
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "gemeinde"),
            # --- organisation ---
            **organisation_common(row),
            # --- gemeinde ---
            einwohner=row.population,
            flaeche=row.total_surface,
            gemeindenummer=row.municipality_number,
            gep_jahr=row.gwdp_year,
            hoehe=row.altitude,
            perimeter=ST_Force2D(row.perimeter_geometry),
        )
        abwasser_session.add(gemeinde)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.waste_water_treatment_plant -> ABWASSER.abwasserreinigungsanlage, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.waste_water_treatment_plant)
    # always export all waste_water_treatment_plant
    if filtered:
        # add sql statement to logger
        statement = query.statement
        logger.info(f" always export all waste_water_treatment_plant datasets query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.waste_water_treatment_plant

        # --- organisation ---
        # to do attributeslist of superclass
        # --- waste_water_treatment_plant ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        abwasserreinigungsanlage = abwasser_model.abwasserreinigungsanlage(
            # FIELDS TO MAP TO ABWASSER.abwasserreinigungsanlage
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "abwasserreinigungsanlage"),
            # --- organisation ---
            **organisation_common(row),
            # --- abwasserreinigungsanlage ---
            anlagenummer=row.installation_number,
            art=row.kind,
            bsb5=row.bod5,
            csb=row.cod,
            eliminationcsb=row.elimination_cod,
            eliminationn=row.elimination_n,
            eliminationnh4=row.elimination_nh4,
            eliminationp=row.elimination_p,
            inbetriebnahme=row.start_year,
            nh4=row.nh4,
        )
        abwasser_session.add(abwasserreinigungsanlage)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.private -> ABWASSER.privat, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.private)
    # always export all private
    if filtered:
        # add sql statement to logger
        statement = query.statement
        logger.info(f" always export all private datasets query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.private

        # --- organisation ---
        # to do attributeslist of superclass
        # --- private ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        privat = abwasser_model.privat(
            # FIELDS TO MAP TO ABWASSER.privat
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "privat"),
            # --- organisation ---
            **organisation_common(row),
            # --- privat ---
            art=row.kind,
        )
        abwasser_session.add(privat)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

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
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
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
            # -- attribute 3D ---
            # deckenkote=row.upper_elevation,
            gewaessersektorref=qgep_export_utils.get_tid(row.fk_sector_water_body__REL),
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
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
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
            art=qgep_export_utils.get_vl(row.kind__REL),
            beschriftung=qgep_export_utils.get_vl(row.labeling__REL),
            # -- attribute 3D ---
            # deckenkote=row.upper_elevation,
            dimension1=row.dimension1,
            dimension2=row.dimension2,
            grundwasserleiterref=qgep_export_utils.get_tid(row.fk_aquifier__REL),
            gwdistanz=row.distance_to_aquifer,
            # -- attribute 3D ---
            # maechtigkeit=row.depth,
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

    logger.info("Exporting QGEP.wwtp_structure -> ABWASSER.arabauwerk, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.wwtp_structure)
    if filtered:
        query = query.join(qgep_model.wastewater_networkelement).filter(
            qgep_model.wastewater_networkelement.obj_id.in_(subset_ids)
        )
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.wwtp_structure

        # --- wastewater_structure ---
        # to do attributeslist of superclass
        # --- wwtp_structure ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        arabauwerk = abwasser_model.arabauwerk(
            # FIELDS TO MAP TO ABWASSER.arabauwerk
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "arabauwerk"),
            # --- abwasserbauwerk ---
            **qgep_export_utils.wastewater_structure_common(row),
            # --- arabauwerk ---
            art=qgep_export_utils.get_vl(row.kind__REL),
        )
        abwasser_session.add(arabauwerk)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.planning_zone -> ABWASSER.planungszone, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.planning_zone)
    for row in query:
        # AVAILABLE FIELDS IN QGEP.planning_zone

        # --- zone ---
        # to do attributeslist of superclass
        # --- planning_zone ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        planungszone = abwasser_model.planungszone(
            # FIELDS TO MAP TO ABWASSER.planungszone
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "planungszone"),
            # --- zone ---
            **zone_common(row),
            # --- planungszone ---
            art=qgep_export_utils.get_vl(row.kind__REL),
            perimeter=ST_Force2D(row.perimeter_geometry),
        )
        abwasser_session.add(planungszone)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.infiltration_zone -> ABWASSER.versickerungsbereich, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.infiltration_zone)
    for row in query:
        # AVAILABLE FIELDS IN QGEP.infiltration_zone

        # --- zone ---
        # to do attributeslist of superclass
        # --- infiltration_zone ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        versickerungsbereich = abwasser_model.versickerungsbereich(
            # FIELDS TO MAP TO ABWASSER.versickerungsbereich
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "versickerungsbereich"),
            # --- zone ---
            **zone_common(row),
            # --- versickerungsbereich ---
            perimeter=ST_Force2D(row.perimeter_geometry),
            versickerungsmoeglichkeit=qgep_export_utils.get_vl(row.infiltration_capacity__REL),
        )
        abwasser_session.add(versickerungsbereich)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.drainage_system -> ABWASSER.entwaesserungssystem, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.drainage_system)
    for row in query:
        # AVAILABLE FIELDS IN QGEP.drainage_system

        # --- zone ---
        # to do attributeslist of superclass
        # --- drainage_system ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        entwaesserungssystem = abwasser_model.entwaesserungssystem(
            # FIELDS TO MAP TO ABWASSER.entwaesserungssystem
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "entwaesserungssystem"),
            # --- zone ---
            **zone_common(row),
            # --- entwaesserungssystem ---
            art=qgep_export_utils.get_vl(row.kind__REL),
            perimeter=ST_Force2D(row.perimeter_geometry),
        )
        abwasser_session.add(entwaesserungssystem)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.water_body_protection_sector -> ABWASSER.gewaesserschutzbereich, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.water_body_protection_sector)
    for row in query:
        # AVAILABLE FIELDS IN QGEP.water_body_protection_sector

        # --- zone ---
        # to do attributeslist of superclass
        # --- water_body_protection_sector ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        gewaesserschutzbereich = abwasser_model.gewaesserschutzbereich(
            # FIELDS TO MAP TO ABWASSER.gewaesserschutzbereich
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "gewaesserschutzbereich"),
            # --- zone ---
            **zone_common(row),
            # --- gewaesserschutzbereich ---
            art=qgep_export_utils.get_vl(row.kind__REL),
            perimeter=ST_Force2D(row.perimeter_geometry),
        )
        abwasser_session.add(gewaesserschutzbereich)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.ground_water_protection_perimeter -> ABWASSER.grundwasserschutzareal, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.ground_water_protection_perimeter)
    for row in query:
        # AVAILABLE FIELDS IN QGEP.ground_water_protection_perimeter

        # --- zone ---
        # to do attributeslist of superclass
        # --- ground_water_protection_perimeter ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        grundwasserschutzareal = abwasser_model.grundwasserschutzareal(
            # FIELDS TO MAP TO ABWASSER.grundwasserschutzareal
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "grundwasserschutzareal"),
            # --- zone ---
            **zone_common(row),
            # --- grundwasserschutzareal ---
            perimeter=ST_Force2D(row.perimeter_geometry),
        )
        abwasser_session.add(grundwasserschutzareal)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.groundwater_protection_zone -> ABWASSER.grundwasserschutzzone, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.groundwater_protection_zone)
    for row in query:
        # AVAILABLE FIELDS IN QGEP.groundwater_protection_zone

        # --- zone ---
        # to do attributeslist of superclass
        # --- groundwater_protection_zone ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        grundwasserschutzzone = abwasser_model.grundwasserschutzzone(
            # FIELDS TO MAP TO ABWASSER.grundwasserschutzzone
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "grundwasserschutzzone"),
            # --- zone ---
            **zone_common(row),
            # --- grundwasserschutzzone ---
            art=qgep_export_utils.get_vl(row.kind__REL),
            perimeter=ST_Force2D(row.perimeter_geometry),
        )
        abwasser_session.add(grundwasserschutzzone)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.pipe_profile -> ABWASSER.rohrprofil, ABWASSER.metaattribute")
    qgep_export_utils.export_pipe_profile()

    logger.info(
        "Exporting QGEP.wwtp_energy_use -> ABWASSER.araenergienutzung, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.wwtp_energy_use)
    for row in query:

        # AVAILABLE FIELDS IN QGEP.wwtp_energy_use

        # --- wwtp_energy_use ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        araenergienutzung = abwasser_model.araenergienutzung(
            # FIELDS TO MAP TO ABWASSER.araenergienutzung
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "araenergienutzung"),
            # --- araenergienutzung ---
            abwasserreinigungsanlageref=qgep_export_utils.get_tid(
                row.fk_waste_water_treatment_plant__REL
            ),
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            gasmotor=row.gas_motor,
            turbinierung=row.turbining,
            waermepumpe=row.heat_pump,
        )
        abwasser_session.add(araenergienutzung)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.waste_water_treatment -> ABWASSER.abwasserbehandlung, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.waste_water_treatment)
    for row in query:

        # AVAILABLE FIELDS IN QGEP.waste_water_treatment

        # --- waste_water_treatment ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        abwasserbehandlung = abwasser_model.abwasserbehandlung(
            # FIELDS TO MAP TO ABWASSER.abwasserbehandlung
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "abwasserbehandlung"),
            # --- abwasserbehandlung ---
            abwasserreinigungsanlageref=qgep_export_utils.get_tid(
                row.fk_waste_water_treatment_plant__REL
            ),
            art=qgep_export_utils.get_vl(row.kind__REL),
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
        )
        abwasser_session.add(abwasserbehandlung)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.sludge_treatment -> ABWASSER.schlammbehandlung, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.sludge_treatment)
    for row in query:

        # AVAILABLE FIELDS IN QGEP.sludge_treatment

        # --- sludge_treatment ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        schlammbehandlung = abwasser_model.schlammbehandlung(
            # FIELDS TO MAP TO ABWASSER.schlammbehandlung
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "schlammbehandlung"),
            # --- schlammbehandlung ---
            abwasserreinigungsanlageref=qgep_export_utils.get_tid(
                row.fk_waste_water_treatment_plant__REL
            ),
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            entwaessertklaerschlammstapelung=row.stacking_of_dehydrated_sludge,
            entwaesserung=row.dehydration,
            faulschlammverbrennung=row.digested_sludge_combustion,
            fluessigklaerschlammstapelung=row.stacking_of_liquid_sludge,
            frischschlammverbrennung=row.fresh_sludge_combustion,
            hygienisierung=row.hygenisation,
            kompostierung=row.composting,
            mischschlammvoreindickung=row.predensification_of_mixed_sludge,
            primaerschlammvoreindickung=row.predensification_of_primary_sludge,
            stabilisierung=qgep_export_utils.get_vl(row.stabilisation__REL),
            trocknung=row.drying,
            ueberschusschlammvoreindickung=row.predensification_of_excess_sludge,
        )
        abwasser_session.add(schlammbehandlung)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.control_center -> ABWASSER.steuerungszentrale, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.control_center)
    if filtered:
        query = (
            query.join(qgep_model.throttle_shut_off_unit)
            .join(
                qgep_model.wastewater_node,
                qgep_model.wastewater_node.obj_id
                == qgep_model.throttle_shut_off_unit.fk_wastewater_node,
            )
            .filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        )
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.control_center

        # --- control_center ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        steuerungszentrale = abwasser_model.steuerungszentrale(
            # FIELDS TO MAP TO ABWASSER.steuerungszentrale
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "steuerungszentrale"),
            # --- steuerungszentrale ---
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            lage=ST_Force2D(row.situation_geometry),
        )
        abwasser_session.add(steuerungszentrale)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.ford -> ABWASSER.furt, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.ford)
    for row in query:
        # AVAILABLE FIELDS IN QGEP.ford

        # --- water_control_structure ---
        # to do attributeslist of superclass
        # --- ford ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        furt = abwasser_model.furt(
            # FIELDS TO MAP TO ABWASSER.furt
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "furt"),
            # --- gewaesserverbauung ---
            **water_control_structure_common(row),
            # --- furt ---
        )
        abwasser_session.add(furt)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.chute -> ABWASSER.gewaesserabsturz, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.chute)
    for row in query:
        # AVAILABLE FIELDS IN QGEP.chute

        # --- water_control_structure ---
        # to do attributeslist of superclass
        # --- chute ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        gewaesserabsturz = abwasser_model.gewaesserabsturz(
            # FIELDS TO MAP TO ABWASSER.gewaesserabsturz
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "gewaesserabsturz"),
            # --- gewaesserverbauung ---
            **water_control_structure_common(row),
            # --- gewaesserabsturz ---
            absturzhoehe=row.vertical_drop,
            material=qgep_export_utils.get_vl(row.material__REL),
            typ=qgep_export_utils.get_vl(row.kind__REL),
        )
        abwasser_session.add(gewaesserabsturz)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.lock -> ABWASSER.schleuse, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.lock)
    for row in query:
        # AVAILABLE FIELDS IN QGEP.lock

        # --- water_control_structure ---
        # to do attributeslist of superclass
        # --- lock ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        schleuse = abwasser_model.schleuse(
            # FIELDS TO MAP TO ABWASSER.schleuse
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "schleuse"),
            # --- gewaesserverbauung ---
            **water_control_structure_common(row),
            # --- schleuse ---
            absturzhoehe=row.vertical_drop,
        )
        abwasser_session.add(schleuse)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.passage -> ABWASSER.durchlass, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.passage)
    for row in query:
        # AVAILABLE FIELDS IN QGEP.passage

        # --- water_control_structure ---
        # to do attributeslist of superclass
        # --- passage ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        durchlass = abwasser_model.durchlass(
            # FIELDS TO MAP TO ABWASSER.durchlass
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "durchlass"),
            # --- gewaesserverbauung ---
            **water_control_structure_common(row),
            # --- durchlass ---
        )
        abwasser_session.add(durchlass)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.blocking_debris -> ABWASSER.geschiebesperre, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.blocking_debris)
    for row in query:
        # AVAILABLE FIELDS IN QGEP.blocking_debris

        # --- water_control_structure ---
        # to do attributeslist of superclass
        # --- blocking_debris ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        geschiebesperre = abwasser_model.geschiebesperre(
            # FIELDS TO MAP TO ABWASSER.geschiebesperre
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "geschiebesperre"),
            # --- gewaesserverbauung ---
            **water_control_structure_common(row),
            # --- geschiebesperre ---
            absturzhoehe=row.vertical_drop,
        )
        abwasser_session.add(geschiebesperre)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.dam -> ABWASSER.gewaesserwehr, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.dam)
    for row in query:
        # AVAILABLE FIELDS IN QGEP.dam

        # --- water_control_structure ---
        # to do attributeslist of superclass
        # --- dam ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        gewaesserwehr = abwasser_model.gewaesserwehr(
            # FIELDS TO MAP TO ABWASSER.gewaesserwehr
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "gewaesserwehr"),
            # --- gewaesserverbauung ---
            **water_control_structure_common(row),
            # --- gewaesserwehr ---
            absturzhoehe=row.vertical_drop,
            art=qgep_export_utils.get_vl(row.kind__REL),
        )
        abwasser_session.add(gewaesserwehr)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.rock_ramp -> ABWASSER.sohlrampe, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.rock_ramp)
    for row in query:
        # AVAILABLE FIELDS IN QGEP.rock_ramp

        # --- water_control_structure ---
        # to do attributeslist of superclass
        # --- rock_ramp ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        sohlrampe = abwasser_model.sohlrampe(
            # FIELDS TO MAP TO ABWASSER.sohlrampe
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "sohlrampe"),
            # --- gewaesserverbauung ---
            **water_control_structure_common(row),
            # --- sohlrampe ---
            absturzhoehe=row.vertical_drop,
            befestigung=qgep_export_utils.get_vl(row.stabilisation__REL),
        )
        abwasser_session.add(sohlrampe)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.fish_pass -> ABWASSER.fischpass, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.fish_pass)
    for row in query:

        # AVAILABLE FIELDS IN QGEP.fish_pass

        # --- fish_pass ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        fischpass = abwasser_model.fischpass(
            # FIELDS TO MAP TO ABWASSER.fischpass
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "fischpass"),
            # --- fischpass ---
            absturzhoehe=row.vertical_drop,
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            gewaesserverbauungref=qgep_export_utils.get_tid(row.fk_water_control_structure__REL),
        )
        abwasser_session.add(fischpass)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.bathing_area -> ABWASSER.badestelle, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.bathing_area)
    for row in query:

        # AVAILABLE FIELDS IN QGEP.bathing_area

        # --- bathing_area ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        badestelle = abwasser_model.badestelle(
            # FIELDS TO MAP TO ABWASSER.badestelle
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "badestelle"),
            # --- badestelle ---
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            lage=ST_Force2D(row.situation_geometry),
            oberflaechengewaesserref=qgep_export_utils.get_tid(row.fk_surface_water_bodies__REL),
        )
        abwasser_session.add(badestelle)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.hydr_geometry -> ABWASSER.hydr_geometrie, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.hydr_geometry)
    if filtered:
        query = query.join(qgep_model.wastewater_node).filter(
            qgep_model.wastewater_networkelement.obj_id.in_(subset_ids)
        )
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.hydr_geometry

        # --- hydr_geometry ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        hydr_geometrie = abwasser_model.hydr_geometrie(
            # FIELDS TO MAP TO ABWASSER.hydr_geometrie
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "hydr_geometrie"),
            # --- hydr_geometrie ---
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            nutzinhalt=row.utilisable_capacity,
            nutzinhalt_fangteil=row.usable_capacity_storage,
            nutzinhalt_klaerteil=row.usable_capacity_treatment,
            stauraum=row.storage_volume,
            volumen_pumpensumpf=row.volume_pump_sump,
        )
        abwasser_session.add(hydr_geometrie)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    # with or without check_fk_in_subset
    if filtered:
        logger.info("Exporting QGEP.reach_point -> ABWASSER.haltungspunkt, ABWASSER.metaattribute")
        qgep_export_utils.export_reach_point_check_fk_in_subset()

        logger.info(
            "Exporting QGEP.wastewater_node -> ABWASSER.abwasserknoten, ABWASSER.metaattribute"
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
                hydr_geometrieref=qgep_export_utils.get_tid(row.fk_hydr_geometry__REL),
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
        qgep_export_utils.export_reach_check_fk_in_subset()

    else:
        logger.info("Exporting QGEP.reach_point -> ABWASSER.haltungspunkt, ABWASSER.metaattribute")
        qgep_export_utils.export_reach_point()

        logger.info(
            "Exporting QGEP.wastewater_node -> ABWASSER.abwasserknoten, ABWASSER.metaattribute"
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
                **qgep_export_utils.wastewater_networkelement_common(row),
                # --- abwasserknoten ---
                hydr_geometrieref=qgep_export_utils.get_tid(row.fk_hydr_geometry__REL),
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

    logger.info(
        "Exporting QGEP.profile_geometry -> ABWASSER.rohrprofil_geometrie, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.profile_geometry)
    # always export all profile_geometry
    if filtered:
        # add sql statement to logger
        statement = query.statement
        logger.info(f" always export all profile_geometry datasets query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.profile_geometry

        # --- profile_geometry ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        rohrprofil_geometrie = abwasser_model.rohrprofil_geometrie(
            # FIELDS TO MAP TO ABWASSER.rohrprofil_geometrie
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "rohrprofil_geometrie"),
            # --- rohrprofil_geometrie ---
            aposition=row.position,
            rohrprofilref=qgep_export_utils.get_tid(row.fk_pipe_profile__REL),
            x=row.x,
            y=row.y,
        )
        abwasser_session.add(rohrprofil_geometrie)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.hydr_geom_relation -> ABWASSER.hydr_geomrelation, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.hydr_geom_relation)
    # always export all hydr_geom_relation - does not work -> fk_errors
    if filtered:
        query = (
            query.join(qgep_model.hydr_geometry)
            .join(qgep_model.wastewater_node)
            .filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        )
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.hydr_geom_relation

        # --- hydr_geom_relation ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        hydr_geomrelation = abwasser_model.hydr_geomrelation(
            # FIELDS TO MAP TO ABWASSER.hydr_geomrelation
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "hydr_geomrelation"),
            # --- hydr_geomrelation ---
            benetztequerschnittsflaeche=row.wet_cross_section_area,
            hydr_geometrieref=qgep_export_utils.get_tid(row.fk_hydr_geometry__REL),
            wasseroberflaeche=row.water_surface,
            wassertiefe=row.water_depth,
        )
        abwasser_session.add(hydr_geomrelation)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.mechanical_pretreatment -> ABWASSER.mechanischevorreinigung, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.mechanical_pretreatment)
    # specify relation key - only directly to wastewater_structure
    if filtered:
        query = (
            query.join(
                qgep_model.wastewater_structure,
                qgep_model.mechanical_pretreatment.fk_wastewater_structure
                == qgep_model.wastewater_structure.obj_id,
            )
            .join(qgep_model.wastewater_networkelement)
            .filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        )
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.mechanical_pretreatment

        # --- mechanical_pretreatment ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        mechanischevorreinigung = abwasser_model.mechanischevorreinigung(
            # FIELDS TO MAP TO ABWASSER.mechanischevorreinigung
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "mechanischevorreinigung"),
            # --- mechanischevorreinigung ---
            abwasserbauwerkref=qgep_export_utils.get_tid(row.fk_wastewater_structure__REL),
            art=qgep_export_utils.get_vl(row.kind__REL),
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            versickerungsanlageref=qgep_export_utils.get_tid(
                row.fk_infiltration_installation__REL
            ),
        )
        abwasser_session.add(mechanischevorreinigung)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.retention_body -> ABWASSER.retentionskoerper, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.retention_body)
    # specify relation key - only directly to wastewater_structure
    if filtered:
        query = (
            query.join(
                qgep_model.infiltration_installation,
                qgep_model.retention_body.fk_infiltration_installation
                == qgep_model.infiltration_installation.obj_id,
            )
            .join(qgep_model.wastewater_networkelement)
            .filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        )
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.retention_body

        # --- retention_body ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        retentionskoerper = abwasser_model.retentionskoerper(
            # FIELDS TO MAP TO ABWASSER.retentionskoerper
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "retentionskoerper"),
            # --- retentionskoerper ---
            art=qgep_export_utils.get_vl(row.kind__REL),
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            retention_volumen=row.volume,
            versickerungsanlageref=qgep_export_utils.get_tid(
                row.fk_infiltration_installation__REL
            ),
        )
        abwasser_session.add(retentionskoerper)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.overflow_char -> ABWASSER.ueberlaufcharakteristik, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.overflow_char)
    # always export all overflow_char datasets
    if filtered:
        # add sql statement to logger
        statement = query.statement
        logger.info(f" always export all overflow_char datasets query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.overflow_char

        # --- overflow_char ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        ueberlaufcharakteristik = abwasser_model.ueberlaufcharakteristik(
            # FIELDS TO MAP TO ABWASSER.ueberlaufcharakteristik
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "ueberlaufcharakteristik"),
            # --- ueberlaufcharakteristik ---
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            kennlinie_digital=qgep_export_utils.get_vl(row.overflow_char_digital__REL),
            kennlinie_typ=qgep_export_utils.get_vl(row.kind_overflow_char__REL),
        )
        abwasser_session.add(ueberlaufcharakteristik)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.hq_relation -> ABWASSER.hq_relation, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.hq_relation)
    if filtered:
        # just check if overflow_char exists, but no filter
        query = query.join(
            qgep_model.overflow_char,
        )
        # add sql statement to logger
        statement = query.statement
        logger.info(f" selection query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.hq_relation

        # --- hq_relation ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        hq_relation = abwasser_model.hq_relation(
            # FIELDS TO MAP TO ABWASSER.hq_relation
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "hq_relation"),
            # --- hq_relation ---
            abfluss=row.flow,
            hoehe=row.altitude,
            ueberlaufcharakteristikref=qgep_export_utils.get_tid(row.fk_overflow_char__REL),
            zufluss=row.flow_from,
        )
        abwasser_session.add(hq_relation)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

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

    logger.info(
        "Exporting QGEP.electric_equipment -> ABWASSER.elektrischeeinrichtung, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.electric_equipment)
    if filtered:
        # query = (
        # query.join(
        # qgep_model.wastewater_structure,
        # qgep_model.structure_part.fk_wastewater_structure
        # == qgep_model.wastewater_structure.obj_id,
        # )
        # .join(qgep_model.wastewater_networkelement)
        # .filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        # )
        # filtering only on wastewater_structures that are in subset_wws_ids
        query = query.join(
            qgep_model.wastewater_structure,
            qgep_model.structure_part.fk_wastewater_structure
            == qgep_model.wastewater_structure.obj_id,
        ).filter(qgep_model.wastewater_structure.obj_id.in_(subset_wws_ids))
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.electric_equipment

        # --- structure_part ---
        # to do attributeslist of superclass
        # --- electric_equipment ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        elektrischeeinrichtung = abwasser_model.elektrischeeinrichtung(
            # FIELDS TO MAP TO ABWASSER.elektrischeeinrichtung
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "elektrischeeinrichtung"),
            # --- bauwerksteil ---
            **qgep_export_utils.structure_part_common(row),
            # --- elektrischeeinrichtung ---
            art=qgep_export_utils.get_vl(row.kind__REL),
            bruttokosten=row.gross_costs,
            ersatzjahr=row.year_of_replacement,
        )
        abwasser_session.add(elektrischeeinrichtung)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.electromechanical_equipment -> ABWASSER.elektromechanischeausruestung, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.electromechanical_equipment)
    if filtered:
        # query = (
        # query.join(
        # qgep_model.wastewater_structure,
        # qgep_model.structure_part.fk_wastewater_structure
        # == qgep_model.wastewater_structure.obj_id,
        # )
        # .join(qgep_model.wastewater_networkelement)
        # .filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        # )
        query = query.join(
            qgep_model.wastewater_structure,
            qgep_model.structure_part.fk_wastewater_structure
            == qgep_model.wastewater_structure.obj_id,
        ).filter(qgep_model.wastewater_structure.obj_id.in_(subset_wws_ids))
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.electromechanical_equipment

        # --- structure_part ---
        # to do attributeslist of superclass
        # --- electromechanical_equipment ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        elektromechanischeausruestung = abwasser_model.elektromechanischeausruestung(
            # FIELDS TO MAP TO ABWASSER.elektromechanischeausruestung
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "elektromechanischeausruestung"),
            # --- bauwerksteil ---
            **qgep_export_utils.structure_part_common(row),
            # --- elektromechanischeausruestung ---
            art=qgep_export_utils.get_vl(row.kind__REL),
            bruttokosten=row.gross_costs,
            ersatzjahr=row.year_of_replacement,
        )
        abwasser_session.add(elektromechanischeausruestung)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.benching -> ABWASSER.bankett, ABWASSER.metaattribute")
    qgep_export_utils.export_benching()

    logger.info("Exporting QGEP.building -> ABWASSER.gebaeude, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.building)
    if filtered:
        query = query.join(qgep_model.wastewater_networkelement).filter(
            qgep_model.wastewater_networkelement.obj_id.in_(subset_ids)
        )
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.building

        # --- connection_object ---
        # to do attributeslist of superclass
        # --- building ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        if filtered or ws_off_sia405abwasser:
            gebaeude = abwasser_model.gebaeude(
                # FIELDS TO MAP TO ABWASSER.gebaeude
                # --- baseclass ---
                # --- sia405_baseclass ---
                **qgep_export_utils.base_common(row, "gebaeude"),
                # --- anschlussobjekt ---
                **connection_object_common_check_fk_in_subset(row),
                # --- gebaeude ---
                hausnummer=row.house_number,
                perimeter=ST_Force2D(row.perimeter_geometry),
                referenzpunkt=ST_Force2D(row.reference_point_geometry),
                standortname=row.location_name,
            )
        else:
            gebaeude = abwasser_model.gebaeude(
                # FIELDS TO MAP TO ABWASSER.gebaeude
                # --- baseclass ---
                # --- sia405_baseclass ---
                **qgep_export_utils.base_common(row, "gebaeude"),
                # --- anschlussobjekt ---
                **connection_object_common(row),
                # --- gebaeude ---
                hausnummer=row.house_number,
                perimeter=ST_Force2D(row.perimeter_geometry),
                referenzpunkt=ST_Force2D(row.reference_point_geometry),
                standortname=row.location_name,
            )
        abwasser_session.add(gebaeude)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.reservoir -> ABWASSER.reservoir, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.reservoir)
    if filtered:
        query = query.join(qgep_model.wastewater_networkelement).filter(
            qgep_model.wastewater_networkelement.obj_id.in_(subset_ids)
        )
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.reservoir

        # --- connection_object ---
        # to do attributeslist of superclass
        # --- reservoir ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        if filtered or ws_off_sia405abwasser:
            reservoir = abwasser_model.reservoir(
                # FIELDS TO MAP TO ABWASSER.reservoir
                # --- baseclass ---
                # --- sia405_baseclass ---
                **qgep_export_utils.base_common(row, "reservoir"),
                # --- anschlussobjekt ---
                **connection_object_common_check_fk_in_subset(row),
                # --- reservoir ---
                lage=ST_Force2D(row.situation_geometry),
                standortname=row.location_name,
            )
        else:
            reservoir = abwasser_model.reservoir(
                # FIELDS TO MAP TO ABWASSER.reservoir
                # --- baseclass ---
                # --- sia405_baseclass ---
                **qgep_export_utils.base_common(row, "reservoir"),
                # --- anschlussobjekt ---
                **connection_object_common(row),
                # --- reservoir ---
                lage=ST_Force2D(row.situation_geometry),
                standortname=row.location_name,
            )
        abwasser_session.add(reservoir)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.individual_surface -> ABWASSER.einzelflaeche, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.individual_surface)
    if filtered:
        query = query.join(qgep_model.wastewater_networkelement).filter(
            qgep_model.wastewater_networkelement.obj_id.in_(subset_ids)
        )
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.individual_surface

        # --- connection_object ---
        # to do attributeslist of superclass
        # --- individual_surface ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        if filtered or ws_off_sia405abwasser:
            einzelflaeche = abwasser_model.einzelflaeche(
                # FIELDS TO MAP TO ABWASSER.einzelflaeche
                # --- baseclass ---
                # --- sia405_baseclass ---
                **qgep_export_utils.base_common(row, "einzelflaeche"),
                # --- anschlussobjekt ---
                **connection_object_common_check_fk_in_subset(row),
                # --- einzelflaeche ---
                befestigung=qgep_export_utils.get_vl(row.pavement__REL),
                funktion=qgep_export_utils.get_vl(row.function__REL),
                neigung=row.inclination,
                perimeter=ST_Force2D(row.perimeter_geometry),
            )
        else:
            einzelflaeche = abwasser_model.einzelflaeche(
                # FIELDS TO MAP TO ABWASSER.einzelflaeche
                # --- baseclass ---
                # --- sia405_baseclass ---
                **qgep_export_utils.base_common(row, "einzelflaeche"),
                # --- anschlussobjekt ---
                **connection_object_common(row),
                # --- einzelflaeche ---
                befestigung=qgep_export_utils.get_vl(row.pavement__REL),
                funktion=qgep_export_utils.get_vl(row.function__REL),
                neigung=row.inclination,
                perimeter=ST_Force2D(row.perimeter_geometry),
            )
        abwasser_session.add(einzelflaeche)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.fountain -> ABWASSER.brunnen, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.fountain)
    if filtered:
        query = query.join(qgep_model.wastewater_networkelement).filter(
            qgep_model.wastewater_networkelement.obj_id.in_(subset_ids)
        )
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.fountain

        # --- connection_object ---
        # to do attributeslist of superclass
        # --- fountain ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        if filtered or ws_off_sia405abwasser:
            brunnen = abwasser_model.brunnen(
                # FIELDS TO MAP TO ABWASSER.brunnen
                # --- baseclass ---
                # --- sia405_baseclass ---
                **qgep_export_utils.base_common(row, "brunnen"),
                # --- anschlussobjekt ---
                **connection_object_common_check_fk_in_subset(row),
                # --- brunnen ---
                lage=ST_Force2D(row.situation_geometry),
                standortname=row.location_name,
            )
        else:
            brunnen = abwasser_model.brunnen(
                # FIELDS TO MAP TO ABWASSER.brunnen
                # --- baseclass ---
                # --- sia405_baseclass ---
                **qgep_export_utils.base_common(row, "brunnen"),
                # --- anschlussobjekt ---
                **connection_object_common(row),
                # --- brunnen ---
                lage=ST_Force2D(row.situation_geometry),
                standortname=row.location_name,
            )
        abwasser_session.add(brunnen)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.hazard_source -> ABWASSER.gefahrenquelle, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.hazard_source)
    if filtered:
        query = (
            query.join(
                qgep_model.connection_object,
                qgep_model.hazard_source.fk_connection_object
                == qgep_model.connection_object.obj_id,
            )
            .join(qgep_model.wastewater_networkelement)
            .filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        )
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.hazard_source

        # --- hazard_source ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        gefahrenquelle = abwasser_model.gefahrenquelle(
            # FIELDS TO MAP TO ABWASSER.gefahrenquelle
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "gefahrenquelle"),
            # --- gefahrenquelle ---
            anschlussobjektref=qgep_export_utils.get_tid(row.fk_connection_object__REL),
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            eigentuemerref=qgep_export_utils.get_tid(row.fk_owner__REL),
            lage=ST_Force2D(row.situation_geometry),
        )
        abwasser_session.add(gefahrenquelle)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.accident -> ABWASSER.unfall, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.accident)
    if filtered:
        query = (
            query.join(qgep_model.hazard_source)
            .join(
                qgep_model.connection_object,
                qgep_model.hazard_source.fk_connection_object
                == qgep_model.connection_object.obj_id,
            )
            .join(qgep_model.wastewater_networkelement)
        ).filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.accident

        # --- accident ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        unfall = abwasser_model.unfall(
            # FIELDS TO MAP TO ABWASSER.unfall
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "unfall"),
            # --- unfall ---
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            datum=row.date,
            gefahrenquelleref=qgep_export_utils.get_tid(row.fk_hazard_source__REL),
            lage=ST_Force2D(row.situation_geometry),
            ort=row.place,
            verursacher=row.responsible,
        )
        abwasser_session.add(unfall)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.substance -> ABWASSER.stoff, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.substance)
    if filtered:

        query = (
            query.join(qgep_model.hazard_source)
            .join(
                qgep_model.connection_object,
                qgep_model.hazard_source.fk_connection_object
                == qgep_model.connection_object.obj_id,
            )
            .join(qgep_model.wastewater_networkelement)
        ).filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.substance

        # --- substance ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        stoff = abwasser_model.stoff(
            # FIELDS TO MAP TO ABWASSER.stoff
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "stoff"),
            # --- stoff ---
            art=row.kind,
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            gefahrenquelleref=qgep_export_utils.get_tid(row.fk_hazard_source__REL),
            lagerung=row.stockage,
        )
        abwasser_session.add(stoff)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.catchment_area -> ABWASSER.einzugsgebiet, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.catchment_area)
    if filtered:
        query = query.join(
            qgep_model.wastewater_networkelement,
            or_(
                qgep_model.wastewater_networkelement.obj_id
                == qgep_model.catchment_area.fk_wastewater_networkelement_rw_planned,
                qgep_model.wastewater_networkelement.obj_id
                == qgep_model.catchment_area.fk_wastewater_networkelement_rw_current,
                qgep_model.wastewater_networkelement.obj_id
                == qgep_model.catchment_area.fk_wastewater_networkelement_ww_planned,
                qgep_model.wastewater_networkelement.obj_id
                == qgep_model.catchment_area.fk_wastewater_networkelement_ww_current,
            ),
        ).filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.catchment_area

        # --- catchment_area ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        if filtered or ws_off_sia405abwasser:
            einzugsgebiet = abwasser_model.einzugsgebiet(
                # FIELDS TO MAP TO ABWASSER.einzugsgebiet
                # --- baseclass ---
                # --- sia405_baseclass ---
                **qgep_export_utils.base_common(row, "einzugsgebiet"),
                # --- einzugsgebiet ---
                abflussbegrenzung_geplant=row.runoff_limit_planned,
                abflussbegrenzung_ist=row.runoff_limit_current,
                abflussbeiwert_rw_geplant=row.discharge_coefficient_rw_planned,
                abflussbeiwert_rw_ist=row.discharge_coefficient_rw_current,
                abflussbeiwert_sw_geplant=row.discharge_coefficient_ww_planned,
                abflussbeiwert_sw_ist=row.discharge_coefficient_ww_current,
                # changed call from qgep_export_utils.get_tid to qgep_export_utils.check_fk_in_subsetid so it does not write foreignkeys on elements that do not exist
                abwassernetzelement_rw_geplantref=qgep_export_utils.check_fk_in_subsetid(
                    subset_ids, row.fk_wastewater_networkelement_rw_planned__REL
                ),
                abwassernetzelement_rw_istref=qgep_export_utils.check_fk_in_subsetid(
                    subset_ids, row.fk_wastewater_networkelement_rw_current__REL
                ),
                abwassernetzelement_sw_geplantref=qgep_export_utils.check_fk_in_subsetid(
                    subset_ids, row.fk_wastewater_networkelement_ww_planned__REL
                ),
                abwassernetzelement_sw_istref=qgep_export_utils.check_fk_in_subsetid(
                    subset_ids, row.fk_wastewater_networkelement_ww_current__REL
                ),
                befestigungsgrad_rw_geplant=row.seal_factor_rw_planned,
                befestigungsgrad_rw_ist=row.seal_factor_rw_current,
                befestigungsgrad_sw_geplant=row.seal_factor_ww_planned,
                befestigungsgrad_sw_ist=row.seal_factor_ww_current,
                bemerkung=qgep_export_utils.truncate(
                    qgep_export_utils.emptystr_to_null(row.remark), 80
                ),
                bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
                direkteinleitung_in_gewaesser_geplant=qgep_export_utils.get_vl(
                    row.direct_discharge_planned__REL
                ),
                direkteinleitung_in_gewaesser_ist=qgep_export_utils.get_vl(
                    row.direct_discharge_current__REL
                ),
                einwohnerdichte_geplant=row.population_density_planned,
                einwohnerdichte_ist=row.population_density_current,
                entwaesserungssystem_geplant=qgep_export_utils.get_vl(
                    row.drainage_system_planned__REL
                ),
                entwaesserungssystem_ist=qgep_export_utils.get_vl(
                    row.drainage_system_current__REL
                ),
                flaeche=row.surface_area,
                fremdwasseranfall_geplant=row.sewer_infiltration_water_production_planned,
                fremdwasseranfall_ist=row.sewer_infiltration_water_production_current,
                perimeter=ST_Force2D(row.perimeter_geometry),
                retention_geplant=qgep_export_utils.get_vl(row.retention_planned__REL),
                retention_ist=qgep_export_utils.get_vl(row.retention_current__REL),
                # sbw_*ref will be added with release 2020
                # sbw_rw_geplantref=qgep_export_utils.get_tid(row.fk_special_building_rw_planned__REL),
                # sbw_rw_istref=qgep_export_utils.get_tid(row.fk_special_building_rw_current__REL),
                # sbw_sw_geplantref=qgep_export_utils.get_tid(row.fk_special_building_ww_planned__REL),
                # sbw_sw_istref=qgep_export_utils.get_tid(row.fk_special_building_ww_current__REL),
                schmutzabwasseranfall_geplant=row.waste_water_production_planned,
                schmutzabwasseranfall_ist=row.waste_water_production_current,
                versickerung_geplant=qgep_export_utils.get_vl(row.infiltration_planned__REL),
                versickerung_ist=qgep_export_utils.get_vl(row.infiltration_current__REL),
            )
        else:
            einzugsgebiet = abwasser_model.einzugsgebiet(
                # FIELDS TO MAP TO ABWASSER.einzugsgebiet
                # --- baseclass ---
                # --- sia405_baseclass ---
                **qgep_export_utils.base_common(row, "einzugsgebiet"),
                # --- einzugsgebiet ---
                abflussbegrenzung_geplant=row.runoff_limit_planned,
                abflussbegrenzung_ist=row.runoff_limit_current,
                abflussbeiwert_rw_geplant=row.discharge_coefficient_rw_planned,
                abflussbeiwert_rw_ist=row.discharge_coefficient_rw_current,
                abflussbeiwert_sw_geplant=row.discharge_coefficient_ww_planned,
                abflussbeiwert_sw_ist=row.discharge_coefficient_ww_current,
                # changed call from qgep_export_utils.get_tid to qgep_export_utils.check_fk_in_subsetid so it does not write foreignkeys on elements that do not exist
                abwassernetzelement_rw_geplantref=qgep_export_utils.get_tid(
                    row.fk_wastewater_networkelement_rw_planned__REL
                ),
                abwassernetzelement_rw_istref=qgep_export_utils.get_tid(
                    row.fk_wastewater_networkelement_rw_current__REL
                ),
                abwassernetzelement_sw_geplantref=qgep_export_utils.get_tid(
                    row.fk_wastewater_networkelement_ww_planned__REL
                ),
                abwassernetzelement_sw_istref=qgep_export_utils.get_tid(
                    row.fk_wastewater_networkelement_ww_current__REL
                ),
                befestigungsgrad_rw_geplant=row.seal_factor_rw_planned,
                befestigungsgrad_rw_ist=row.seal_factor_rw_current,
                befestigungsgrad_sw_geplant=row.seal_factor_ww_planned,
                befestigungsgrad_sw_ist=row.seal_factor_ww_current,
                bemerkung=qgep_export_utils.truncate(
                    qgep_export_utils.emptystr_to_null(row.remark), 80
                ),
                bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
                direkteinleitung_in_gewaesser_geplant=qgep_export_utils.get_vl(
                    row.direct_discharge_planned__REL
                ),
                direkteinleitung_in_gewaesser_ist=qgep_export_utils.get_vl(
                    row.direct_discharge_current__REL
                ),
                einwohnerdichte_geplant=row.population_density_planned,
                einwohnerdichte_ist=row.population_density_current,
                entwaesserungssystem_geplant=qgep_export_utils.get_vl(
                    row.drainage_system_planned__REL
                ),
                entwaesserungssystem_ist=qgep_export_utils.get_vl(
                    row.drainage_system_current__REL
                ),
                flaeche=row.surface_area,
                fremdwasseranfall_geplant=row.sewer_infiltration_water_production_planned,
                fremdwasseranfall_ist=row.sewer_infiltration_water_production_current,
                perimeter=ST_Force2D(row.perimeter_geometry),
                retention_geplant=qgep_export_utils.get_vl(row.retention_planned__REL),
                retention_ist=qgep_export_utils.get_vl(row.retention_current__REL),
                # sbw_*ref will be added with release 2020
                # sbw_rw_geplantref=qgep_export_utils.get_tid(row.fk_special_building_rw_planned__REL),
                # sbw_rw_istref=qgep_export_utils.get_tid(row.fk_special_building_rw_current__REL),
                # sbw_sw_geplantref=qgep_export_utils.get_tid(row.fk_special_building_ww_planned__REL),
                # sbw_sw_istref=qgep_export_utils.get_tid(row.fk_special_building_ww_current__REL),
                schmutzabwasseranfall_geplant=row.waste_water_production_planned,
                schmutzabwasseranfall_ist=row.waste_water_production_current,
                versickerung_geplant=qgep_export_utils.get_vl(row.infiltration_planned__REL),
                versickerung_ist=qgep_export_utils.get_vl(row.infiltration_current__REL),
            )

        abwasser_session.add(einzugsgebiet)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.measuring_point -> ABWASSER.messstelle, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.measuring_point)
    if filtered:
        query1 = query.join(
            qgep_model.wastewater_structure,
            qgep_model.measuring_point.fk_wastewater_structure
            == qgep_model.wastewater_structure.obj_id,
        ).join(qgep_model.wastewater_networkelement)

        # needs to add QGEP.wastewater_structure as waste_water_treatment_plant is a subclass of organisation that has a relation to wastewater_structure and then wastewater_networkelement
        # variant1 for query2
        # query2=query.join(
        #   QGEP.waste_water_treatment_plant,
        #   (QGEP.wastewater_structure, QGEP.waste_water_treatment_plant.obj_id == QGEP.wastewater_structure.fk_owner),
        #   (QGEP.wastewater_structure, QGEP.waste_water_treatment_plant.obj_id == QGEP.wastewater_structure.fk_provider),
        #   QGEP.wastewater_networkelement,
        # )
        # variant2 for query2
        # try with extra or_
        # or_(
        # QGEP.waste_water_treatment_plant.obj_id == QGEP.wastewater_structure.fk_owner,
        # QGEP.waste_water_treatment_plant.obj_id == QGEP.wastewater_structure.fk_provider,
        # ),
        # QGEP.wastewater_networkelement,

        # )

        # query2 via waste_water_treatment_plant Release 2015 where waste_water_treatment_plant is subclass of organisation
        query2 = (
            query.join(
                qgep_model.waste_water_treatment_plant,
                qgep_model.measuring_point.fk_waste_water_treatment_plant
                == qgep_model.waste_water_treatment_plant.obj_id,
            )
            .join(
                qgep_model.wastewater_structure,
                qgep_model.wastewater_structure.fk_owner
                == qgep_model.waste_water_treatment_plant.obj_id,
            )
            .join(qgep_model.wastewater_networkelement)
        )
        # only until VSA-DSS Release 2015
        query3 = (
            query.join(
                qgep_model.water_course_segment,
                qgep_model.measuring_point.fk_water_course_segment
                == qgep_model.water_course_segment.obj_id,
            )
            .join(
                qgep_model.river,
                # Fehler im Datenmodell fk_watercourse should be name fk_surface_water_bodies (resp. fk_surface_water_body - class should be renamed to single)
                qgep_model.water_course_segment.fk_watercourse == qgep_model.river.obj_id,
            )
            .join(
                qgep_model.sector_water_body,
                qgep_model.sector_water_body.fk_surface_water_bodies
                == qgep_model.sector_water_body.obj_id,
            )
            .join(
                qgep_model.discharge_point,
                qgep_model.discharge_point.fk_sector_water_body
                == qgep_model.sector_water_body.obj_id,
            )
            .join(qgep_model.wastewater_networkelement)
        )
        query = query.union(query1, query2, query3)
        # query = query.union(query1, query3)
        query = query.filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.measuring_point

        # --- measuring_point ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        if filtered:
            messstelle = abwasser_model.messstelle(
                # FIELDS TO MAP TO ABWASSER.messstelle
                # --- baseclass ---
                # --- sia405_baseclass ---
                **qgep_export_utils.base_common(row, "messstelle"),
                # --- messstelle ---
                # abwasserbauwerkref=qgep_export_utils.get_tid(row.fk_wastewater_structure__REL),
                abwasserbauwerkref=qgep_export_utils.check_fk_in_subsetid(
                    subset_wws_ids, row.fk_wastewater_structure__REL
                ),
                abwasserreinigungsanlageref=qgep_export_utils.get_tid(
                    row.fk_waste_water_treatment_plant__REL
                ),
                art=row.kind,
                bemerkung=qgep_export_utils.truncate(
                    qgep_export_utils.emptystr_to_null(row.remark), 80
                ),
                betreiberref=qgep_export_utils.get_tid(row.fk_operator__REL),
                bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
                gewaesserabschnittref=qgep_export_utils.get_tid(row.fk_water_course_segment__REL),
                lage=ST_Force2D(row.situation_geometry),
                # not supported in qgep datamodel yet, reference on same class
                # referenzstelleref=qgep_export_utils.get_tid(row.fk_reference_station__REL),
                staukoerper=qgep_export_utils.get_vl(row.damming_device__REL),
                zweck=qgep_export_utils.get_vl(row.purpose__REL),
            )
        else:
            messstelle = abwasser_model.messstelle(
                # FIELDS TO MAP TO ABWASSER.messstelle
                # --- baseclass ---
                # --- sia405_baseclass ---
                **qgep_export_utils.base_common(row, "messstelle"),
                # --- messstelle ---
                abwasserbauwerkref=qgep_export_utils.get_tid(row.fk_wastewater_structure__REL),
                abwasserreinigungsanlageref=qgep_export_utils.get_tid(
                    row.fk_waste_water_treatment_plant__REL
                ),
                art=row.kind,
                bemerkung=qgep_export_utils.truncate(
                    qgep_export_utils.emptystr_to_null(row.remark), 80
                ),
                betreiberref=qgep_export_utils.get_tid(row.fk_operator__REL),
                bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
                gewaesserabschnittref=qgep_export_utils.get_tid(row.fk_water_course_segment__REL),
                lage=ST_Force2D(row.situation_geometry),
                # not supported in qgep datamodel yet, reference on same class
                # referenzstelleref=qgep_export_utils.get_tid(row.fk_reference_station__REL),
                staukoerper=qgep_export_utils.get_vl(row.damming_device__REL),
                zweck=qgep_export_utils.get_vl(row.purpose__REL),
            )
        abwasser_session.add(messstelle)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.measuring_device -> ABWASSER.messgeraet, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.measuring_device)
    if filtered:
        # query = query.join(
        # QGEP.measuring_point, QGEP.wastewater_structure, QGEP.wastewater_networkelement
        # ).filter(QGEP.wastewater_networkelement.obj_id.in_(subset_ids))

        query1 = (
            query.join(
                qgep_model.measuring_point,
                qgep_model.measuring_device.fk_measuring_point
                == qgep_model.measuring_point.obj_id,
            )
            .join(
                qgep_model.wastewater_structure,
                qgep_model.measuring_point.fk_wastewater_structure
                == qgep_model.wastewater_structure.obj_id,
            )
            .join(qgep_model.wastewater_networkelement)
        )
        # query2 via waste_water_treatment_plant Release 2015 where waste_water_treatment_plant is subclass of organisation
        query2 = (
            query.join(
                qgep_model.measuring_point,
                qgep_model.measuring_device.fk_measuring_point
                == qgep_model.measuring_point.obj_id,
            )
            .join(
                qgep_model.waste_water_treatment_plant,
                qgep_model.measuring_point.fk_waste_water_treatment_plant
                == qgep_model.waste_water_treatment_plant.obj_id,
            )
            .join(
                qgep_model.wastewater_structure,
                qgep_model.wastewater_structure.fk_owner
                == qgep_model.waste_water_treatment_plant.obj_id,
            )
            .join(qgep_model.wastewater_networkelement)
        )
        # only until VSA-DSS Release 2015
        query3 = (
            query.join(
                qgep_model.measuring_point,
                qgep_model.measuring_device.fk_measuring_point
                == qgep_model.measuring_point.obj_id,
            )
            .join(
                qgep_model.water_course_segment,
                qgep_model.measuring_point.fk_water_course_segment
                == qgep_model.water_course_segment.obj_id,
            )
            .join(
                qgep_model.river,
                # Fehler im Datenmodell fk_watercourse should be name fk_surface_water_bodies (resp. fk_surface_water_body - class should be renamed to single)
                qgep_model.water_course_segment.fk_watercourse == qgep_model.river.obj_id,
            )
            .join(
                qgep_model.sector_water_body,
                qgep_model.sector_water_body.fk_surface_water_bodies
                == qgep_model.sector_water_body.obj_id,
            )
            .join(
                qgep_model.discharge_point,
                qgep_model.discharge_point.fk_sector_water_body
                == qgep_model.sector_water_body.obj_id,
            )
            .join(qgep_model.wastewater_networkelement)
        )
        query = query.union(query1, query2, query3)
        # query = query.union(query1, query3)
        query = query.filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")

    for row in query:

        # AVAILABLE FIELDS IN qgep_model.measuring_device

        # --- measuring_device ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        messgeraet = abwasser_model.messgeraet(
            # FIELDS TO MAP TO ABWASSER.messgeraet
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "messgeraet"),
            # --- messgeraet ---
            art=qgep_export_utils.get_vl(row.kind__REL),
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            fabrikat=row.brand,
            messstelleref=qgep_export_utils.get_tid(row.fk_measuring_point__REL),
            seriennummer=row.serial_number,
        )
        abwasser_session.add(messgeraet)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.measurement_series -> ABWASSER.messreihe, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.measurement_series)
    if filtered:

        # query = query.join(
        # qgep_model.measuring_point, qgep_model.wastewater_structure, qgep_model.wastewater_networkelement
        # ).filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        query1 = (
            query.join(
                qgep_model.measuring_point,
                qgep_model.measurement_series.fk_measuring_point
                == qgep_model.measuring_point.obj_id,
            )
            .join(
                qgep_model.wastewater_structure,
                qgep_model.measuring_point.fk_wastewater_structure
                == qgep_model.wastewater_structure.obj_id,
            )
            .join(qgep_model.wastewater_networkelement)
        )
        # query2 via waste_water_treatment_plant Release 2015 where waste_water_treatment_plant is subclass of organisation
        query2 = (
            query.join(
                qgep_model.measuring_point,
                qgep_model.measurement_series.fk_measuring_point
                == qgep_model.measuring_point.obj_id,
            )
            .join(
                qgep_model.waste_water_treatment_plant,
                qgep_model.measuring_point.fk_waste_water_treatment_plant
                == qgep_model.waste_water_treatment_plant.obj_id,
            )
            .join(
                qgep_model.wastewater_structure,
                qgep_model.wastewater_structure.fk_owner
                == qgep_model.waste_water_treatment_plant.obj_id,
            )
            .join(qgep_model.wastewater_networkelement)
        )
        # only until VSA-DSS Release 2015
        query3 = (
            query.join(
                qgep_model.measuring_point,
                qgep_model.measurement_series.fk_measuring_point
                == qgep_model.measuring_point.obj_id,
            )
            .join(
                qgep_model.water_course_segment,
                qgep_model.measuring_point.fk_water_course_segment
                == qgep_model.water_course_segment.obj_id,
            )
            .join(
                qgep_model.river,
                # Fehler im Datenmodell fk_watercourse should be name fk_surface_water_bodies (resp. fk_surface_water_body - class should be renamed to single)
                qgep_model.water_course_segment.fk_watercourse == qgep_model.river.obj_id,
            )
            .join(
                qgep_model.sector_water_body,
                qgep_model.sector_water_body.fk_surface_water_bodies
                == qgep_model.sector_water_body.obj_id,
            )
            .join(
                qgep_model.discharge_point,
                qgep_model.discharge_point.fk_sector_water_body
                == qgep_model.sector_water_body.obj_id,
            )
            .join(qgep_model.wastewater_networkelement)
        )
        query = query.union(query1, query2, query3)
        # query = query.union(query1, query3)
        query = query.filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.measurement_series

        # --- measurement_series ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        messreihe = abwasser_model.messreihe(
            # FIELDS TO MAP TO ABWASSER.messreihe
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "messreihe"),
            # --- messreihe ---
            # not supported in qgep - will be introduced with VSA-DSS 2020
            # abwassernetzelementref=qgep_export_utils.get_tid(row.fk_wastewater_networkelement__REL),
            art=qgep_export_utils.get_vl(row.kind__REL),
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            dimension=row.dimension,
            messstelleref=qgep_export_utils.get_tid(row.fk_measuring_point__REL),
        )
        abwasser_session.add(messreihe)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.measurement_result -> ABWASSER.messresultat, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.measurement_result)
    if filtered:

        # query = query.join(
        # QGEP.measurement_series,
        # QGEP.measuring_point,
        # QGEP.wastewater_structure,
        # QGEP.wastewater_networkelement,
        # ).filter(QGEP.wastewater_networkelement.obj_id.in_(subset_ids))
        query1 = (
            query.join(
                qgep_model.measurement_series,
                qgep_model.measurement_result.fk_measurement_series
                == qgep_model.measurement_series.obj_id,
            )
            .join(
                qgep_model.measuring_point,
                qgep_model.measurement_series.fk_measuring_point
                == qgep_model.measuring_point.obj_id,
            )
            .join(
                qgep_model.wastewater_structure,
                qgep_model.measuring_point.fk_wastewater_structure
                == qgep_model.wastewater_structure.obj_id,
            )
            .join(qgep_model.wastewater_networkelement)
        )
        # query2 via waste_water_treatment_plant Release 2015 where waste_water_treatment_plant is subclass of organisation
        query2 = (
            query.join(
                qgep_model.measurement_series,
                qgep_model.measurement_result.fk_measurement_series
                == qgep_model.measurement_series.obj_id,
            )
            .join(
                qgep_model.measuring_point,
                qgep_model.measurement_series.fk_measuring_point
                == qgep_model.measuring_point.obj_id,
            )
            .join(
                qgep_model.waste_water_treatment_plant,
                qgep_model.measuring_point.fk_waste_water_treatment_plant
                == qgep_model.waste_water_treatment_plant.obj_id,
            )
            .join(
                qgep_model.wastewater_structure,
                qgep_model.wastewater_structure.fk_owner
                == qgep_model.waste_water_treatment_plant.obj_id,
            )
            .join(qgep_model.wastewater_networkelement)
        )
        # only until VSA-DSS Release 2015
        query3 = (
            query.join(
                qgep_model.measurement_series,
                qgep_model.measurement_result.fk_measurement_series
                == qgep_model.measurement_series.obj_id,
            )
            .join(
                qgep_model.measuring_point,
                qgep_model.measurement_series.fk_measuring_point
                == qgep_model.measuring_point.obj_id,
            )
            .join(
                qgep_model.water_course_segment,
                qgep_model.measuring_point.fk_water_course_segment
                == qgep_model.water_course_segment.obj_id,
            )
            .join(
                qgep_model.river,
                # Fehler im Datenmodell fk_watercourse should be name fk_surface_water_bodies (resp. fk_surface_water_body - class should be renamed to single)
                qgep_model.water_course_segment.fk_watercourse == qgep_model.river.obj_id,
            )
            .join(
                qgep_model.sector_water_body,
                qgep_model.sector_water_body.fk_surface_water_bodies
                == qgep_model.sector_water_body.obj_id,
            )
            .join(
                qgep_model.discharge_point,
                qgep_model.discharge_point.fk_sector_water_body
                == qgep_model.sector_water_body.obj_id,
            )
            .join(qgep_model.wastewater_networkelement)
        )
        # query4 not implemented via measuring_device
        query = query.union(query1, query2, query3)
        # query = query.union(query1, query3)
        query = query.filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")

    for row in query:

        # AVAILABLE FIELDS IN QGEP.measurement_result

        # --- measurement_result ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        messresultat = abwasser_model.messresultat(
            # FIELDS TO MAP TO ABWASSER.messresultat
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "messresultat"),
            # --- messresultat ---
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            messart=qgep_export_utils.get_vl(row.measurement_type__REL),
            messdauer=row.measuring_duration,
            messgeraetref=qgep_export_utils.get_tid(row.fk_measuring_device__REL),
            messreiheref=qgep_export_utils.get_tid(row.fk_measurement_series__REL),
            wert=row.value,
            zeit=row.time,
        )
        abwasser_session.add(messresultat)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.throttle_shut_off_unit -> ABWASSER.absperr_drosselorgan, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.throttle_shut_off_unit)
    # sqlalchemy.exc.InvalidRequestError: Don't know how to join to . Please use the .select_from() method to establish an explicit left side, as well as providing an explcit ON clause if not present already to help resolve the ambiguity.
    # fk_control_center has also to be NOT considered
    if filtered:
        query = query.join(
            qgep_model.wastewater_node,
            qgep_model.wastewater_node.obj_id
            == qgep_model.throttle_shut_off_unit.fk_wastewater_node,
        ).filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.throttle_shut_off_unit

        # --- throttle_shut_off_unit ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        absperr_drosselorgan = abwasser_model.absperr_drosselorgan(
            # FIELDS TO MAP TO ABWASSER.absperr_drosselorgan
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "absperr_drosselorgan"),
            # --- absperr_drosselorgan ---
            abwasserknotenref=qgep_export_utils.get_tid(row.fk_wastewater_node__REL),
            antrieb=qgep_export_utils.get_vl(row.actuation__REL),
            art=qgep_export_utils.get_vl(row.kind__REL),
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            bruttokosten=row.gross_costs,
            drosselorgan_oeffnung_ist=row.throttle_unit_opening_current,
            drosselorgan_oeffnung_ist_optimiert=row.throttle_unit_opening_current_optimized,
            fabrikat=row.manufacturer,
            querschnitt=row.cross_section,
            signaluebermittlung=qgep_export_utils.get_vl(row.signal_transmission__REL),
            steuerung=qgep_export_utils.get_vl(row.control__REL),
            steuerungszentraleref=qgep_export_utils.get_tid(row.fk_control_center__REL),
            subventionen=row.subsidies,
            ueberlaufref=qgep_export_utils.get_tid(row.fk_overflow__REL),
            verstellbarkeit=qgep_export_utils.get_vl(row.adjustability__REL),
            wirksamer_qs=row.effective_cross_section,
        )
        abwasser_session.add(absperr_drosselorgan)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.prank_weir -> ABWASSER.streichwehr, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.prank_weir)
    # to check if fk_overflow_char also has to be considered
    if filtered:
        query = query.join(
            qgep_model.wastewater_node,
            or_(
                qgep_model.wastewater_node.obj_id == qgep_model.prank_weir.fk_wastewater_node,
                qgep_model.wastewater_node.obj_id == qgep_model.prank_weir.fk_overflow_to,
            ),
        ).filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")

    for row in query:
        # AVAILABLE FIELDS IN QGEP.prank_weir

        # --- overflow ---
        # to do attributeslist of superclass
        # --- prank_weir ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        streichwehr = abwasser_model.streichwehr(
            # FIELDS TO MAP TO ABWASSER.streichwehr
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "streichwehr"),
            # --- ueberlauf ---
            **overflow_common(row),
            # --- streichwehr ---
            hydrueberfalllaenge=row.hydraulic_overflow_length,
            kotemax=row.level_max,
            kotemin=row.level_min,
            ueberfallkante=qgep_export_utils.get_vl(row.weir_edge__REL),
            wehr_art=qgep_export_utils.get_vl(row.weir_kind__REL),
        )
        abwasser_session.add(streichwehr)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.pump -> ABWASSER.foerderaggregat, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.pump)
    # to check if fk_overflow_char also has to be considered
    if filtered:
        query = query.join(
            qgep_model.wastewater_node,
            or_(
                qgep_model.wastewater_node.obj_id == qgep_model.prank_weir.fk_wastewater_node,
                qgep_model.wastewater_node.obj_id == qgep_model.prank_weir.fk_overflow_to,
            ),
        ).filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.pump

        # --- overflow ---
        # to do attributeslist of superclass
        # --- pump ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        foerderaggregat = abwasser_model.foerderaggregat(
            # FIELDS TO MAP TO ABWASSER.foerderaggregat
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "foerderaggregat"),
            # --- ueberlauf ---
            **overflow_common(row),
            # --- foerderaggregat ---
            arbeitspunkt=row.operating_point,
            aufstellungantrieb=qgep_export_utils.get_vl(row.placement_of_actuation__REL),
            aufstellungfoerderaggregat=qgep_export_utils.get_vl(row.placement_of_pump__REL),
            bauart=qgep_export_utils.get_vl(row.construction_type__REL),
            foerderstrommax_einzel=row.pump_flow_max_single,
            foerderstrommin_einzel=row.pump_flow_min_single,
            kotestart=row.start_level,
            kotestop=row.stop_level,
            nutzungsart_ist=qgep_export_utils.get_vl(row.usage_current__REL),
        )
        abwasser_session.add(foerderaggregat)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.leapingweir -> ABWASSER.leapingwehr, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.leapingweir)
    # to check if fk_overflow_char also has to be considered
    if filtered:
        query = query.join(
            qgep_model.wastewater_node,
            or_(
                qgep_model.wastewater_node.obj_id == qgep_model.prank_weir.fk_wastewater_node,
                qgep_model.wastewater_node.obj_id == qgep_model.prank_weir.fk_overflow_to,
            ),
        ).filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.leapingweir

        # --- overflow ---
        # to do attributeslist of superclass
        # --- leapingweir ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        leapingwehr = abwasser_model.leapingwehr(
            # FIELDS TO MAP TO ABWASSER.leapingwehr
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "leapingwehr"),
            # --- ueberlauf ---
            **overflow_common(row),
            # --- leapingwehr ---
            breite=row.width,
            laenge=row.length,
            oeffnungsform=qgep_export_utils.get_vl(row.opening_shape__REL),
        )
        abwasser_session.add(leapingwehr)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.hydraulic_char_data -> ABWASSER.hydr_kennwerte, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.hydraulic_char_data)
    # side fk_overflow_char not considered in filter query
    if filtered:
        query = query.join(
            qgep_model.wastewater_node,
            or_(
                qgep_model.wastewater_node.obj_id
                == qgep_model.hydraulic_char_data.fk_wastewater_node,
                # fk_primary_direction only added with VSA-DSS 2020
            ),
        ).filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.hydraulic_char_data

        # --- hydraulic_char_data ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        hydr_kennwerte = abwasser_model.hydr_kennwerte(
            # FIELDS TO MAP TO ABWASSER.hydr_kennwerte
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "hydr_kennwerte"),
            # --- hydr_kennwerte ---
            abwasserknotenref=qgep_export_utils.get_tid(row.fk_wastewater_node__REL),
            aggregatezahl=row.aggregate_number,
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            foerderhoehe_geodaetisch=row.delivery_height_geodaetic,
            foerderstrommax=row.pump_flow_max,
            foerderstrommin=row.pump_flow_min,
            hauptwehrart=qgep_export_utils.get_vl(row.main_weir_kind__REL),
            mehrbelastung=row.overcharge,
            # primaerrichtungref will be added with release 2020
            # primaerrichtungref=qgep_export_utils.get_tid(row.fk_primary_direction__REL),
            pumpenregime=qgep_export_utils.get_vl(row.pump_characteristics__REL),
            qab=row.q_discharge,
            qan=row.qon,
            springt_an=qgep_export_utils.get_vl(row.is_overflowing__REL),
            astatus=qgep_export_utils.get_vl(row.status__REL),
            ueberlaufcharakteristikref=qgep_export_utils.get_tid(row.fk_overflow_char__REL),
            ueberlaufdauer=row.overflow_duration,
            ueberlauffracht=row.overflow_freight,
            ueberlaufhaeufigkeit=row.overflow_frequency,
            ueberlaufmenge=row.overflow_volume,
        )
        abwasser_session.add(hydr_kennwerte)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.backflow_prevention -> ABWASSER.rueckstausicherung, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.backflow_prevention)
    # side fk_throttle_shut_off_unit and fk_overflow not considered in filter query - they are usually added only for log_cards and then the corresponding nodes exist anyway thru the direct relation.
    if filtered:
        # query = (
        # query.join(
        # qgep_model.wastewater_structure,
        # qgep_model.structure_part.fk_wastewater_structure
        # == qgep_model.wastewater_structure.obj_id,
        # )
        # .join(qgep_model.wastewater_networkelement)
        # .filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        # )
        query = query.join(
            qgep_model.wastewater_structure,
            qgep_model.structure_part.fk_wastewater_structure
            == qgep_model.wastewater_structure.obj_id,
        ).filter(qgep_model.wastewater_structure.obj_id.in_(subset_wws_ids))
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.backflow_prevention

        # --- structure_part ---
        # to do attributeslist of superclass
        # --- backflow_prevention ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        rueckstausicherung = abwasser_model.rueckstausicherung(
            # FIELDS TO MAP TO ABWASSER.rueckstausicherung
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "rueckstausicherung"),
            # --- bauwerksteil ---
            **qgep_export_utils.structure_part_common(row),
            # --- rueckstausicherung ---
            absperr_drosselorganref=qgep_export_utils.get_tid(row.fk_throttle_shut_off_unit__REL),
            art=qgep_export_utils.get_vl(row.kind__REL),
            bruttokosten=row.gross_costs,
            ersatzjahr=row.year_of_replacement,
            foerderaggregatref=qgep_export_utils.get_tid(row.fk_pump__REL),
        )
        abwasser_session.add(rueckstausicherung)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.solids_retention -> ABWASSER.feststoffrueckhalt, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.solids_retention)
    if filtered:
        # query = (
        # query.join(
        # qgep_model.wastewater_structure,
        # qgep_model.structure_part.fk_wastewater_structure
        # == qgep_model.wastewater_structure.obj_id,
        # )
        # .join(qgep_model.wastewater_networkelement)
        # .filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        # )
        query = query.join(
            qgep_model.wastewater_structure,
            qgep_model.structure_part.fk_wastewater_structure
            == qgep_model.wastewater_structure.obj_id,
        ).filter(qgep_model.wastewater_structure.obj_id.in_(subset_wws_ids))
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.solids_retention

        # --- structure_part ---
        # to do attributeslist of superclass
        # --- solids_retention ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        feststoffrueckhalt = abwasser_model.feststoffrueckhalt(
            # FIELDS TO MAP TO ABWASSER.feststoffrueckhalt
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "feststoffrueckhalt"),
            # --- bauwerksteil ---
            **qgep_export_utils.structure_part_common(row),
            # --- feststoffrueckhalt ---
            anspringkote=row.overflow_level,
            art=qgep_export_utils.get_vl(row.type__REL),
            bruttokosten=row.gross_costs,
            dimensionierungswert=row.dimensioning_value,
            ersatzjahr=row.year_of_replacement,
        )
        abwasser_session.add(feststoffrueckhalt)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.tank_cleaning -> ABWASSER.beckenreinigung, ABWASSER.metaattribute")
    query = qgep_session.query(qgep_model.tank_cleaning)
    if filtered:
        # query = (
        # query.join(
        # qgep_model.wastewater_structure,
        # qgep_model.structure_part.fk_wastewater_structure
        # == qgep_model.wastewater_structure.obj_id,
        # )
        # .join(qgep_model.wastewater_networkelement)
        # .filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        # )
        query = query.join(
            qgep_model.wastewater_structure,
            qgep_model.structure_part.fk_wastewater_structure
            == qgep_model.wastewater_structure.obj_id,
        ).filter(qgep_model.wastewater_structure.obj_id.in_(subset_wws_ids))
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.tank_cleaning

        # --- structure_part ---
        # to do attributeslist of superclass
        # --- tank_cleaning ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        beckenreinigung = abwasser_model.beckenreinigung(
            # FIELDS TO MAP TO ABWASSER.beckenreinigung
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "beckenreinigung"),
            # --- bauwerksteil ---
            **qgep_export_utils.structure_part_common(row),
            # --- beckenreinigung ---
            art=qgep_export_utils.get_vl(row.type__REL),
            bruttokosten=row.gross_costs,
            ersatzjahr=row.year_of_replacement,
        )
        abwasser_session.add(beckenreinigung)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.tank_emptying -> ABWASSER.beckenentleerung, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.tank_emptying)
    # side fk_throttle_shut_off_unit and fk_overflow not considered in filter query - they are usually added only for log_cards and then the corresponding nodes exist anyway thru the direct relation.
    if filtered:
        # query = (
        # query.join(
        # qgep_model.wastewater_structure,
        # qgep_model.structure_part.fk_wastewater_structure
        # == qgep_model.wastewater_structure.obj_id,
        # )
        # .join(qgep_model.wastewater_networkelement)
        # .filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        # )
        query = query.join(
            qgep_model.wastewater_structure,
            qgep_model.structure_part.fk_wastewater_structure
            == qgep_model.wastewater_structure.obj_id,
        ).filter(qgep_model.wastewater_structure.obj_id.in_(subset_wws_ids))
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.tank_emptying

        # --- structure_part ---
        # to do attributeslist of superclass
        # --- tank_emptying ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        beckenentleerung = abwasser_model.beckenentleerung(
            # FIELDS TO MAP TO ABWASSER.beckenentleerung
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "beckenentleerung"),
            # --- bauwerksteil ---
            **qgep_export_utils.structure_part_common(row),
            # --- beckenentleerung ---
            absperr_drosselorganref=qgep_export_utils.get_tid(row.fk_throttle_shut_off_unit__REL),
            art=qgep_export_utils.get_vl(row.type__REL),
            bruttokosten=row.gross_costs,
            ersatzjahr=row.year_of_replacement,
            leistung=row.flow,
            ueberlaufref=qgep_export_utils.get_tid(row.fk_overflow__REL),
        )
        abwasser_session.add(beckenentleerung)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.param_ca_general -> ABWASSER.ezg_parameter_allg, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.param_ca_general)
    if filtered:
        query = query.join(qgep_model.catchment_area).filter(
            qgep_model.wastewater_networkelement.obj_id.in_(subset_ids)
        )
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.param_ca_general

        # --- surface_runoff_parameters ---
        # to do attributeslist of superclass
        # --- param_ca_general ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        ezg_parameter_allg = abwasser_model.ezg_parameter_allg(
            # FIELDS TO MAP TO ABWASSER.ezg_parameter_allg
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "ezg_parameter_allg"),
            # --- oberflaechenabflussparameter ---
            **surface_runoff_parameters_common(row),
            # --- ezg_parameter_allg ---
            einwohnergleichwert=row.population_equivalent,
            flaeche=row.surface_ca,
            fliessweggefaelle=row.flow_path_slope,
            fliessweglaenge=row.flow_path_length,
            trockenwetteranfall=row.dry_wheather_flow,
        )
        abwasser_session.add(ezg_parameter_allg)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.param_ca_mouse1 -> ABWASSER.ezg_parameter_mouse1, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.param_ca_mouse1)
    if filtered:
        query = query.join(qgep_model.catchment_area).filter(
            qgep_model.wastewater_networkelement.obj_id.in_(subset_ids)
        )
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:
        # AVAILABLE FIELDS IN QGEP.param_ca_mouse1

        # --- surface_runoff_parameters ---
        # to do attributeslist of superclass
        # --- param_ca_mouse1 ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        ezg_parameter_mouse1 = abwasser_model.ezg_parameter_mouse1(
            # FIELDS TO MAP TO ABWASSER.ezg_parameter_mouse1
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "ezg_parameter_mouse1"),
            # --- oberflaechenabflussparameter ---
            **surface_runoff_parameters_common(row),
            # --- ezg_parameter_mouse1 ---
            einwohnergleichwert=row.population_equivalent,
            flaeche=row.surface_ca_mouse,
            fliessweggefaelle=row.flow_path_slope,
            fliessweglaenge=row.flow_path_length,
            nutzungsart=row.usage,
            trockenwetteranfall=row.dry_wheather_flow,
        )
        abwasser_session.add(ezg_parameter_mouse1)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    # class maintenance_event as class, is not superclass in VSA-DSS 2015
    logger.info(
        "Exporting QGEP.maintenance_event -> ABWASSER.maintenance_event, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.maintenance_event)
    # explicit join for n:m re_maintenance_event_wastewater_structure
    if filtered:
        query = (
            query.join(
                qgep_model.re_maintenance_event_wastewater_structure,
                qgep_model.re_maintenance_event_wastewater_structure.fk_maintenance_event
                == qgep_model.maintenance_event.obj_id,
            )
            .join(
                qgep_model.wastewater_structure,
                qgep_model.re_maintenance_event_wastewater_structure.fk_wastewater_structure
                == qgep_model.wastewater_structure.obj_id,
            )
            .join(qgep_model.wastewater_networkelement)
            .filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        )
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.maintenance_event

        # --- maintenance_event ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        erhaltungsereignis = abwasser_model.erhaltungsereignis(
            # FIELDS TO MAP TO ABWASSER.erhaltungsereignis
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "erhaltungsereignis"),
            # --- erhaltungsereignis ---
            # abwasserbauwerkref=row.REPLACE_ME,  # TODO : convert this to M2N relation through re_maintenance_event_wastewater_structure
            art=qgep_export_utils.get_vl(row.kind__REL),
            astatus=qgep_export_utils.get_vl(row.status__REL),
            ausfuehrende_firmaref=qgep_export_utils.get_tid(row.fk_operating_company__REL),
            ausfuehrender=row.operator,
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            # model difference qgep and vsa-dss 2015
            # bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            bezeichnung=qgep_export_utils.truncate(
                qgep_export_utils.null_to_emptystr(row.identifier), 20
            ),
            # model difference qgep (unlimited text) and vsa-dss 2015 / 2020 TEXT*50
            # datengrundlage=row.base_data,
            datengrundlage=qgep_export_utils.truncate(row.base_data, 50),
            dauer=row.duration,
            detaildaten=row.data_details,
            # model difference qgep TEXT*255 and vsa-dss 2015 TEXT*50, no qgep_export_utils.truncate needed anymore for 2020
            # ergebnis=row.result,
            ergebnis=qgep_export_utils.truncate(row.result, 50),
            grund=row.reason,
            kosten=row.cost,
            # will be added in VSA-DSS 2020
            # massnahmeref: qgep_export_utils.get_tid(row.fk_measure__REL),
            zeitpunkt=row.time_point,
        )
        abwasser_session.add(erhaltungsereignis)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    # Labels
    # Note: these are extracted from the optional labels file (not exported from the QGEP database)
    if labels_file:
        logger.info(f"Exporting label positions from {labels_file}")

        # Get t_id by obj_name to create the reference on the labels below
        tid_for_obj_id = {
            "haltung": {},
            "abwasserbauwerk": {},
            "einzugsgebiet": {},
        }
        for row in abwasser_session.query(abwasser_model.haltung):
            tid_for_obj_id["haltung"][row.obj_id] = row.t_id
        for row in abwasser_session.query(abwasser_model.abwasserbauwerk):
            tid_for_obj_id["abwasserbauwerk"][row.obj_id] = row.t_id
        for row in abwasser_session.query(abwasser_model.einzugsgebiet):
            tid_for_obj_id["einzugsgebiet"][row.obj_id] = row.t_id

        with open(labels_file) as labels_file_handle:
            labels = json.load(labels_file_handle)

        try:
            geojson_crs_def = labels["crs"]
        except Exception:
            logger.warning("No labels available - no labels will be exported")
        else:
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
                            f"Label for haltung (reach) `{obj_id}` exists, but that object is not part of the export"
                        )
                        continue
                    ili_label = abwasser_model.haltung_text(
                        **qgep_export_utils.textpos_common(
                            label, "haltung_text (reach_text)", geojson_crs_def
                        ),
                        haltungref=tid_for_obj_id["haltung"][obj_id],
                    )

                elif layer_name == "vw_qgep_wastewater_structure":
                    if obj_id not in tid_for_obj_id["abwasserbauwerk"]:
                        logger.warning(
                            f"Label for abwasserbauwerk (wastewater_structure) `{obj_id}` exists, but that object is not part of the export"
                        )
                        continue
                    ili_label = abwasser_model.abwasserbauwerk_text(
                        **qgep_export_utils.textpos_common(
                            label, "abwasserbauwerk_text", geojson_crs_def
                        ),
                        abwasserbauwerkref=tid_for_obj_id["abwasserbauwerk"][obj_id],
                    )

                elif layer_name == "catchment_area":
                    if obj_id not in tid_for_obj_id["einzugsgebiet"]:
                        logger.warning(
                            f"Label for einzugsgebiet (catchment_area) `{obj_id}` exists, but that object is not part of the export"
                        )
                        continue
                    ili_label = abwasser_model.einzugsgebiet_text(
                        **qgep_export_utils.textpos_common(
                            label, "einzugsgebiet_text (catchment_area_text)", geojson_crs_def
                        ),
                        einzugsgebietref=tid_for_obj_id["einzugsgebiet"][obj_id],
                    )

                else:
                    logger.warning(
                        f"Unknown layer for label `{layer_name}`. Label will be ignored",
                    )
                    continue

                abwasser_session.add(ili_label)

            print(".", end="")

        finally:
            logger.info("done")
            abwasser_session.flush()

    # -- extra commit
    abwasser_session.commit()

    # -- extra session2 for re_maintenance_event_wastewater_structure
    abwasser_session2 = Session(
        utils.sqlalchemy.create_engine(), autocommit=False, autoflush=False
    )

    logger.info(
        "Exporting QGEP.re_maintenance_event_wastewater_structure -> ABWASSER.erhaltungsereignis_abwasserbauwerkassoc"
    )
    query = qgep_session.query(qgep_model.re_maintenance_event_wastewater_structure)
    if filtered:
        query = (
            query.join(
                qgep_model.maintenance_event,
                qgep_model.re_maintenance_event_wastewater_structure.fk_maintenance_event
                == qgep_model.maintenance_event.obj_id,
            )
            .join(
                qgep_model.wastewater_structure,
                qgep_model.re_maintenance_event_wastewater_structure.fk_wastewater_structure
                == qgep_model.wastewater_structure.obj_id,
            )
            .join(
                qgep_model.wastewater_networkelement,
            )
            .filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
        )
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
    for row in query:

        # AVAILABLE FIELDS IN QGEP.maintenance_event_wastewater_structure

        # --- maintenance_event_wastewater_structure ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL

        erhaltungsereignis_abwasserbauwerk = abwasser_model.erhaltungsereignis_abwasserbauwerkassoc(
            # FIELDS TO MAP TO ABWASSER.erhaltungsereignis_abwasserbauwerk
            # --- baseclass ---
            # --- sia405_baseclass ---
            # --- erhaltungsereignis_abwasserbauwerk ---
            abwasserbauwerkref=qgep_export_utils.get_tid(row.fk_wastewater_structure__REL),
            erhaltungsereignis_abwasserbauwerkassocref=qgep_export_utils.get_tid(
                row.fk_maintenance_event__REL
            ),
        )

        abwasser_session2.add(erhaltungsereignis_abwasserbauwerk)

        print(".", end="")
    logger.info("done")

    abwasser_session2.flush()

    abwasser_session2.commit()

    qgep_session.close()
    abwasser_session.close()
