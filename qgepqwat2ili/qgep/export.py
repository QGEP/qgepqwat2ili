import json

from geoalchemy2.functions import ST_Force2D
from sqlalchemy import or_
from sqlalchemy.orm import Session
from sqlalchemy.sql import text

from .. import utils
from ..utils.basket_utils import BasketUtils
from ..utils.ili2db import skip_wwtp_structure_ids
from ..utils.qgep_export_utils import QgepExportUtils
from ..utils.various import logger
from .model_abwasser import get_abwasser_model
from .model_qgep import get_qgep_model


def qgep_export(selection=None, labels_file=None, orientation=None, basket_enabled=False):
    """
    Export data from the QGEP model into the ili2pg model.

    Args:
        selection:      if provided, limits the export to networkelements that are provided in the selection
    """

    QGEP = get_qgep_model()
    ABWASSER = get_abwasser_model()

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
        basket_utils = BasketUtils(ABWASSER, abwasser_session)
        basket_utils.create_basket()

        current_basket = basket_utils.basket_topic_sia405_abwasser

    # Filtering
    filtered = selection is not None
    subset_ids = selection if selection is not None else []

    # get list of id's of class wwtp_structure (ARABauwerk) to be able to check if fk_wastewater_structure references to wwtp_structure

    wastewater_structure_id_sia405abwasser_list = None
    wastewater_structure_id_sia405abwasser_list = skip_wwtp_structure_ids()

    logger.info(
        f"wastewater_structure_id_sia405abwasser_list : {wastewater_structure_id_sia405abwasser_list}",
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
        abwasser_model=ABWASSER,
        labelorientation=labelorientation,
    )

    # ADAPTED FROM 052a_sia405_abwasser_2015_2_d_interlisexport2.sql
    logger.info("Exporting QGEP.organisation -> ABWASSER.organisation, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.organisation)
    for row in query:

        # AVAILABLE FIELDS IN QGEP.organisation

        # --- organisation ---
        # fk_dataowner, fk_provider, identifier, last_modification, obj_id, remark, uid

        # --- _bwrel_ ---
        # accident__BWREL_fk_dataowner, accident__BWREL_fk_provider, administrative_office__BWREL_obj_id, aquifier__BWREL_fk_dataowner, aquifier__BWREL_fk_provider, bathing_area__BWREL_fk_dataowner, bathing_area__BWREL_fk_provider, canton__BWREL_obj_id, catchment_area__BWREL_fk_dataowner, catchment_area__BWREL_fk_provider, connection_object__BWREL_fk_dataowner, connection_object__BWREL_fk_operator, connection_object__BWREL_fk_owner, connection_object__BWREL_fk_provider, control_center__BWREL_fk_dataowner, control_center__BWREL_fk_provider, cooperative__BWREL_obj_id, damage__BWREL_fk_dataowner, damage__BWREL_fk_provider, data_media__BWREL_fk_dataowner, data_media__BWREL_fk_provider, file__BWREL_fk_dataowner, file__BWREL_fk_provider, fish_pass__BWREL_fk_dataowner, fish_pass__BWREL_fk_provider, hazard_source__BWREL_fk_dataowner, hazard_source__BWREL_fk_owner, hazard_source__BWREL_fk_provider, hq_relation__BWREL_fk_dataowner, hq_relation__BWREL_fk_provider, hydr_geom_relation__BWREL_fk_dataowner, hydr_geom_relation__BWREL_fk_provider, hydr_geometry__BWREL_fk_dataowner, hydr_geometry__BWREL_fk_provider, hydraulic_char_data__BWREL_fk_dataowner, hydraulic_char_data__BWREL_fk_provider, maintenance_event__BWREL_fk_dataowner, maintenance_event__BWREL_fk_operating_company, maintenance_event__BWREL_fk_provider, measurement_result__BWREL_fk_dataowner, measurement_result__BWREL_fk_provider, measurement_series__BWREL_fk_dataowner, measurement_series__BWREL_fk_provider, measuring_device__BWREL_fk_dataowner, measuring_device__BWREL_fk_provider, measuring_point__BWREL_fk_dataowner, measuring_point__BWREL_fk_operator, measuring_point__BWREL_fk_provider, mechanical_pretreatment__BWREL_fk_dataowner, mechanical_pretreatment__BWREL_fk_provider, municipality__BWREL_obj_id, mutation__BWREL_fk_dataowner, mutation__BWREL_fk_provider, organisation__BWREL_fk_dataowner, organisation__BWREL_fk_provider, overflow__BWREL_fk_dataowner, overflow__BWREL_fk_provider, overflow_char__BWREL_fk_dataowner, overflow_char__BWREL_fk_provider, pipe_profile__BWREL_fk_dataowner, pipe_profile__BWREL_fk_provider, private__BWREL_obj_id, profile_geometry__BWREL_fk_dataowner, profile_geometry__BWREL_fk_provider, reach_point__BWREL_fk_dataowner, reach_point__BWREL_fk_provider, retention_body__BWREL_fk_dataowner, retention_body__BWREL_fk_provider, river_bank__BWREL_fk_dataowner, river_bank__BWREL_fk_provider, river_bed__BWREL_fk_dataowner, river_bed__BWREL_fk_provider, sector_water_body__BWREL_fk_dataowner, sector_water_body__BWREL_fk_provider, sludge_treatment__BWREL_fk_dataowner, sludge_treatment__BWREL_fk_provider, structure_part__BWREL_fk_dataowner, structure_part__BWREL_fk_provider, substance__BWREL_fk_dataowner, substance__BWREL_fk_provider, surface_runoff_parameters__BWREL_fk_dataowner, surface_runoff_parameters__BWREL_fk_provider, surface_water_bodies__BWREL_fk_dataowner, surface_water_bodies__BWREL_fk_provider, throttle_shut_off_unit__BWREL_fk_dataowner, throttle_shut_off_unit__BWREL_fk_provider, txt_symbol__BWREL_fk_dataowner, txt_symbol__BWREL_fk_provider, waste_water_association__BWREL_obj_id, waste_water_treatment__BWREL_fk_dataowner, waste_water_treatment__BWREL_fk_provider, waste_water_treatment_plant__BWREL_obj_id, wastewater_networkelement__BWREL_fk_dataowner, wastewater_networkelement__BWREL_fk_provider, wastewater_structure__BWREL_fk_dataowner, wastewater_structure__BWREL_fk_operator, wastewater_structure__BWREL_fk_owner, wastewater_structure__BWREL_fk_provider, wastewater_structure_symbol__BWREL_fk_dataowner, wastewater_structure_symbol__BWREL_fk_provider, water_catchment__BWREL_fk_dataowner, water_catchment__BWREL_fk_provider, water_control_structure__BWREL_fk_dataowner, water_control_structure__BWREL_fk_provider, water_course_segment__BWREL_fk_dataowner, water_course_segment__BWREL_fk_provider, wwtp_energy_use__BWREL_fk_dataowner, wwtp_energy_use__BWREL_fk_provider, zone__BWREL_fk_dataowner, zone__BWREL_fk_provider

        # --- _rel_ ---
        # fk_dataowner__REL, fk_provider__REL

        organisation = ABWASSER.organisation(
            # FIELDS TO MAP TO ABWASSER.organisation
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "organisation"),
            # --- organisation ---
            auid=row.uid,
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
        )
        abwasser_session.add(organisation)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.channel -> ABWASSER.kanal, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.channel)
    if filtered:
        query = query.join(QGEP.wastewater_networkelement).filter(
            QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        )
    for row in query:
        # AVAILABLE FIELDS IN QGEP.channel

        # --- wastewater_structure ---
        # _bottom_label, _cover_label, _depth, _function_hierarchic, _input_label, _label, _output_label, _usage_current, accessibility, contract_section, detail_geometry_geometry, financing, fk_dataowner, fk_main_cover, fk_main_wastewater_node, fk_operator, fk_owner, fk_provider, gross_costs, identifier, inspection_interval, last_modification, location_name, records, remark, renovation_necessity, replacement_value, rv_base_year, rv_construction_type, status, structure_condition, subsidies, year_of_construction, year_of_replacement

        # --- _bwrel_ ---
        # measuring_point__BWREL_fk_wastewater_structure, mechanical_pretreatment__BWREL_fk_wastewater_structure, re_maintenance_event_wastewater_structure__BWREL_fk_wastewater_structure, structure_part__BWREL_fk_wastewater_structure, txt_symbol__BWREL_fk_wastewater_structure, txt_text__BWREL_fk_wastewater_structure, wastewater_networkelement__BWREL_fk_wastewater_structure, wastewater_structure_symbol__BWREL_fk_wastewater_structure, wastewater_structure_text__BWREL_fk_wastewater_structure, wwtp_structure_kind__BWREL_obj_id

        # --- _rel_ ---
        # accessibility__REL, bedding_encasement__REL, connection_type__REL, financing__REL, fk_dataowner__REL, fk_main_cover__REL, fk_main_wastewater_node__REL, fk_operator__REL, fk_owner__REL, fk_provider__REL, function_hierarchic__REL, function_hydraulic__REL, renovation_necessity__REL, rv_construction_type__REL, status__REL, structure_condition__REL, usage_current__REL, usage_planned__REL

        kanal = ABWASSER.kanal(
            # FIELDS TO MAP TO ABWASSER.kanal
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "kanal"),
            # --- abwasserbauwerk ---
            **qgep_export_utils.wastewater_structure_common(row),
            # --- kanal ---
            bettung_umhuellung=qgep_export_utils.get_vl(row.bedding_encasement__REL),
            funktionhierarchisch=qgep_export_utils.get_vl(row.function_hierarchic__REL),
            funktionhydraulisch=qgep_export_utils.get_vl(row.function_hydraulic__REL),
            nutzungsart_geplant=qgep_export_utils.get_vl(row.usage_planned__REL),
            nutzungsart_ist=qgep_export_utils.get_vl(row.usage_current__REL),
            rohrlaenge=row.pipe_length,
            spuelintervall=row.jetting_interval,
            verbindungsart=qgep_export_utils.get_vl(row.connection_type__REL),
        )
        abwasser_session.add(kanal)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.manhole -> ABWASSER.normschacht, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.manhole)
    if filtered:
        query = query.join(QGEP.wastewater_networkelement).filter(
            QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        )
    for row in query:
        # AVAILABLE FIELDS IN QGEP.manhole

        # --- wastewater_structure ---
        # to do attributeslist of superclass
        # --- manhole ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden

        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,

        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,

        normschacht = ABWASSER.normschacht(
            # FIELDS TO MAP TO ABWASSER.normschacht
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "normschacht"),
            # --- abwasserbauwerk ---
            **qgep_export_utils.wastewater_structure_common(row),
            # --- normschacht ---
            dimension1=row.dimension1,
            dimension2=row.dimension2,
            funktion=qgep_export_utils.get_vl(row.function__REL),
            # -- attribute 3D ---
            # maechtigkeit=row.depth,
            material=qgep_export_utils.get_vl(row.material__REL),
            oberflaechenzulauf=qgep_export_utils.get_vl(row.surface_inflow__REL),
        )
        abwasser_session.add(normschacht)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.discharge_point -> ABWASSER.einleitstelle, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.discharge_point)
    if filtered:
        query = query.join(QGEP.wastewater_networkelement).filter(
            QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
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

        einleitstelle = ABWASSER.einleitstelle(
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
    query = qgep_session.query(QGEP.special_structure)
    if filtered:
        query = query.join(QGEP.wastewater_networkelement).filter(
            QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        )
    for row in query:
        # AVAILABLE FIELDS IN QGEP.special_structure

        # --- wastewater_structure ---
        # _bottom_label, _cover_label, _depth, _function_hierarchic, _input_label, _label, _output_label, _usage_current, accessibility, contract_section, detail_geometry_geometry, financing, fk_dataowner, fk_main_cover, fk_main_wastewater_node, fk_operator, fk_owner, fk_provider, gross_costs, identifier, inspection_interval, last_modification, location_name, records, remark, renovation_necessity, replacement_value, rv_base_year, rv_construction_type, status, structure_condition, subsidies, year_of_construction, year_of_replacement

        # --- special_structure ---
        # bypass, emergency_spillway, function, obj_id, stormwater_tank_arrangement, upper_elevation

        # --- _bwrel_ ---
        # measuring_point__BWREL_fk_wastewater_structure, mechanical_pretreatment__BWREL_fk_wastewater_structure, re_maintenance_event_wastewater_structure__BWREL_fk_wastewater_structure, structure_part__BWREL_fk_wastewater_structure, txt_symbol__BWREL_fk_wastewater_structure, txt_text__BWREL_fk_wastewater_structure, wastewater_networkelement__BWREL_fk_wastewater_structure, wastewater_structure_symbol__BWREL_fk_wastewater_structure, wastewater_structure_text__BWREL_fk_wastewater_structure, wwtp_structure_kind__BWREL_obj_id

        # --- _rel_ ---
        # accessibility__REL, bypass__REL, emergency_spillway__REL, financing__REL, fk_dataowner__REL, fk_main_cover__REL, fk_main_wastewater_node__REL, fk_operator__REL, fk_owner__REL, fk_provider__REL, function__REL, renovation_necessity__REL, rv_construction_type__REL, status__REL, stormwater_tank_arrangement__REL, structure_condition__REL

        # QGEP field special_structure.upper_elevation is a 3D attribute and has no equivalent in the INTERLIS 2D model release used. It will be ignored for now and not supported with QGEP.

        spezialbauwerk = ABWASSER.spezialbauwerk(
            # FIELDS TO MAP TO ABWASSER.spezialbauwerk
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "spezialbauwerk"),
            # --- abwasserbauwerk ---
            **qgep_export_utils.wastewater_structure_common(row),
            # --- spezialbauwerk ---
            # TODO : WARNING : upper_elevation is not mapped
            bypass=qgep_export_utils.get_vl(row.bypass__REL),
            funktion=qgep_export_utils.get_vl(row.function__REL),
            notueberlauf=qgep_export_utils.get_vl(row.emergency_spillway__REL),
            regenbecken_anordnung=qgep_export_utils.get_vl(row.stormwater_tank_arrangement__REL),
        )
        abwasser_session.add(spezialbauwerk)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.infiltration_installation -> ABWASSER.versickerungsanlage, ABWASSER.metaattribute"
    )
    query = qgep_session.query(QGEP.infiltration_installation)
    if filtered:
        query = query.join(QGEP.wastewater_networkelement).filter(
            QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
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

        logger.warning(
            "QGEP field infiltration_installation.upper_elevation has no equivalent in the interlis model. It will be ignored."
        )
        versickerungsanlage = ABWASSER.versickerungsanlage(
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
    query = qgep_session.query(QGEP.pipe_profile)
    if filtered:
        query = query.join(QGEP.reach).filter(
            QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        )
    for row in query:

        # AVAILABLE FIELDS IN QGEP.pipe_profile

        # --- pipe_profile ---
        # fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark

        # --- _bwrel_ ---
        # profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

        # --- _rel_ ---
        # fk_dataowner__REL, fk_provider__REL, profile_type__REL

        rohrprofil = ABWASSER.rohrprofil(
            # FIELDS TO MAP TO ABWASSER.rohrprofil
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "rohrprofil"),
            # --- rohrprofil ---
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            hoehenbreitenverhaeltnis=row.height_width_ratio,
            profiltyp=qgep_export_utils.get_vl(row.profile_type__REL),
        )
        abwasser_session.add(rohrprofil)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.reach_point -> ABWASSER.haltungspunkt, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.reach_point)
    if filtered:
        query = query.join(
            QGEP.reach,
            or_(
                QGEP.reach_point.obj_id == QGEP.reach.fk_reach_point_from,
                QGEP.reach_point.obj_id == QGEP.reach.fk_reach_point_to,
            ),
        ).filter(QGEP.wastewater_networkelement.obj_id.in_(subset_ids))
    for row in query:

        # AVAILABLE FIELDS IN QGEP.reach_point

        # --- reach_point ---
        # elevation_accuracy, fk_dataowner, fk_provider, fk_wastewater_networkelement, identifier, last_modification, level, obj_id, outlet_shape, position_of_connection, remark, situation_geometry

        # --- _bwrel_ ---
        # examination__BWREL_fk_reach_point, reach__BWREL_fk_reach_point_from, reach__BWREL_fk_reach_point_to

        # --- _rel_ ---
        # elevation_accuracy__REL, fk_dataowner__REL, fk_provider__REL, fk_wastewater_networkelement__REL, outlet_shape__REL

        haltungspunkt = ABWASSER.haltungspunkt(
            # FIELDS TO MAP TO ABWASSER.haltungspunkt
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "haltungspunkt"),
            # --- haltungspunkt ---
            # changed call from qgep_export_utils.get_tid to qgep_export_utils.check_fk_in_subsetid so it does not wirte foreignkeys on elements that do not exist
            # abwassernetzelementref=qgep_export_utils.get_tid(row.fk_wastewater_networkelement__REL),
            abwassernetzelementref=qgep_export_utils.check_fk_in_subsetid(
                subset_ids, row.fk_wastewater_networkelement__REL, filtered
            ),
            auslaufform=qgep_export_utils.get_vl(row.outlet_shape__REL),
            bemerkung=qgep_export_utils.truncate(
                qgep_export_utils.emptystr_to_null(row.remark), 80
            ),
            bezeichnung=qgep_export_utils.null_to_emptystr(row.identifier),
            hoehengenauigkeit=qgep_export_utils.get_vl(row.elevation_accuracy__REL),
            kote=row.level,
            lage=ST_Force2D(row.situation_geometry),
            lage_anschluss=row.position_of_connection,
        )
        abwasser_session.add(haltungspunkt)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.wastewater_node -> ABWASSER.abwasserknoten, ABWASSER.metaattribute"
    )
    query = qgep_session.query(QGEP.wastewater_node)
    if filtered:
        query = query.filter(QGEP.wastewater_networkelement.obj_id.in_(subset_ids))
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

        abwasserknoten = ABWASSER.abwasserknoten(
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
    query = qgep_session.query(QGEP.reach)
    if filtered:
        query = query.filter(QGEP.wastewater_networkelement.obj_id.in_(subset_ids))
    for row in query:
        # AVAILABLE FIELDS IN QGEP.reach

        # --- wastewater_networkelement ---
        # fk_dataowner, fk_provider, fk_wastewater_structure, identifier, last_modification, remark

        # --- reach ---
        # clear_height, coefficient_of_friction, elevation_determination, fk_pipe_profile, fk_reach_point_from, fk_reach_point_to, horizontal_positioning, inside_coating, length_effective, material, obj_id, progression_geometry, reliner_material, reliner_nominal_size, relining_construction, relining_kind, ring_stiffness, slope_building_plan, wall_roughness

        # --- _bwrel_ ---
        # catchment_area__BWREL_fk_wastewater_networkelement_rw_current, catchment_area__BWREL_fk_wastewater_networkelement_rw_planned, catchment_area__BWREL_fk_wastewater_networkelement_ww_current, catchment_area__BWREL_fk_wastewater_networkelement_ww_planned, connection_object__BWREL_fk_wastewater_networkelement, reach_point__BWREL_fk_wastewater_networkelement, reach_text__BWREL_fk_reach, txt_text__BWREL_fk_reach

        # --- _rel_ ---
        # elevation_determination__REL, fk_dataowner__REL, fk_pipe_profile__REL, fk_provider__REL, fk_reach_point_from__REL, fk_reach_point_to__REL, fk_wastewater_structure__REL, horizontal_positioning__REL, inside_coating__REL, material__REL, reliner_material__REL, relining_construction__REL, relining_kind__REL

        # QGEP field reach.elevation_determination has no equivalent in the interlis model. It will be ignored.

        haltung = ABWASSER.haltung(
            # FIELDS TO MAP TO ABWASSER.haltung
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "haltung"),
            # --- abwassernetzelement ---
            **qgep_export_utils.wastewater_networkelement_common(row),
            # --- haltung ---
            # NOT MAPPED : elevation_determination
            innenschutz=qgep_export_utils.get_vl(row.inside_coating__REL),
            laengeeffektiv=row.length_effective,
            lagebestimmung=qgep_export_utils.get_vl(row.horizontal_positioning__REL),
            lichte_hoehe=row.clear_height,
            material=qgep_export_utils.get_vl(row.material__REL),
            nachhaltungspunktref=qgep_export_utils.get_tid(row.fk_reach_point_to__REL),
            plangefaelle=row.slope_building_plan,  # TODO : check, does this need conversion ?
            reibungsbeiwert=row.coefficient_of_friction,
            reliner_art=qgep_export_utils.get_vl(row.relining_kind__REL),
            reliner_bautechnik=qgep_export_utils.get_vl(row.relining_construction__REL),
            reliner_material=qgep_export_utils.get_vl(row.reliner_material__REL),
            reliner_nennweite=row.reliner_nominal_size,
            ringsteifigkeit=row.ring_stiffness,
            rohrprofilref=qgep_export_utils.get_tid(row.fk_pipe_profile__REL),
            verlauf=ST_Force2D(row.progression_geometry),
            # -- attribute 3D ---
            # verlauf3d=row.progression3d,
            vonhaltungspunktref=qgep_export_utils.get_tid(row.fk_reach_point_from__REL),
            wandrauhigkeit=row.wall_roughness,
        )
        abwasser_session.add(haltung)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.dryweather_downspout -> ABWASSER.trockenwetterfallrohr, ABWASSER.metaattribute"
    )
    query = qgep_session.query(QGEP.dryweather_downspout)
    if filtered:
        query = query.join(QGEP.wastewater_structure, QGEP.wastewater_networkelement).filter(
            QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        )
    for row in query:
        # AVAILABLE FIELDS IN QGEP.dryweather_downspout

        # --- structure_part ---
        # fk_dataowner, fk_provider, fk_wastewater_structure, identifier, last_modification, remark, renovation_demand

        # --- dryweather_downspout ---
        # diameter, obj_id

        # --- _bwrel_ ---
        # access_aid_kind__BWREL_obj_id, backflow_prevention__BWREL_obj_id, benching_kind__BWREL_obj_id, dryweather_flume_material__BWREL_obj_id, electric_equipment__BWREL_obj_id, electromechanical_equipment__BWREL_obj_id, solids_retention__BWREL_obj_id, tank_cleaning__BWREL_obj_id, tank_emptying__BWREL_obj_id

        # --- _rel_ ---
        # fk_dataowner__REL, fk_provider__REL, fk_wastewater_structure__REL, renovation_demand__REL

        trockenwetterfallrohr = ABWASSER.trockenwetterfallrohr(
            # FIELDS TO MAP TO ABWASSER.trockenwetterfallrohr
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "trockenwetterfallrohr"),
            # --- bauwerksteil ---
            **qgep_export_utils.structure_part_common(row),
            # --- trockenwetterfallrohr ---
            durchmesser=row.diameter,
        )
        abwasser_session.add(trockenwetterfallrohr)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.access_aid -> ABWASSER.einstiegshilfe, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.access_aid)
    if filtered:
        query = query.join(QGEP.wastewater_structure, QGEP.wastewater_networkelement).filter(
            QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        )
    for row in query:
        # AVAILABLE FIELDS IN QGEP.access_aid

        # --- structure_part ---
        # fk_dataowner, fk_provider, fk_wastewater_structure, identifier, last_modification, remark, renovation_demand

        # --- access_aid ---
        # kind, obj_id

        # --- _bwrel_ ---
        # access_aid_kind__BWREL_obj_id, backflow_prevention__BWREL_obj_id, benching_kind__BWREL_obj_id, dryweather_flume_material__BWREL_obj_id, electric_equipment__BWREL_obj_id, electromechanical_equipment__BWREL_obj_id, solids_retention__BWREL_obj_id, tank_cleaning__BWREL_obj_id, tank_emptying__BWREL_obj_id

        # --- _rel_ ---
        # fk_dataowner__REL, fk_provider__REL, fk_wastewater_structure__REL, kind__REL, renovation_demand__REL

        einstiegshilfe = ABWASSER.einstiegshilfe(
            # FIELDS TO MAP TO ABWASSER.einstiegshilfe
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "einstiegshilfe"),
            # --- bauwerksteil ---
            **qgep_export_utils.structure_part_common(row),
            # --- einstiegshilfe ---
            art=qgep_export_utils.get_vl(row.kind__REL),
        )
        abwasser_session.add(einstiegshilfe)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info(
        "Exporting QGEP.dryweather_flume -> ABWASSER.trockenwetterrinne, ABWASSER.metaattribute"
    )
    query = qgep_session.query(QGEP.dryweather_flume)
    if filtered:
        query = query.join(QGEP.wastewater_structure, QGEP.wastewater_networkelement).filter(
            QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        )
    for row in query:
        # AVAILABLE FIELDS IN QGEP.dryweather_flume

        # --- structure_part ---
        # fk_dataowner, fk_provider, fk_wastewater_structure, identifier, last_modification, remark, renovation_demand

        # --- dryweather_flume ---
        # material, obj_id

        # --- _bwrel_ ---
        # access_aid_kind__BWREL_obj_id, backflow_prevention__BWREL_obj_id, benching_kind__BWREL_obj_id, dryweather_flume_material__BWREL_obj_id, electric_equipment__BWREL_obj_id, electromechanical_equipment__BWREL_obj_id, solids_retention__BWREL_obj_id, tank_cleaning__BWREL_obj_id, tank_emptying__BWREL_obj_id

        # --- _rel_ ---
        # fk_dataowner__REL, fk_provider__REL, fk_wastewater_structure__REL, material__REL, renovation_demand__REL

        trockenwetterrinne = ABWASSER.trockenwetterrinne(
            # FIELDS TO MAP TO ABWASSER.trockenwetterrinne
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "trockenwetterrinne"),
            # --- bauwerksteil ---
            **qgep_export_utils.structure_part_common(row),
            # --- trockenwetterrinne ---
            material=qgep_export_utils.get_vl(row.material__REL),
        )
        abwasser_session.add(trockenwetterrinne)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.cover -> ABWASSER.deckel, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.cover)
    if filtered:
        query = query.join(QGEP.wastewater_structure, QGEP.wastewater_networkelement).filter(
            QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        )
    for row in query:
        # AVAILABLE FIELDS IN QGEP.cover

        # --- structure_part ---
        # fk_dataowner, fk_provider, fk_wastewater_structure, identifier, last_modification, remark, renovation_demand

        # --- cover ---
        # brand, cover_shape, diameter, fastening, level, material, obj_id, positional_accuracy, situation_geometry, sludge_bucket, venting

        # --- _bwrel_ ---
        # access_aid_kind__BWREL_obj_id, backflow_prevention__BWREL_obj_id, benching_kind__BWREL_obj_id, dryweather_flume_material__BWREL_obj_id, electric_equipment__BWREL_obj_id, electromechanical_equipment__BWREL_obj_id, solids_retention__BWREL_obj_id, tank_cleaning__BWREL_obj_id, tank_emptying__BWREL_obj_id, wastewater_structure__BWREL_fk_main_cover

        # --- _rel_ ---
        # cover_shape__REL, fastening__REL, fk_dataowner__REL, fk_provider__REL, fk_wastewater_structure__REL, material__REL, positional_accuracy__REL, renovation_demand__REL, sludge_bucket__REL, venting__REL

        deckel = ABWASSER.deckel(
            # FIELDS TO MAP TO ABWASSER.deckel
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "deckel"),
            # --- bauwerksteil ---
            **qgep_export_utils.structure_part_common(row),
            # --- deckel ---
            deckelform=qgep_export_utils.get_vl(row.cover_shape__REL),
            durchmesser=row.diameter,
            entlueftung=qgep_export_utils.get_vl(row.venting__REL),
            fabrikat=row.brand,
            kote=row.level,
            lage=ST_Force2D(row.situation_geometry),
            lagegenauigkeit=qgep_export_utils.get_vl(row.positional_accuracy__REL),
            material=qgep_export_utils.get_vl(row.material__REL),
            schlammeimer=qgep_export_utils.get_vl(row.sludge_bucket__REL),
            verschluss=qgep_export_utils.get_vl(row.fastening__REL),
        )
        abwasser_session.add(deckel)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.benching -> ABWASSER.bankett, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.benching)
    if filtered:
        query = query.join(QGEP.wastewater_structure, QGEP.wastewater_networkelement).filter(
            QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        )
    for row in query:
        # AVAILABLE FIELDS IN QGEP.benching

        # --- structure_part ---
        # fk_dataowner, fk_provider, fk_wastewater_structure, identifier, last_modification, remark, renovation_demand

        # --- benching ---
        # kind, obj_id

        # --- _bwrel_ ---
        # access_aid_kind__BWREL_obj_id, backflow_prevention__BWREL_obj_id, benching_kind__BWREL_obj_id, dryweather_flume_material__BWREL_obj_id, electric_equipment__BWREL_obj_id, electromechanical_equipment__BWREL_obj_id, solids_retention__BWREL_obj_id, tank_cleaning__BWREL_obj_id, tank_emptying__BWREL_obj_id

        # --- _rel_ ---
        # fk_dataowner__REL, fk_provider__REL, fk_wastewater_structure__REL, kind__REL, renovation_demand__REL

        bankett = ABWASSER.bankett(
            # FIELDS TO MAP TO ABWASSER.bankett
            # --- baseclass ---
            # --- sia405_baseclass ---
            **qgep_export_utils.base_common(row, "bankett"),
            # --- bauwerksteil ---
            **qgep_export_utils.structure_part_common(row),
            # --- bankett ---
            art=qgep_export_utils.get_vl(row.kind__REL),
        )
        abwasser_session.add(bankett)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.examination -> ABWASSER.untersuchung, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.examination)
    if filtered:
        query = (
            query.join(QGEP.re_maintenance_event_wastewater_structure)
            .join(QGEP.wastewater_structure)
            .join(QGEP.wastewater_networkelement)
            .filter(QGEP.wastewater_networkelement.obj_id.in_(subset_ids))
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

        untersuchung = ABWASSER.untersuchung(
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
    query = qgep_session.query(QGEP.damage_manhole)
    if filtered:
        query = (
            query.join(QGEP.examination)
            .join(QGEP.re_maintenance_event_wastewater_structure)
            .join(QGEP.wastewater_structure)
            .join(QGEP.wastewater_networkelement)
            .filter(QGEP.wastewater_networkelement.obj_id.in_(subset_ids))
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

        normschachtschaden = ABWASSER.normschachtschaden(
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
    query = qgep_session.query(QGEP.damage_channel)
    if filtered:
        query = (
            query.join(QGEP.examination)
            .join(QGEP.re_maintenance_event_wastewater_structure)
            .join(QGEP.wastewater_structure)
            .join(QGEP.wastewater_networkelement)
            .filter(QGEP.wastewater_networkelement.obj_id.in_(subset_ids))
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

        kanalschaden = ABWASSER.kanalschaden(
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
        abwasser_session.add(kanalschaden)
        qgep_export_utils.create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.data_media -> ABWASSER.datentraeger, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.data_media)
    for row in query:

        # AVAILABLE FIELDS IN QGEP.data_media

        # --- data_media ---
        # fk_dataowner, fk_provider, identifier, kind, last_modification, location, obj_id, path, remark

        # --- _rel_ ---
        # fk_dataowner__REL, fk_provider__REL, kind__REL

        datentraeger = ABWASSER.datentraeger(
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
    query = qgep_session.query(QGEP.file)
    if filtered:
        query = (
            query.outerjoin(QGEP.damage, QGEP.file.object == QGEP.damage.obj_id)
            .join(
                QGEP.examination,
                or_(
                    QGEP.file.object == QGEP.damage.obj_id,
                    QGEP.file.object == QGEP.examination.obj_id,
                ),
            )
            .join(QGEP.re_maintenance_event_wastewater_structure)
            .join(QGEP.wastewater_structure)
            .join(QGEP.wastewater_networkelement)
            .filter(QGEP.wastewater_networkelement.obj_id.in_(subset_ids))
        )
    for row in query:

        # AVAILABLE FIELDS IN QGEP.file

        # --- file ---
        # class, fk_data_media, fk_dataowner, fk_provider, identifier, kind, last_modification, obj_id, object, path_relative, remark

        # --- _rel_ ---
        # class__REL, fk_dataowner__REL, fk_provider__REL, kind__REL

        datei = ABWASSER.datei(
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

    # Labels
    # Note: these are extracted from the optional labels file (not exported from the QGEP database)
    if labels_file:
        logger.info(f"Exporting label positions from {labels_file}")

        # Get t_id by obj_name to create the reference on the labels below
        tid_for_obj_id = {
            "haltung": {},
            "abwasserbauwerk": {},
        }
        for row in abwasser_session.query(ABWASSER.haltung):
            tid_for_obj_id["haltung"][row.obj_id] = row.t_id
        for row in abwasser_session.query(ABWASSER.abwasserbauwerk):
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
                ili_label = ABWASSER.haltung_text(
                    **qgep_export_utils.textpos_common(label, "haltung_text", geojson_crs_def),
                    haltungref=tid_for_obj_id["haltung"][obj_id],
                )

            elif layer_name == "vw_qgep_wastewater_structure":
                if obj_id not in tid_for_obj_id["abwasserbauwerk"]:
                    logger.warning(
                        f"Label for abwasserbauwerk `{obj_id}` exists, but that object is not part of the export"
                    )
                    continue
                ili_label = ABWASSER.abwasserbauwerk_text(
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
