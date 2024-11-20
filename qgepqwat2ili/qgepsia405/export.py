import json

from geoalchemy2.functions import ST_Force2D
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
        abwasser_model=abwasser_model,
        qgep_session=qgep_session,
        qgep_model=qgep_model,
        labelorientation=labelorientation,
        filtered=filtered,
        subset_ids=subset_ids,
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

        logger.warning(
            "QGEP field infiltration_installation.upper_elevation has no equivalent in the interlis model. It will be ignored."
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

    logger.info("Exporting QGEP.reach_point -> ABWASSER.haltungspunkt, ABWASSER.metaattribute")
    qgep_export_utils.export_reach_point()

    logger.info(
        "Exporting QGEP.wastewater_node -> ABWASSER.abwasserknoten, ABWASSER.metaattribute"
    )
    query = qgep_session.query(qgep_model.wastewater_node)
    if filtered:
        query = query.filter(qgep_model.wastewater_networkelement.obj_id.in_(subset_ids))
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
