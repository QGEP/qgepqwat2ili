import json

from geoalchemy2.functions import ST_Force2D
from sqlalchemy.orm import Session
from sqlalchemy.sql import text

from .. import utils
from ..utils.basket_utils import BasketUtils

# 4.10.2024
# 6.11.2024 replaced with / 15.11.2024 get_ws_selected_ww_networkelements added
from ..utils.ili2db import (
    add_to_selection,
    get_ws_selected_ww_networkelements,
    get_ws_wn_ids,
    remove_from_selection,
)
from ..utils.qgep_export_utils import QgepExportUtils
from ..utils.various import logger
from .model_abwasser import get_abwasser_model
from .model_qgep import get_qgep_model


def qgep_export_sia405(selection=None, labels_file=None, orientation=None, basket_enabled=False):
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

    # 1. Filtering - check if selection

    filtered = selection is not None
    subset_ids = selection if selection is not None else []

    flag_approach_urs = true

    if flag_approach_urs:
        # 2. Get all connected from wastewater_nodes of selected reaches
        connected_from_wn_ids = get_connected_we_from_re(subset_ids)
        # 3. Get all connected to wastewater_nodes of selected reaches
        connected_to_wn_ids = get_connected_we_to_re(subset_ids)
        # 4. Get all connected wastewater_nodes from overflows.fk_overflow_to
        connected_overflow_to_wn_ids = get_connected_overflow_to_wn_ids(subset_ids)
        # 5. Add results from 2., 3. and 4. to subset_ids -> adapted_subset_ids
        adapted_subset_ids = add_to_selection(subset_ids, connected_from_wn_ids)
        adapted_subset_ids = add_to_selection(adapted_subset_ids, connected_to_wn_ids)
        adapted_subset_ids = add_to_selection(adapted_subset_ids, connected_overflow_to_wn_ids)
        # 6. check blind connections - are there reaches in adapted_subset_ids that have not been in subset_ids
        subset_ids_reaches = filter_reaches(subset_ids)
        adapted_subset_ids_reaches = filter_reaches(adapted_subset_ids)

        # https://www.w3schools.com/python/ref_set_difference.asp
        # x = {"apple", "banana", "cherry"}
        # y = {"google", "microsoft", "apple"}
        # z = x.difference(y)
        extra_reaches_ids = subset_ids_reaches.difference(adapted_subset_ids_reaches)
        # 7. If extra_reaches then remove from adapted_subset_ids
        if not extra_reaches_ids:
            # list is empty - no need for adaption
            logger.debug(
                f"no extra reaches - so nothing to remove from adapted_subset_ids",
            )
        else:
            # if len(extra_reaches_ids) > 0:
            adapted_subset_ids = remove_from_selection(adapted_subset_ids, extra_reaches_ids)
        # 8. get all id's of connected wastewater_structures
        subset_wws_ids = get_ws_selected_ww_networkelements(adapted_subset_ids)
        logger.debug(
            f"subset_wws_ids: {subset_wws_ids}",
        )
        # 9. if sia405 export: check if wastewater_structures exist that are not part of SIA 405 Abwasser (in Release 2015 this is the class wwtp_structures, in Release 2020 it will be more - to be extended in tww)
        ws_off_sia405abwasser_list = None
        ws_off_sia405abwasser_list = get_ws_wn_ids("wwtp_structure")

        # 10. Show ws_off_sia405abwasser_list
        logger.debug(
            f"ws_off_sia405abwasser_list : {ws_off_sia405abwasser_list}",
        )
        # 11. take out ws_off_sia405abwasser_list from subset_wws_ids
        subset_wws_ids = remove_from_selection(subset_wws_ids, ws_off_sia405abwasser_list)
        logger.debug(
            f"subset_ids of all wws minus ws_off_sia405abwasser_list: {subset_wws_ids}",
        )
    else:
        # 2. check if wastewater_structures exist that are not part of SIA 405 Abwasser (in Release 2015 this is the class wwtp_structures, in Release 2020 it will be more - to be extended in tww)
        ws_off_sia405abwasser_list = None
        ws_off_sia405abwasser_list = get_ws_wn_ids("wwtp_structure")

        # 3. Show ws_off_sia405abwasser_list
        logger.debug(
            f"ws_off_sia405abwasser_list : {ws_off_sia405abwasser_list}",
        )

        # 4. check if filtered
        if filtered:
            if ws_off_sia405abwasser_list:
                # take out ws_off_sia405abwasser_list from selection
                subset_ids = remove_from_selection(subset_ids, ws_off_sia405abwasser_list)
            # else do nothing
        else:
            if ws_off_sia405abwasser_list:
                # add all data of wastewater_structures to selection
                subset_ids = add_to_selection(subset_ids, get_ws_wn_ids("wastewater_structure"))
                logger.debug(
                    f"subset_ids of all wws : {subset_ids}",
                )
                # take out ws_off_sia405abwasser_list from selection
                subset_ids = remove_from_selection(subset_ids, ws_off_sia405abwasser_list)
                logger.debug(
                    f"subset_ids of all wws minus ws_off_sia405abwasser_list: {subset_ids}",
                )
                # add reach_ids
                # subset_ids = add_to_selection(subset_ids, get_cl_re_ids("channel"))
                # treat export as with a selection
                filtered = True

            # else do nothing

        # 5. get and add all id's of connected wastewater_structures (not only of wastewater_network_element (reach, wwn)
        subset_wws_ids = get_ws_selected_ww_networkelements(subset_ids)
        logger.debug(
            f"subset_wws_ids: {subset_wws_ids}",
        )
        subset_ids = add_to_selection(subset_ids, subset_wws_ids)
        logger.debug(
            f"subset_ids with wws : {subset_ids}",
        )

    # Orientation
    oriented = orientation is not None
    if oriented:
        labelorientation = orientation
    else:
        labelorientation = 0


    def get_tid(relation):
        """
        Makes a tid for a relation
        """
        if relation is None:
            return None

        return tid_maker.tid_for_row(relation)

    def get_vl(relation):
        """
        Gets a literal value from a value list relation
        """
        if relation is None:
            return None
        return relation.value_de

    def null_to_emptystr(val):
        """
        Converts nulls to blank strings and raises a warning
        """
        if val is None:
            logger.warning(
                "A mandatory value was null. It will be cast to a blank string, and probably cause validation errors",
            )
            val = ""
        return val

    def emptystr_to_null(val):
        """
        Converts blank strings to nulls and raises a warning

        This is needed as is seems ili2pg 4.4.6 crashes with emptystrings under certain circumstances (see https://github.com/QGEP/qgepqwat2ili/issues/33)
        """
        if val == "":
            logger.warning(
                "An empty string was converted to NULL, to workaround ili2pg issue. This should have no impact on output.",
            )
            val = None
        return val

    def truncate(val, max_length):
        """
        Raises a warning if values gets truncated
        """
        if val is None:
            return None
        if len(val) > max_length:
            # _log() got an unexpected keyword argument 'stacklevel'
            #    logger.warning(f"Value '{val}' exceeds expected length ({max_length})", stacklevel=2)
            logger.warning(f"Value '{val}' exceeds expected length ({max_length})")
        return val[0:max_length]

    def modulo_angle(val):
        """
        Returns an angle between 0 and 359.9 (for Orientierung in Base_d-20181005.ili)
        """
        if val is None:
            return None

        # add orientation
        val = val + float(labelorientation)

        val = val % 360.0
        if val > 359.9:
            val = 0

        logger.info(f"modulo_angle - added orientation: {labelorientation}")
        print("modulo_angle - added orientation: ", str(labelorientation))

        return val

    def check_fk_in_subsetid(subset, relation):
        """
        checks, whether foreignkey is in the subset_ids - if yes it return the tid of the foreignkey, if no it will return None
        """
        # first check for None, as is get_tid
        if relation is None:
            return None

        logger.debug(f"check_fk_in_subsetid -  Subset ID's '{subset}'")
        # get the value of the fk_ attribute as str out of the relation to be able to check whether it is in the subset
        fremdschluesselstr = getattr(relation, "obj_id")
        logger.debug(f"check_fk_in_subsetid -  fremdschluesselstr '{fremdschluesselstr}'")

        if fremdschluesselstr in subset:
            logger.debug(f"check_fk_in_subsetid - '{fremdschluesselstr}' is in subset ")
            logger.debug(f"check_fk_in_subsetid - tid = '{tid_maker.tid_for_row(relation)}' ")
            return tid_maker.tid_for_row(relation)
        else:
            if filtered:
                logger.warning(
                    f"check_fk_in_subsetid - '{fremdschluesselstr}' is not in filtered subset - replaced with None instead!"
                )
                return None
            else:
                logger.warning(
                    f"check_fk_in_subsetid - '{fremdschluesselstr}' is not in datamodel - replaced with None instead!"
                )
                return None

    def create_metaattributes(row):
        metaattribute = ABWASSER.metaattribute(
            # FIELDS TO MAP TO ABWASSER.metaattribute
            # --- metaattribute ---
            # datenherr=getattr(row.fk_dataowner__REL, "name", "unknown"),  # TODO : is unknown ok ?
            # datenlieferant=getattr(row.fk_provider__REL, "name", "unknown"),  # TODO : is unknown ok ?
            # obj_id instead of name
            datenherr=getattr(
                row.fk_dataowner__REL, "obj_id", "unknown"
            ),  # TODO : is unknown ok ?
            datenlieferant=getattr(
                row.fk_provider__REL, "obj_id", "unknown"
            ),  # TODO : is unknown ok ?
            letzte_aenderung=row.last_modification,
            sia405_baseclass_metaattribute=get_tid(row),
            # OD : is this OK ? Don't we need a different t_id from what inserted above in organisation ? if so, consider adding a "for_class" arg to tid_for_row
            t_id=get_tid(row),
            t_seq=0,
        )
        abwasser_session.add(metaattribute)

    def base_common(row, type_name):
        """
        Returns common attributes for base
        """
        return {
            "t_ili_tid": row.obj_id,
            "t_type": type_name,
            "obj_id": row.obj_id,
            "t_id": get_tid(row),
        }

    def wastewater_structure_common(row):
        """
        Returns common attributes for wastewater_structure
        ATTENTION : Mapping of 3D wastewater_structure->abwasserbauerk
        is not fully implemented.
        """
        return {
            # --- abwasserbauwerk ---
            "akten": row.records,
            "astatus": get_vl(row.status__REL),
            "baujahr": row.year_of_construction,
            "baulicherzustand": get_vl(row.structure_condition__REL),
            "baulos": row.contract_section,
            "bemerkung": truncate(emptystr_to_null(row.remark), 80),
            "betreiberref": get_tid(row.fk_operator__REL),
            "bezeichnung": null_to_emptystr(row.identifier),
            "bruttokosten": row.gross_costs,
            "detailgeometrie": ST_Force2D(row.detail_geometry_geometry),
            "eigentuemerref": get_tid(row.fk_owner__REL),
            "ersatzjahr": row.year_of_replacement,
            "finanzierung": get_vl(row.financing__REL),
            "inspektionsintervall": row.inspection_interval,
            "sanierungsbedarf": get_vl(row.renovation_necessity__REL),
            "standortname": row.location_name,
            "subventionen": row.subsidies,
            "wbw_basisjahr": row.rv_base_year,
            "wbw_bauart": get_vl(row.rv_construction_type__REL),
            "wiederbeschaffungswert": row.replacement_value,
            "zugaenglichkeit": get_vl(row.accessibility__REL),
        }

    def wastewater_networkelement_common(row):
        """
        Returns common attributes for network_element
        """

        return {
            # "abwasserbauwerkref": get_tid(row.fk_wastewater_structure__REL),
            # 6.11.2024 Besides wn_id and re_id we also need ws_obj_ids in a separate subset - call it ws_subset_id / 15.11.2024 integrated wws in subset_ids
            "abwasserbauwerkref": check_fk_in_subsetid(
                subset_ids, row.fk_wastewater_structure__REL
            ),
            "bemerkung": truncate(emptystr_to_null(row.remark), 80),
            "bezeichnung": null_to_emptystr(row.identifier),
        }

    def structure_part_common(row):
        """
        Returns common attributes for structure_part
        """
        return {
            # abwasserbauwerkref is MANDATORY, so it cannot be set to NULL
            "abwasserbauwerkref": get_tid(row.fk_wastewater_structure__REL),
            # "abwasserbauwerkref": check_fk_in_subsetid(
            #    subset_ids, row.fk_wastewater_structure__REL
            # ),
            "bemerkung": truncate(emptystr_to_null(row.remark), 80),
            "bezeichnung": null_to_emptystr(row.identifier),
            "instandstellung": get_vl(row.renovation_demand__REL),
        }

    def textpos_common(row, t_type, geojson_crs_def):
        """
        Returns common attributes for textpos
        """
        t_id = tid_maker.next_tid()
        return {
            "t_id": t_id,
            "t_type": t_type,
            "t_ili_tid": t_id,
            # --- TextPos ---
            "textpos": ST_GeomFromGeoJSON(
                json.dumps(
                    {
                        "type": "Point",
                        "coordinates": row["geometry"]["coordinates"],
                        "crs": geojson_crs_def,
                    }
                )
            ),
            "textori": modulo_angle(row["properties"]["LabelRotation"]),
            "texthali": "Left",  # can be Left/Center/Right
            "textvali": "Bottom",  # can be Top,Cap,Half,Base,Bottom
            # --- SIA405_TextPos ---
            "plantyp": row["properties"]["scale"],
            "textinhalt": row["properties"]["LabelText"],
            "bemerkung": None,
        }

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
            **base_common(row, "haltungspunkt"),
            # --- haltungspunkt ---
            # changed call from get_tid to check_fk_in_subsetid so it does not write foreignkeys on elements that do not exist
            # abwassernetzelementref=get_tid(row.fk_wastewater_networkelement__REL),
            abwassernetzelementref=check_fk_in_subsetid(
                subset_ids, row.fk_wastewater_networkelement__REL
            ),
            auslaufform=get_vl(row.outlet_shape__REL),
            bemerkung=truncate(emptystr_to_null(row.remark), 80),
            bezeichnung=null_to_emptystr(row.identifier),
            hoehengenauigkeit=get_vl(row.elevation_accuracy__REL),
            kote=row.level,
            lage=ST_Force2D(row.situation_geometry),
            lage_anschluss=row.position_of_connection,
        )
        abwasser_session.add(haltungspunkt)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    # qgep_export_utils.export_reach_point()


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


    query = qgep_session.query(QGEP.dryweather_downspout)
    if filtered:
        logger.info(f"filtered: subset_ids = {subset_ids}")
        # query = query.join(QGEP.wastewater_structure, QGEP.wastewater_networkelement).filter(
        #    QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        # )
        query = (
            query.join(
                QGEP.wastewater_structure,
                QGEP.structure_part.fk_wastewater_structure == QGEP.wastewater_structure.obj_id,
            )
            .join(QGEP.wastewater_networkelement)
            .filter(QGEP.wastewater_networkelement.obj_id.in_(subset_ids))
        )
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
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
            **base_common(row, "trockenwetterfallrohr"),
            # --- bauwerksteil ---
            **structure_part_common(row),
            # --- trockenwetterfallrohr ---
            durchmesser=row.diameter,
        )
        abwasser_session.add(trockenwetterfallrohr)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.access_aid -> ABWASSER.einstiegshilfe, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.access_aid)
    if filtered:
        # query = query.join(QGEP.wastewater_structure, QGEP.wastewater_networkelement).filter(
        # QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        # )
        query = (
            query.join(
                QGEP.wastewater_structure,
                QGEP.structure_part.fk_wastewater_structure == QGEP.wastewater_structure.obj_id,
            )
            .join(QGEP.wastewater_networkelement)
            .filter(QGEP.wastewater_networkelement.obj_id.in_(subset_ids))
        )
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
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
            **base_common(row, "einstiegshilfe"),
            # --- bauwerksteil ---
            **structure_part_common(row),
            # --- einstiegshilfe ---
            art=get_vl(row.kind__REL),
        )
        abwasser_session.add(einstiegshilfe)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    # qgep_export_utils.export_dryweather_downspout()

    logger.info("Exporting QGEP.access_aid -> ABWASSER.einstiegshilfe, ABWASSER.metaattribute")
    # qgep_export_utils.export_access_aid()


    logger.info(
        "Exporting QGEP.dryweather_flume -> ABWASSER.trockenwetterrinne, ABWASSER.metaattribute"
    )

    query = qgep_session.query(QGEP.dryweather_flume)
    if filtered:
        # query = query.join(QGEP.wastewater_structure, QGEP.wastewater_networkelement).filter(
        # QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        # )
        query = (
            query.join(
                QGEP.wastewater_structure,
                QGEP.structure_part.fk_wastewater_structure == QGEP.wastewater_structure.obj_id,
            )
            .join(QGEP.wastewater_networkelement)
            .filter(QGEP.wastewater_networkelement.obj_id.in_(subset_ids))
        )
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
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
            **base_common(row, "trockenwetterrinne"),
            # --- bauwerksteil ---
            **structure_part_common(row),
            # --- trockenwetterrinne ---
            material=get_vl(row.material__REL),
        )
        abwasser_session.add(trockenwetterrinne)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.cover -> ABWASSER.deckel, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.cover)
    if filtered:
        # query = query.join(QGEP.wastewater_structure, QGEP.wastewater_networkelement).filter(
        # QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        # )
        query = (
            query.join(
                QGEP.wastewater_structure,
                QGEP.structure_part.fk_wastewater_structure == QGEP.wastewater_structure.obj_id,
            )
            .join(QGEP.wastewater_networkelement)
            .filter(QGEP.wastewater_networkelement.obj_id.in_(subset_ids))
        )
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
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
            **base_common(row, "deckel"),
            # --- bauwerksteil ---
            **structure_part_common(row),
            # --- deckel ---
            deckelform=get_vl(row.cover_shape__REL),
            durchmesser=row.diameter,
            entlueftung=get_vl(row.venting__REL),
            fabrikat=row.brand,
            kote=row.level,
            lage=ST_Force2D(row.situation_geometry),
            lagegenauigkeit=get_vl(row.positional_accuracy__REL),
            material=get_vl(row.material__REL),
            schlammeimer=get_vl(row.sludge_bucket__REL),
            verschluss=get_vl(row.fastening__REL),
        )
        abwasser_session.add(deckel)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.benching -> ABWASSER.bankett, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.benching)
    if filtered:
        # query = query.join(QGEP.wastewater_structure, QGEP.wastewater_networkelement).filter(
        # QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        # )
        query = (
            query.join(
                QGEP.wastewater_structure,
                QGEP.structure_part.fk_wastewater_structure == QGEP.wastewater_structure.obj_id,
            )
            .join(QGEP.wastewater_networkelement)
            .filter(QGEP.wastewater_networkelement.obj_id.in_(subset_ids))
        )
        # add sql statement to logger
        statement = query.statement
        logger.debug(f" selection query = {statement}")
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
            **base_common(row, "bankett"),
            # --- bauwerksteil ---
            **structure_part_common(row),
            # --- bankett ---
            art=get_vl(row.kind__REL),
        )
        abwasser_session.add(bankett)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    # VSA -KEK commented out

    # logger.info("Exporting QGEP.examination -> ABWASSER.untersuchung, ABWASSER.metaattribute")
    # query = qgep_session.query(QGEP.examination)
    # if filtered:
    # query = (
    # query.join(QGEP.re_maintenance_event_wastewater_structure)
    # .join(QGEP.wastewater_structure)
    # .join(QGEP.wastewater_networkelement)
    # .filter(QGEP.wastewater_networkelement.obj_id.in_(subset_ids))
    # )

    # for row in query:

    # # AVAILABLE FIELDS IN QGEP.examination

    # # --- maintenance_event ---
    # # --- examination ---
    # # equipment, fk_reach_point, from_point_identifier, inspected_length, obj_id, recording_type, to_point_identifier, vehicle, videonumber, weather

    # # --- _bwrel_ ---
    # # damage__BWREL_fk_examination, re_maintenance_event_wastewater_structure__BWREL_fk_maintenance_event

    # # --- _rel_ ---
    # # fk_dataowner__REL, fk_operating_company__REL, fk_provider__REL, fk_reach_point__REL, kind__REL, recording_type__REL, status__REL, weather__REL
    # logger.warning(
    # f"QGEP field maintenance_event.active_zone has no equivalent in the interlis model. It will be ignored."
    # )

    # untersuchung = ABWASSER.untersuchung(
    # # FIELDS TO MAP TO ABWASSER.untersuchung
    # # --- baseclass ---
    # # --- sia405_baseclass ---
    # **base_common(row, "untersuchung"),
    # # --- erhaltungsereignis ---
    # # abwasserbauwerkref=row.REPLACE_ME,  # TODO : convert this to M2N relation through re_maintenance_event_wastewater_structure
    # art=get_vl(row.kind__REL),
    # astatus=get_vl(row.status__REL),
    # ausfuehrende_firmaref=get_tid(row.fk_operating_company__REL),
    # ausfuehrender=row.operator,
    # bemerkung=truncate(emptystr_to_null(row.remark), 80),
    # bezeichnung=null_to_emptystr(row.identifier),
    # datengrundlage=row.base_data,
    # dauer=row.duration,
    # detaildaten=row.data_details,
    # ergebnis=row.result,
    # grund=row.reason,
    # kosten=row.cost,
    # zeitpunkt=row.time_point,
    # # --- untersuchung ---
    # bispunktbezeichnung=row.to_point_identifier,
    # erfassungsart=get_vl(row.recording_type__REL),
    # fahrzeug=row.vehicle,
    # geraet=row.equipment,
    # haltungspunktref=get_tid(row.fk_reach_point__REL),
    # inspizierte_laenge=row.inspected_length,
    # videonummer=row.videonumber,
    # vonpunktbezeichnung=row.from_point_identifier,
    # witterung=get_vl(row.weather__REL),
    # )
    # abwasser_session.add(untersuchung)
    # create_metaattributes(row)
    # print(".", end="")
    # logger.info("done")
    # abwasser_session.flush()

    # logger.info("Exporting QGEP.damage_manhole -> ABWASSER.normschachtschaden, ABWASSER.metaattribute")
    # query = qgep_session.query(QGEP.damage_manhole)
    # if filtered:
    # query = (
    # query.join(QGEP.examination)
    # .join(QGEP.re_maintenance_event_wastewater_structure)
    # .join(QGEP.wastewater_structure)
    # .join(QGEP.wastewater_networkelement)
    # .filter(QGEP.wastewater_networkelement.obj_id.in_(subset_ids))
    # )
    # for row in query:

    # # AVAILABLE FIELDS IN QGEP.damage_manhole

    # # --- damage ---

    # # --- damage_manhole ---
    # # manhole_damage_code, manhole_shaft_area, obj_id

    # # --- _bwrel_ ---
    # # damage_channel_channel_damage_code__BWREL_obj_id

    # # --- _rel_ ---
    # # connection__REL, fk_dataowner__REL, fk_examination__REL, fk_provider__REL, manhole_damage_code__REL, manhole_shaft_area__REL, single_damage_class__REL

    # normschachtschaden = ABWASSER.normschachtschaden(
    # # FIELDS TO MAP TO ABWASSER.normschachtschaden
    # # --- baseclass ---
    # # --- sia405_baseclass ---
    # **base_common(row, "normschachtschaden"),
    # # --- schaden ---
    # anmerkung=row.comments,
    # ansichtsparameter=row.view_parameters,
    # einzelschadenklasse=get_vl(row.single_damage_class__REL),
    # streckenschaden=row.damage_reach,
    # untersuchungref=get_tid(row.fk_examination__REL),
    # verbindung=get_vl(row.connection__REL),
    # videozaehlerstand=row.video_counter,
    # # --- normschachtschaden ---
    # distanz=row.distance,
    # quantifizierung1=row.quantification1,
    # quantifizierung2=row.quantification2,
    # schachtbereich=get_vl(row.manhole_shaft_area__REL),
    # schachtschadencode=get_vl(row.manhole_damage_code__REL),
    # schadenlageanfang=row.damage_begin,
    # schadenlageende=row.damage_end,
    # )
    # abwasser_session.add(normschachtschaden)
    # create_metaattributes(row)
    # print(".", end="")
    # logger.info("done")
    # abwasser_session.flush()

    # logger.info("Exporting QGEP.damage_channel -> ABWASSER.kanalschaden, ABWASSER.metaattribute")
    # query = qgep_session.query(QGEP.damage_channel)
    # if filtered:
    # query = (
    # query.join(QGEP.examination)
    # .join(QGEP.re_maintenance_event_wastewater_structure)
    # .join(QGEP.wastewater_structure)
    # .join(QGEP.wastewater_networkelement)
    # .filter(QGEP.wastewater_networkelement.obj_id.in_(subset_ids))
    # )
    # for row in query:

    # # AVAILABLE FIELDS IN QGEP.damage_channel

    # # --- damage ---
    # # comments, connection, damage_begin, damage_end, damage_reach, distance, fk_dataowner, fk_examination, fk_provider, last_modification, quantification1, quantification2, single_damage_class, video_counter, view_parameters

    # # --- damage_channel ---
    # # , obj_id

    # # --- _bwrel_ ---
    # # damage_channel_channel_damage_code__BWREL_obj_id

    # # --- _rel_ ---
    # # channel_damage_code__REL, connection__REL, fk_dataowner__REL, fk_examination__REL, fk_provider__REL, single_damage_class__REL

    # kanalschaden = ABWASSER.kanalschaden(
    # # FIELDS TO MAP TO ABWASSER.kanalschaden
    # # --- baseclass ---
    # # --- sia405_baseclass ---
    # **base_common(row, "kanalschaden"),
    # # --- schaden ---
    # anmerkung=row.comments,
    # ansichtsparameter=row.view_parameters,
    # einzelschadenklasse=get_vl(row.single_damage_class__REL),
    # streckenschaden=row.damage_reach,
    # untersuchungref=get_tid(row.fk_examination__REL),
    # verbindung=get_vl(row.connection__REL),
    # videozaehlerstand=row.video_counter,
    # # --- kanalschaden ---
    # distanz=row.distance,
    # kanalschadencode=get_vl(row.channel_damage_code__REL),
    # quantifizierung1=row.quantification1,
    # quantifizierung2=row.quantification2,
    # schadenlageanfang=row.damage_begin,
    # schadenlageende=row.damage_end,
    # )
    # abwasser_session.add(kanalschaden)
    # create_metaattributes(row)
    # print(".", end="")
    # logger.info("done")
    # abwasser_session.flush()

    # logger.info("Exporting QGEP.data_media -> ABWASSER.datentraeger, ABWASSER.metaattribute")
    # query = qgep_session.query(QGEP.data_media)
    # for row in query:

    # # AVAILABLE FIELDS IN QGEP.data_media

    # # --- data_media ---
    # # fk_dataowner, fk_provider, identifier, kind, last_modification, location, obj_id, path, remark

    # # --- _rel_ ---
    # # fk_dataowner__REL, fk_provider__REL, kind__REL

    # datentraeger = ABWASSER.datentraeger(
    # # FIELDS TO MAP TO ABWASSER.datentraeger
    # # --- baseclass ---
    # # --- sia405_baseclass ---
    # **base_common(row, "datentraeger"),
    # # --- datentraeger ---
    # art=get_vl(row.kind__REL),
    # bemerkung=truncate(emptystr_to_null(row.remark), 80),
    # bezeichnung=null_to_emptystr(row.identifier),
    # pfad=row.path,
    # standort=row.location,
    # )
    # abwasser_session.add(datentraeger)
    # create_metaattributes(row)
    # print(".", end="")
    # logger.info("done")
    # abwasser_session.flush()

    # logger.info("Exporting QGEP.file -> ABWASSER.datei, ABWASSER.metaattribute")
    # query = qgep_session.query(QGEP.file)
    # if filtered:
    # query = (
    # query.outerjoin(QGEP.damage, QGEP.file.object == QGEP.damage.obj_id)
    # .join(
    # QGEP.examination,
    # or_(QGEP.file.object == QGEP.damage.obj_id, QGEP.file.object == QGEP.examination.obj_id),
    # )
    # .join(QGEP.re_maintenance_event_wastewater_structure)
    # .join(QGEP.wastewater_structure)
    # .join(QGEP.wastewater_networkelement)
    # .filter(QGEP.wastewater_networkelement.obj_id.in_(subset_ids))
    # )
    # for row in query:

    # # AVAILABLE FIELDS IN QGEP.file

    # # --- file ---
    # # class, fk_data_media, fk_dataowner, fk_provider, identifier, kind, last_modification, obj_id, object, path_relative, remark

    # # --- _rel_ ---
    # # class__REL, fk_dataowner__REL, fk_provider__REL, kind__REL

    # datei = ABWASSER.datei(
    # # FIELDS TO MAP TO ABWASSER.datei
    # # --- baseclass ---
    # # --- sia405_baseclass ---
    # **base_common(row, "datei"),
    # # --- datei ---
    # art=get_vl(row.kind__REL) or "andere",
    # bemerkung=truncate(emptystr_to_null(row.remark), 80),
    # bezeichnung=null_to_emptystr(row.identifier),
    # datentraegerref=get_tid(row.fk_data_media__REL),
    # klasse=get_vl(row.class__REL),
    # objekt=null_to_emptystr(row.object),
    # relativpfad=row.path_relative,
    # )
    # abwasser_session.add(datei)
    # create_metaattributes(row)
    # print(".", end="")
    # logger.info("done")
    # abwasser_session.flush()

    # qgep_export_utils.export_dryweather_flume()

    # logger.info("Exporting QGEP.cover -> ABWASSER.deckel, ABWASSER.metaattribute")
    # qgep_export_utils.export_cover()

    # logger.info("Exporting QGEP.benching -> ABWASSER.bankett, ABWASSER.metaattribute")
    # qgep_export_utils.export_benching()


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
