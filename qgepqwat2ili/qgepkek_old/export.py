from geoalchemy2.functions import ST_Force2D
from sqlalchemy import or_
from sqlalchemy.orm import Session

from .. import utils
from ..utils.various import logger
from .model_abwasser import get_abwasser_model
from .model_qgep import get_qgep_model


def qgep_export(selection=None):
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

    # Filtering
    filtered = selection is not None
    subset_ids = selection if selection is not None else []

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
                f"A mandatory value was null. It will be cast to a blank string, and probably cause validation errors",
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
                f"An empty string was converted to NULL, to workaround ili2pg issue. This should have no impact on output.",
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
            logger.warning(f"Value '{val}' exceeds expected length ({max_length})", stacklevel=2)
        return val[0:max_length]

    def create_metaattributes(row):
        metaattribute = ABWASSER.metaattribute(
            # FIELDS TO MAP TO ABWASSER.metaattribute
            # --- metaattribute ---
            datenherr=getattr(row.fk_dataowner__REL, "name", "unknown"),  # TODO : is unknown ok ?
            datenlieferant=getattr(row.fk_provider__REL, "name", "unknown"),  # TODO : is unknown ok ?
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
        """
        logger.warning(f"Mapping of wastewater_structure->abwasserbauwerk is not fully implemented.")
        return {
            # --- abwasserbauwerk ---
            "akten": row.records,
            "baujahr": row.year_of_construction,
            "baulicherzustand": get_vl(row.structure_condition__REL),
            "baulos": row.contract_section,
            "bemerkung": truncate(emptystr_to_null(row.remark), 80),
            "betreiberref": get_tid(row.fk_operator__REL),
            "bezeichnung": null_to_emptystr(row.identifier),
            "bruttokosten": row.gross_costs,
            "detailgeometrie": ST_Force2D(row.detail_geometry_geometry),
            #-- attribute 3D ---
            #"detailgeometrie3d": ST_Force3D(row.detail_geometry3d),
            "eigentuemerref": get_tid(row.fk_owner__REL),
            "ersatzjahr": row.year_of_replacement,
            "finanzierung": get_vl(row.financing__REL),
            "inspektionsintervall": row.inspection_interval,
            "sanierungsbedarf": get_vl(row.renovation_necessity__REL),
            "standortname": row.location_name,
            "astatus": get_vl(row.status__REL),
            "subventionen": row.subsidies,
            "wbw_basisjahr": row.rv_base_year,
            "wbw_bauart": get_vl(row.rv_construction_type__REL),
            "wiederbeschaffungswert": row.replacement_value,
            "zugaenglichkeit": get_vl(row.accessibility__REL),
        }
    def wastewater_networkelement_common(row):
        """
        Returns common attributes for wastewater_networkelement
        """
        return {
            "abwasserbauwerkref": get_tid(row.fk_wastewater_structure__REL),
            "bemerkung": truncate(emptystr_to_null(row.remark), 80),
            "bezeichnung": null_to_emptystr(row.identifier),
        }
    def structure_part_common(row):
        """
        Returns common attributes for structure_part
        """
        return {
            "abwasserbauwerkref": get_tid(row.fk_wastewater_structure__REL),
            "bemerkung": truncate(emptystr_to_null(row.remark), 80),
            "bezeichnung": null_to_emptystr(row.identifier),
            "instandstellung": get_vl(row.renovation_demand__REL),
        }

    # ADAPTED FROM 052a_sia405_abwasser_2015_2_d_interlisexport2.sql
    logger.info("Exporting QGEP.organisation -> ABWASSER.organisation, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.organisation)
    for row in query:

        # AVAILABLE FIELDS IN QGEP.organisation
        
        # --- organisation ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile
        
        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL
    
        organisation = ABWASSER.organisation(
            # FIELDS TO MAP TO ABWASSER.organisation
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "organisation"),
            # --- organisation ---

            bemerkung=truncate(emptystr_to_null(row.remark), 80),
            bezeichnung=row.identifier,
            # not supported in model qgep 2015
            # teil_vonref=get_tid(row.fk_part_of__REL),
            auid=row.uid,
        )
        abwasser_session.add(organisation)
        create_metaattributes(row)
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
        # to do attributeslist of superclass
        # --- channel ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden
        
        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,
        
        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,
        
        kanal = ABWASSER.kanal(
            # FIELDS TO MAP TO ABWASSER.kanal
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "kanal"),
            # --- abwasserbauwerk ---
            **wastewater_structure_common(row),
            # --- kanal ---

            bettung_umhuellung=get_vl(row.bedding_encasement__REL),
            funktionhierarchisch=get_vl(row.function_hierarchic__REL),
            funktionhydraulisch=get_vl(row.function_hydraulic__REL),
            nutzungsart_geplant=get_vl(row.usage_planned__REL),
            nutzungsart_ist=get_vl(row.usage_current__REL),
            rohrlaenge=row.pipe_length,
            spuelintervall=row.jetting_interval,
            verbindungsart=get_vl(row.connection_type__REL),

        )
        abwasser_session.add(kanal)
        create_metaattributes(row)
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
            **base_common(row, "normschacht"),
            # --- abwasserbauwerk ---
            **wastewater_structure_common(row),
            # --- normschacht ---

            dimension1=row.dimension1,
            dimension2=row.dimension2,
            funktion=get_vl(row.function__REL),
            #-- attribute 3D ---
            #maechtigkeit=row.depth,
            material=get_vl(row.material__REL),
            oberflaechenzulauf=get_vl(row.surface_inflow__REL),

        )
        abwasser_session.add(normschacht)
        create_metaattributes(row)
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
            **base_common(row, "einleitstelle"),
            # --- abwasserbauwerk ---
            **wastewater_structure_common(row),
            # --- einleitstelle ---

            #-- attribute 3D ---
            #deckenkote=row.upper_elevation,
            hochwasserkote=row.highwater_level,
            #-- attribute 3D ---
            #maechtigkeit=row.depth,
            relevanz=get_vl(row.relevance__REL),
            terrainkote=row.terrain_level,
            wasserspiegel_hydraulik=row.waterlevel_hydraulic,

        )
        abwasser_session.add(einleitstelle)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.special_structure -> ABWASSER.spezialbauwerk, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.special_structure)
    if filtered:
        query = query.join(QGEP.wastewater_networkelement).filter(
            QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        )
    for row in query:
        # AVAILABLE FIELDS IN QGEP.special_structure
        
        # --- wastewater_structure ---
        # to do attributeslist of superclass
        # --- special_structure ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden
        
        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,
        
        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,
        
        spezialbauwerk = ABWASSER.spezialbauwerk(
            # FIELDS TO MAP TO ABWASSER.spezialbauwerk
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "spezialbauwerk"),
            # --- abwasserbauwerk ---
            **wastewater_structure_common(row),
            # --- spezialbauwerk ---

            bypass=get_vl(row.bypass__REL),
            #-- attribute 3D ---
            #deckenkote=row.upper_elevation,
            funktion=get_vl(row.function__REL),
            #-- attribute 3D ---
            #maechtigkeit=row.depth,
            notueberlauf=get_vl(row.emergency_spillway__REL),
            regenbecken_anordnung=get_vl(row.stormwater_tank_arrangement__REL),

        )
        abwasser_session.add(spezialbauwerk)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.infiltration_installation -> ABWASSER.versickerungsanlage, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.infiltration_installation)
    if filtered:
        query = query.join(QGEP.wastewater_networkelement).filter(
            QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        )
    for row in query:
        # AVAILABLE FIELDS IN QGEP.infiltration_installation
        
        # --- wastewater_structure ---
        # to do attributeslist of superclass
        # --- infiltration_installation ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden
        
        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,
        
        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,
        
        versickerungsanlage = ABWASSER.versickerungsanlage(
            # FIELDS TO MAP TO ABWASSER.versickerungsanlage
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "versickerungsanlage"),
            # --- abwasserbauwerk ---
            **wastewater_structure_common(row),
            # --- versickerungsanlage ---

            art=get_vl(row.kind__REL),
            beschriftung=get_vl(row.labeling__REL),
            #-- attribute 3D ---
            #deckenkote=row.upper_elevation,
            dimension1=row.dimension1,
            dimension2=row.dimension2,
            gwdistanz=row.distance_to_aquifer,
            #-- attribute 3D ---
            #maechtigkeit=row.depth,
            maengel=get_vl(row.defects__REL),
            notueberlauf=get_vl(row.emergency_spillway__REL),
            saugwagen=get_vl(row.vehicle_access__REL),
            schluckvermoegen=row.absorption_capacity,
            versickerungswasser=get_vl(row.seepage_utilization__REL),
            wasserdichtheit=get_vl(row.watertightness__REL),
            wirksameflaeche=row.effective_area,

        )
        abwasser_session.add(versickerungsanlage)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.pipe_profile -> ABWASSER.rohrprofil, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.pipe_profile)
    if filtered:
        query = query.join(QGEP.reach).filter(QGEP.wastewater_networkelement.obj_id.in_(subset_ids))
    for row in query:

        # AVAILABLE FIELDS IN QGEP.pipe_profile
        
        # --- pipe_profile ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile
        
        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL
    
        rohrprofil = ABWASSER.rohrprofil(
            # FIELDS TO MAP TO ABWASSER.rohrprofil
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "rohrprofil"),
            # --- rohrprofil ---

            bemerkung=truncate(emptystr_to_null(row.remark), 80),
            bezeichnung=null_to_emptystr(row.identifier),
            hoehenbreitenverhaeltnis=row.height_width_ratio,
            profiltyp=get_vl(row.profile_type__REL),
        )
        abwasser_session.add(rohrprofil)
        create_metaattributes(row)
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
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile
        
        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL
    
        haltungspunkt = ABWASSER.haltungspunkt(
            # FIELDS TO MAP TO ABWASSER.haltungspunkt
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "haltungspunkt"),
            # --- haltungspunkt ---

            abwassernetzelementref=get_tid(row.fk_wastewater_networkelement__REL),
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

    logger.info("Exporting QGEP.wastewater_node -> ABWASSER.abwasserknoten, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.wastewater_node)
    if filtered:
        query = query.filter(QGEP.wastewater_networkelement.obj_id.in_(subset_ids))
    for row in query:
        # AVAILABLE FIELDS IN QGEP.wastewater_node
        
        # --- wastewater_networkelement ---
        # to do attributeslist of superclass
        # --- wastewater_node ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden
        
        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,
        
        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,
        
        abwasserknoten = ABWASSER.abwasserknoten(
            # FIELDS TO MAP TO ABWASSER.abwasserknoten
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "abwasserknoten"),
            # --- abwassernetzelement ---
            **wastewater_networkelement_common(row),
            # --- abwasserknoten ---
            # attribut release 2020
            # ara_nr=row.wwtp_number,
            lage=ST_Force2D(row.situation_geometry),
            rueckstaukote=row.backflow_level,
            sohlenkote=row.bottom_level,

        )
        abwasser_session.add(abwasserknoten)
        create_metaattributes(row)
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
        # to do attributeslist of superclass
        # --- reach ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden
        
        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,
        
        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,
        
        haltung = ABWASSER.haltung(
            # FIELDS TO MAP TO ABWASSER.haltung
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "haltung"),
            # --- abwassernetzelement ---
            **wastewater_networkelement_common(row),
            # --- haltung ---

            #-- attribute 3D ---            #hoehenbestimmung=get_vl(row.elevation_determination__REL),
            innenschutz=get_vl(row.inside_coating__REL),
            laengeeffektiv=row.length_effective,
            lagebestimmung=get_vl(row.horizontal_positioning__REL),
            lichte_hoehe=row.clear_height,
            material=get_vl(row.material__REL),
            nachhaltungspunktref=get_tid(row.fk_reach_point_to__REL),
            plangefaelle=row.slope_building_plan,
            reibungsbeiwert=row.coefficient_of_friction,
            reliner_art=get_vl(row.relining_kind__REL),
            reliner_bautechnik=get_vl(row.relining_construction__REL),
            reliner_material=get_vl(row.reliner_material__REL),
            reliner_nennweite=row.reliner_nominal_size,
            ringsteifigkeit=row.ring_stiffness,
            rohrprofilref=get_tid(row.fk_pipe_profile__REL),
            verlauf=ST_Force2D(row.progression_geometry),
            #-- attribute 3D ---
            #verlauf3d=row.progression3d,
            vonhaltungspunktref=get_tid(row.fk_reach_point_from__REL),
            wandrauhigkeit=row.wall_roughness,

        )
        abwasser_session.add(haltung)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.dryweather_downspout -> ABWASSER.trockenwetterfallrohr, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.dryweather_downspout)
    if filtered:
        query = query.join(QGEP.wastewater_structure, QGEP.wastewater_networkelement).filter(
            QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        )
    for row in query:
        # AVAILABLE FIELDS IN QGEP.dryweather_downspout
        
        # --- structure_part ---
        # to do attributeslist of superclass
        # --- dryweather_downspout ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden
        
        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,
        
        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,
        
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
        query = query.join(QGEP.wastewater_structure, QGEP.wastewater_networkelement).filter(
            QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        )
    for row in query:
        # AVAILABLE FIELDS IN QGEP.access_aid
        
        # --- structure_part ---
        # to do attributeslist of superclass
        # --- access_aid ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden
        
        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,
        
        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,
        
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

    logger.info("Exporting QGEP.dryweather_flume -> ABWASSER.trockenwetterrinne, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.dryweather_flume)
    if filtered:
        query = query.join(QGEP.wastewater_structure, QGEP.wastewater_networkelement).filter(
            QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        )
    for row in query:
        # AVAILABLE FIELDS IN QGEP.dryweather_flume
        
        # --- structure_part ---
        # to do attributeslist of superclass
        # --- dryweather_flume ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden
        
        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,
        
        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,
        
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
        query = query.join(QGEP.wastewater_structure, QGEP.wastewater_networkelement).filter(
            QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        )
    for row in query:
        # AVAILABLE FIELDS IN QGEP.cover
        
        # --- structure_part ---
        # to do attributeslist of superclass
        # --- cover ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden
        
        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,
        
        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,
        
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
            #-- attribute 3D ---
            #maechtigkeit=row.depth,
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
        query = query.join(QGEP.wastewater_structure, QGEP.wastewater_networkelement).filter(
            QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        )
    for row in query:
        # AVAILABLE FIELDS IN QGEP.benching
        
        # --- structure_part ---
        # to do attributeslist of superclass
        # --- benching ---
        # to do attributeslist of subclass
        # to do extra funktion schreiben wo alle englischen attribute erzeugt werden
        
        # --- _bwrel_ ---
        # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,
        
        # --- _rel_ ---
        # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,
        
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



    abwasser_session.commit()

    qgep_session.close()
    abwasser_session.close()
