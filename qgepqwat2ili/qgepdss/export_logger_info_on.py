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
    # 18.7.22 mit logger für debugging
    qgep_session = Session(utils.sqlalchemy.create_engine(logger_name="qgep"), autocommit=False, autoflush=False)
    abwasser_session = Session(utils.sqlalchemy.create_engine(logger_name="abwasser"), autocommit=False, autoflush=False)
    
    # temporär auskommentiert
    #qgep_session = Session(utils.sqlalchemy.create_engine(), autocommit=False, autoflush=False)
    #abwasser_session = Session(utils.sqlalchemy.create_engine(), autocommit=False, autoflush=False)
    
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


    def organisation_common(row):
        """
        Returns common attributes for organisation
        """
        return {
            "bemerkung": truncate(emptystr_to_null(row.remark), 80),
            "bezeichnung": row.identifier,
            "gemeindenummer": row.municipality_number,
            # not supported in model qgep 2015
            # "teil_vonref": get_tid(row.fk_part_of__REL),
            "auid": row.uid,
        }
    def surface_water_bodies_common(row):
        """
        Returns common attributes for surface_water_bodies
        """
        return {
            "bemerkung": truncate(emptystr_to_null(row.remark), 80),
            "bezeichnung": null_to_emptystr(row.identifier),
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
    def maintenance_event_common(row):
        """
        Returns common attributes for maintenance_event
        """
        return {
            "ausfuehrende_firmaref": get_tid(row.fk_operating_company__REL),
            "massnahmeref": get_tid(row.fk_measure__REL),
        }
    def zone_common(row):
        """
        Returns common attributes for zone
        """
        return {
            "bemerkung": truncate(emptystr_to_null(row.remark), 80),
            "bezeichnung": null_to_emptystr(row.identifier),
        }
    def water_control_structure_common(row):
        """
        Returns common attributes for water_control_structure
        """
        return {
            "bemerkung": truncate(emptystr_to_null(row.remark), 80),
            "bezeichnung": null_to_emptystr(row.identifier),
            "gewaesserabschnittref": get_tid(row.fk_water_course_segment__REL),
            "lage": ST_Force2D(row.situation_geometry),
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
    def connection_object_common(row):
        """
        Returns common attributes for connection_object
        """
        return {
            "abwassernetzelementref": get_tid(row.fk_wastewater_networkelement__REL),
            "bemerkung": truncate(emptystr_to_null(row.remark), 80),
            "betreiberref": get_tid(row.fk_operator__REL),
            "bezeichnung": null_to_emptystr(row.identifier),
            "eigentuemerref": get_tid(row.fk_owner__REL),
            "fremdwasseranfall": row.sewer_infiltration_water_production,
        }
    def surface_runoff_parameters_common(row):
        """
        Returns common attributes for surface_runoff_parameters
        """
        return {
            "bemerkung": truncate(emptystr_to_null(row.remark), 80),
            "benetzungsverlust": row.wetting_loss,
            "bezeichnung": null_to_emptystr(row.identifier),
            "einzugsgebietref": get_tid(row.fk_catchment_area__REL),
            "muldenverlust": row.surface_storage,
            "verdunstungsverlust": row.evaporation_loss,
            "versickerungsverlust": row.infiltration_loss,
        }
    def overflow_common(row):
        """
        Returns common attributes for overflow
        """
        return {
            "abwasserknotenref": get_tid(row.fk_wastewater_node__REL),
            "antrieb": get_vl(row.actuation__REL),
            "bemerkung": truncate(emptystr_to_null(row.remark), 80),
            "bezeichnung": null_to_emptystr(row.identifier),
            "bruttokosten": row.gross_costs,
            "einleitstelle": null_to_emptystr(row.discharge_point),
            "fabrikat": row.brand,
            "funktion": get_vl(row.function__REL),
            "qan_dim": row.qon_dim,
            "signaluebermittlung": get_vl(row.signal_transmission__REL),
            "steuerung": get_vl(row.control__REL),
            "steuerungszentraleref": get_tid(row.fk_control_center__REL),
            "subventionen": row.subsidies,
            "ueberlaufcharakteristikref": get_tid(row.fk_overflow_char__REL),
            "ueberlaufnachref": get_tid(row.fk_overflow_to__REL),
            "verstellbarkeit": get_vl(row.adjustability__REL),
        }

    logger.info("Exporting QGEP.maintenance_event_wastewater_structure -> ABWASSER.erhaltungsereignis_abwasserbauwerk, ABWASSER.metaattribute")
    #18.7.2022query = qgep_session.query(QGEP.maintenance_event_wastewater_structure)
    query = qgep_session.query(QGEP.re_maintenance_event_wastewater_structure)
    for row in query:

        # AVAILABLE FIELDS IN QGEP.maintenance_event_wastewater_structure
        
        # --- maintenance_event_wastewater_structure ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile
        
        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL
    
        erhaltungsereignis_abwasserbauwerk = ABWASSER.erhaltungsereignis_abwasserbauwerk(
            # FIELDS TO MAP TO ABWASSER.erhaltungsereignis_abwasserbauwerk
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "erhaltungsereignis_abwasserbauwerk"),
            # --- erhaltungsereignis_abwasserbauwerk ---

            abwasserbauwerkref=get_tid(row.fk_wastewater_structure__REL),
            erhaltungsereignisref=get_tid(row.fk_maintenance_event__REL),
        )
        abwasser_session.add(erhaltungsereignis_abwasserbauwerk)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.mutation -> ABWASSER.mutation, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.mutation)
    for row in query:

        # AVAILABLE FIELDS IN QGEP.mutation
        
        # --- mutation ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile
        
        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL
    
        mutation = ABWASSER.mutation(
            # FIELDS TO MAP TO ABWASSER.mutation
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "mutation"),
            # --- mutation ---

            art=get_vl(row.kind__REL),
            attribut=row.attribute,
            aufnahmedatum=row.date_time,
            aufnehmer=row.recorded_by,
            bemerkung=truncate(emptystr_to_null(row.remark), 80),
            #klasse=row.class,
            letzter_wert=row.last_value,
            mutationsdatum=row.date_mutation,
            objekt=null_to_emptystr(row.object),
            systembenutzer=null_to_emptystr(row.system_user),
        )
        abwasser_session.add(mutation)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.aquifier -> ABWASSER.grundwasserleiter, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.aquifier)
    for row in query:

        # AVAILABLE FIELDS IN QGEP.aquifier
        
        # --- aquifier ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile
        
        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL
    
        grundwasserleiter = ABWASSER.grundwasserleiter(
            # FIELDS TO MAP TO ABWASSER.grundwasserleiter
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "grundwasserleiter"),
            # --- grundwasserleiter ---

            bemerkung=truncate(emptystr_to_null(row.remark), 80),
            bezeichnung=null_to_emptystr(row.identifier),
            maxgwspiegel=row.maximal_groundwater_level,
            mingwspiegel=row.minimal_groundwater_level,
            mittlerergwspiegel=row.average_groundwater_level,
            perimeter=ST_Force2D(row.perimeter_geometry),
        )
        abwasser_session.add(grundwasserleiter)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.river -> ABWASSER.fliessgewaesser, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.river)
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
        
        fliessgewaesser = ABWASSER.fliessgewaesser(
            # FIELDS TO MAP TO ABWASSER.fliessgewaesser
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "fliessgewaesser"),
            # --- oberflaechengewaesser ---
            **surface_water_bodies_common(row),
            # --- fliessgewaesser ---

            art=get_vl(row.kind__REL),

        )
        abwasser_session.add(fliessgewaesser)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.lake -> ABWASSER.see, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.lake)
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
        
        see = ABWASSER.see(
            # FIELDS TO MAP TO ABWASSER.see
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "see"),
            # --- oberflaechengewaesser ---
            **surface_water_bodies_common(row),
            # --- see ---

            perimeter=ST_Force2D(row.perimeter_geometry),

        )
        abwasser_session.add(see)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.water_course_segment -> ABWASSER.gewaesserabschnitt, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.water_course_segment)
    for row in query:

        # AVAILABLE FIELDS IN QGEP.water_course_segment
        
        # --- water_course_segment ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile
        
        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL
    
        gewaesserabschnitt = ABWASSER.gewaesserabschnitt(
            # FIELDS TO MAP TO ABWASSER.gewaesserabschnitt
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "gewaesserabschnitt"),
            # --- gewaesserabschnitt ---

            abflussregime=get_vl(row.discharge_regime__REL),
            algenbewuchs=get_vl(row.algae_growth__REL),
            art=get_vl(row.kind__REL),
            bemerkung=truncate(emptystr_to_null(row.remark), 80),
            bezeichnung=null_to_emptystr(row.identifier),
            bis=ST_Force2D(row.to_geometry),
            breitenvariabilitaet=get_vl(row.width_variability__REL),
            fliessgewaesserref=get_tid(row.fk_watercourse__REL),
            gefaelle=get_vl(row.slope__REL),
            groesse=row.size,
            hoehenstufe=get_vl(row.altitudinal_zone__REL),
            laengsprofil=get_vl(row.length_profile__REL),
            linienfuehrung=get_vl(row.section_morphology__REL),
            makrophytenbewuchs=get_vl(row.macrophyte_coverage__REL),
            nutzung=get_vl(row.utilisation__REL),
            oekom_klassifizierung=get_vl(row.ecom_classification__REL),
            sohlenbreite=row.bed_with,
            tiefenvariabilitaet=get_vl(row.depth_variability__REL),
            totholz=get_vl(row.dead_wood__REL),
            von=ST_Force2D(row.from_geometry),
            wasserhaerte=get_vl(row.water_hardness__REL),
        )
        abwasser_session.add(gewaesserabschnitt)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.water_catchment -> ABWASSER.wasserfassung, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.water_catchment)
    for row in query:

        # AVAILABLE FIELDS IN QGEP.water_catchment
        
        # --- water_catchment ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile
        
        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL
    
        wasserfassung = ABWASSER.wasserfassung(
            # FIELDS TO MAP TO ABWASSER.wasserfassung
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "wasserfassung"),
            # --- wasserfassung ---

            art=get_vl(row.kind__REL),
            bemerkung=truncate(emptystr_to_null(row.remark), 80),
            bezeichnung=null_to_emptystr(row.identifier),
            grundwasserleiterref=get_tid(row.fk_aquifier__REL),
            lage=ST_Force2D(row.situation_geometry),
            oberflaechengewaesserref=get_tid(row.fk_surface_water_body__REL),
        )
        abwasser_session.add(wasserfassung)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.river_bank -> ABWASSER.ufer, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.river_bank)
    for row in query:

        # AVAILABLE FIELDS IN QGEP.river_bank
        
        # --- river_bank ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile
        
        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL
    
        ufer = ABWASSER.ufer(
            # FIELDS TO MAP TO ABWASSER.ufer
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "ufer"),
            # --- ufer ---

            bemerkung=truncate(emptystr_to_null(row.remark), 80),
            bezeichnung=null_to_emptystr(row.identifier),
            breite=row.width,
            gewaesserabschnittref=get_tid(row.fk_water_course_segment__REL),
            seite=get_vl(row.side__REL),
            uferbereich=get_vl(row.shores__REL),
            umlandnutzung=get_vl(row.utilisation_of_shore_surroundings__REL),
            vegetation=get_vl(row.vegetation__REL),
            verbauungsart=get_vl(row.river_control_type__REL),
            verbauungsgrad=get_vl(row.control_grade_of_river__REL),
        )
        abwasser_session.add(ufer)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.river_bed -> ABWASSER.gewaessersohle, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.river_bed)
    for row in query:

        # AVAILABLE FIELDS IN QGEP.river_bed
        
        # --- river_bed ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile
        
        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL
    
        gewaessersohle = ABWASSER.gewaessersohle(
            # FIELDS TO MAP TO ABWASSER.gewaessersohle
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "gewaessersohle"),
            # --- gewaessersohle ---

            art=get_vl(row.kind__REL),
            bemerkung=truncate(emptystr_to_null(row.remark), 80),
            bezeichnung=null_to_emptystr(row.identifier),
            breite=row.width,
            gewaesserabschnittref=get_tid(row.fk_water_course_segment__REL),
            verbauungsart=get_vl(row.river_control_type__REL),
            verbauungsgrad=get_vl(row.control_grade_of_river__REL),
        )
        abwasser_session.add(gewaessersohle)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.sector_water_body -> ABWASSER.gewaessersektor, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.sector_water_body)
    for row in query:

        # AVAILABLE FIELDS IN QGEP.sector_water_body
        
        # --- sector_water_body ---
        # to do e.g. fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark
        # --- _bwrel_ ---
        # to do add superclassrelations e.g. profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile
        
        # --- _rel_ ---
        # to do add relations fk_dataowner__REL, fk_provider__REL, profile_type__REL
    
        gewaessersektor = ABWASSER.gewaessersektor(
            # FIELDS TO MAP TO ABWASSER.gewaessersektor
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "gewaessersektor"),
            # --- gewaessersektor ---

            art=get_vl(row.kind__REL),
            bemerkung=truncate(emptystr_to_null(row.remark), 80),
            bezeichnung=null_to_emptystr(row.identifier),
            bwg_code=row.code_bwg,
            kilomo=row.km_down,
            kilomu=row.km_up,
            oberflaechengewaesserref=get_tid(row.fk_surface_water_body__REL),
            reflaenge=row.ref_length,
            verlauf=ST_Force2D(row.progression_geometry),
            vorherigersektorref=get_tid(row.fk_sector_previous__REL),
        )
        abwasser_session.add(gewaessersektor)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.administrative_office -> ABWASSER.amt, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.administrative_office)
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
        
        amt = ABWASSER.amt(
            # FIELDS TO MAP TO ABWASSER.amt
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "amt"),
            # --- organisation ---
            **organisation_common(row),
            # --- amt ---


        )
        abwasser_session.add(amt)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.cooperative -> ABWASSER.genossenschaft_korporation, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.cooperative)
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
        
        genossenschaft_korporation = ABWASSER.genossenschaft_korporation(
            # FIELDS TO MAP TO ABWASSER.genossenschaft_korporation
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "genossenschaft_korporation"),
            # --- organisation ---
            **organisation_common(row),
            # --- genossenschaft_korporation ---


        )
        abwasser_session.add(genossenschaft_korporation)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.canton -> ABWASSER.kanton, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.canton)
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
        
        kanton = ABWASSER.kanton(
            # FIELDS TO MAP TO ABWASSER.kanton
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "kanton"),
            # --- organisation ---
            **organisation_common(row),
            # --- kanton ---

            perimeter=ST_Force2D(row.perimeter_geometry),

        )
        abwasser_session.add(kanton)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.waste_water_association -> ABWASSER.abwasserverband, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.waste_water_association)
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
        
        abwasserverband = ABWASSER.abwasserverband(
            # FIELDS TO MAP TO ABWASSER.abwasserverband
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "abwasserverband"),
            # --- organisation ---
            **organisation_common(row),
            # --- abwasserverband ---


        )
        abwasser_session.add(abwasserverband)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.municipality -> ABWASSER.gemeinde, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.municipality)
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
        
        gemeinde = ABWASSER.gemeinde(
            # FIELDS TO MAP TO ABWASSER.gemeinde
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "gemeinde"),
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
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.waste_water_treatment_plant -> ABWASSER.abwasserreinigungsanlage, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.waste_water_treatment_plant)
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
        
        abwasserreinigungsanlage = ABWASSER.abwasserreinigungsanlage(
            # FIELDS TO MAP TO ABWASSER.abwasserreinigungsanlage
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "abwasserreinigungsanlage"),
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
        create_metaattributes(row)
        print(".", end="")
    logger.info("done")
    abwasser_session.flush()

    logger.info("Exporting QGEP.private -> ABWASSER.privat, ABWASSER.metaattribute")
    query = qgep_session.query(QGEP.private)
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
        
        privat = ABWASSER.privat(
            # FIELDS TO MAP TO ABWASSER.privat
            # --- baseclass ---
            # --- sia405_baseclass ---
            **base_common(row, "privat"),
            # --- organisation ---
            **organisation_common(row),
            # --- privat ---

            art=row.kind,

        )
        abwasser_session.add(privat)
        create_metaattributes(row)
        print(".", end="")
    logger.info("done privat")
    abwasser_session.flush()

    #abwasser_session.commit()
    #abwasser_session.close()
    # breakpoint()
    


    # logger.info("Exporting QGEP.channel -> ABWASSER.kanal, ABWASSER.metaattribute")
    # query = qgep_session.query(QGEP.channel)
    # if filtered:
        # query = query.join(QGEP.wastewater_networkelement).filter(
            # QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        # )
    # for row in query:
        # # AVAILABLE FIELDS IN QGEP.channel
        
        # # --- wastewater_structure ---
        # # to do attributeslist of superclass
        # # --- channel ---
        # # to do attributeslist of subclass
        # # to do extra funktion schreiben wo alle englischen attribute erzeugt werden
        
        # # --- _bwrel_ ---
        # # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,
        
        # # --- _rel_ ---
        # # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,
        
        # kanal = ABWASSER.kanal(
            # # FIELDS TO MAP TO ABWASSER.kanal
            # # --- baseclass ---
            # # --- sia405_baseclass ---
            # **base_common(row, "kanal"),
            # # --- abwasserbauwerk ---
            # **wastewater_structure_common(row),
            # # --- kanal ---

            # bettung_umhuellung=get_vl(row.bedding_encasement__REL),
            # funktionhierarchisch=get_vl(row.function_hierarchic__REL),
            # funktionhydraulisch=get_vl(row.function_hydraulic__REL),
            # nutzungsart_geplant=get_vl(row.usage_planned__REL),
            # nutzungsart_ist=get_vl(row.usage_current__REL),
            # rohrlaenge=row.pipe_length,
            # spuelintervall=row.jetting_interval,
            # verbindungsart=get_vl(row.connection_type__REL),

        # )
        # abwasser_session.add(kanal)
        # create_metaattributes(row)
        # print(".", end="")
    # logger.info("done")
    # abwasser_session.flush()

    # logger.info("Exporting QGEP.manhole -> ABWASSER.normschacht, ABWASSER.metaattribute")
    # query = qgep_session.query(QGEP.manhole)
    # if filtered:
        # query = query.join(QGEP.wastewater_networkelement).filter(
            # QGEP.wastewater_networkelement.obj_id.in_(subset_ids)
        # )
    # for row in query:
        # # AVAILABLE FIELDS IN QGEP.manhole
        
        # # --- wastewater_structure ---
        # # to do attributeslist of superclass
        # # --- manhole ---
        # # to do attributeslist of subclass
        # # to do extra funktion schreiben wo alle englischen attribute erzeugt werden
        
        # # --- _bwrel_ ---
        # # to do extra funktion schreiben wo alle fk auf diese superklasse erzeugt werden z.B. # measuring_point__BWREL_fk_wastewater_structure,
        
        # # --- _rel_ ---
        # # to do extra funktion schreiben wo alle fk auf diese klasse erzeugt werden z.B. # accessibility__REL, bedding_encasement__REL,
        
        # normschacht = ABWASSER.normschacht(
            # # FIELDS TO MAP TO ABWASSER.normschacht
            # # --- baseclass ---
            # # --- sia405_baseclass ---
            # **base_common(row, "normschacht"),
            # # --- abwasserbauwerk ---
            # **wastewater_structure_common(row),
            # # --- normschacht ---

            # dimension1=row.dimension1,
            # dimension2=row.dimension2,
            # funktion=get_vl(row.function__REL),
            # #-- attribute 3D ---
            # #maechtigkeit=row.depth,
            # material=get_vl(row.material__REL),
            # oberflaechenzulauf=get_vl(row.surface_inflow__REL),

        # )
        # abwasser_session.add(normschacht)
        # create_metaattributes(row)
        # print(".", end="")
    # logger.info("done")
    # abwasser_session.flush()

 

    abwasser_session.commit()
    #abwasser_session2.commit()

    qgep_session.close()
    abwasser_session.close()
    #abwasser_session2.close()