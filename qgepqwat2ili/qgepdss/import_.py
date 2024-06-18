# Definitions for qgep datamodel with delta >= 1.5.91

from functools import lru_cache

from geoalchemy2.functions import ST_Force3D
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_dirty

from .. import utils
from ..utils.various import logger
from .model_abwasser import get_abwasser_model
from .model_qgep import get_qgep_model


def qgep_import(precommit_callback=None):
    """
    Imports data from the ili2pg model into the QGEP model.

    Args:
        precommit_callback: optional callable that gets invoked with the sqlalchemy's session,
                            allowing for a GUI to  filter objects before committing. It MUST either
                            commit or rollback and close the session.
    """

    QGEP = get_qgep_model()
    ABWASSER = get_abwasser_model()

    pre_session = Session(utils.sqlalchemy.create_engine(), autocommit=False, autoflush=False)

    # We also drop symbology triggers as they badly affect performance. This must be done in a separate session as it
    # would deadlock other sessions.
    logger.info("drop symbology triggers")
    pre_session.execute("SELECT qgep_sys.drop_symbology_triggers();")
    pre_session.commit()
    pre_session.close()

    # We use two different sessions for reading and writing so it's easier to
    # review imports and to keep the door open to getting data from another
    # connection / database type.
    abwasser_session = Session(utils.sqlalchemy.create_engine(), autocommit=False, autoflush=False)
    qgep_session = Session(utils.sqlalchemy.create_engine(), autocommit=False, autoflush=False)



    # Allow to insert rows with cyclic dependencies at once, needs data modell version 1.6.2 https://github.com/QGEP/datamodel/pull/235 to work properly
    logger.info("SET CONSTRAINTS ALL DEFERRED;")
    qgep_session.execute("SET CONSTRAINTS ALL DEFERRED;")



    def get_vl_instance(vl_table, value):
        """
        Gets a value list instance from the value_de name. Returns None and a warning if not found.
        """
        # TODO : memoize (and get the whole table at once) to improve N+1 performance issue
        # TODO : return "other" (or other applicable value) rather than None, or even throwing an exception, would probably be better
        row = qgep_session.query(vl_table).filter(vl_table.value_de == value).first()
        if row is None:
            # write logger.warning only if value is not None
            if value != None:
                logger.warning(
                    f'Could not find value `{value}` in value list "{vl_table.__table__.schema}.{vl_table.__name__}". Setting to None instead.'
                )
            return None
        return row

    def get_pk(relation):
        """
        Returns the primary key for a relation
        """
        if relation is None:
            return None
        return relation.obj_id

    def create_or_update(cls, **kwargs):
        """
        Updates an existing instance (if obj_id is found) or creates an instance of the provided class
        with given kwargs, and returns it.
        """
        instance = None

        # We try to get the instance from the session/database
        obj_id = kwargs.get("obj_id", None)
        if obj_id:
            instance = qgep_session.query(cls).get(kwargs.get("obj_id", None))

        if instance:
            # We found it -> update
            instance.__dict__.update(kwargs)
            flag_dirty(instance)  # we flag it as dirty so it stays in the session
        else:
            # We didn't find it -> create
            instance = cls(**kwargs)

        return instance

    @lru_cache(maxsize=None)
    def create_or_update_organisation(name):
        """
        Gets an organisation ID from it's name (and creates an entry if not existing)
        """
        if not name:
            return None

        instance = qgep_session.query(QGEP.organisation).filter(QGEP.organisation.identifier == name).first()

        # also look for non-flushed objects in the session
        if not instance:
            for obj in qgep_session:
                # for VSA-DSS look in subclasses instead of organisation
                #if obj.__class__ is QGEP.organisation and obj.identifier == name:
                #    instance = obj
                #    break
                if obj.__class__ is QGEP.municipality and obj.identifier == name:
                    instance = obj
                    break
                if obj.__class__ is QGEP.administrative_office and obj.identifier == name:
                    instance = obj
                    break
                if obj.__class__ is QGEP.canton and obj.identifier == name:
                    instance = obj
                    break
                if obj.__class__ is QGEP.cooperative and obj.identifier == name:
                    instance = obj
                    break
                if obj.__class__ is QGEP.private and obj.identifier == name:
                    instance = obj
                    break
                if obj.__class__ is QGEP.waste_water_association and obj.identifier == name:
                    instance = obj
                    break

        # if still nothing, we create it
        if not instance:
            # 10.8.2022 extra logger info added
            logger.info("Adding additional organisation (administrative_office) from name (please check after import if subclass is correct):" + name)
            
#           instance = create_or_update(QGEP.organisation, identifier=name)
#            qgep_session.add(instance)

            # 11.8.2022 v13 nur kurzer aufruf statt lang
            # 10.8.2022 for VSA-DSS for an organisation a subclass has to be defined - create as administrative_office (Amt), as we cannot know from the name what it is - can be discussed and changed if needed.
            instance = create_or_update(QGEP.administrative_office, identifier=name)

# has to have more parameters ? recursion error 
# 11.8.2022 v6 parameter identifier=name am Schluss eingefügt
            # instance = administrative_office = create_or_update(
                # QGEP.administrative_office, 
                # **base_common(row),
                # # 11.8.2022 v7 commented out as in qgep template from Olivier for class organisation
                # # 11.8.2022 v11 reingenommen / v12 wieder raus - geht nicht mit grossen datensätzen rekursiv error
                # #**metaattribute_common(metaattribute),  # TODO : currently this fails because organisations are not created yet
                # # --- organisation ---
                # **organisation_common(row),
                # # --- administrative_office ---
                # # 11.8.2022 v7 gelöscht identifier=name rausgenommen
                # # 11.8.2022 v10 wieder erstellt, damit zusätzliche organisation erstellt wird
                # identifier=name
            # )
            qgep_session.add(instance)
        return instance

    def metaattribute_common(metaattribute):
        """
        Common parameters for metaattributes
        """
        return {
            "fk_dataowner__REL": create_or_update_organisation(metaattribute.datenherr),
            "fk_provider__REL": create_or_update_organisation(metaattribute.datenlieferant),
            "last_modification": metaattribute.letzte_aenderung,
        }

    def base_common(row):
        """
        Returns common attributes for base
        """
        return {
            "obj_id": row.obj_id,
        }

    def organisation_common(row):
        """
        Returns common attributes for organisation
        """
        return {
            # not supported in qgep datamodel yet, reference on same class
            #"fk_part_of": get_pk(row.teil_vonref__REL),
            "identifier": row.bezeichnung,
            #not part of qgep datamodel, only for release 2020
            #"municipality_number": row.gemeindenummer,
            "remark": row.bemerkung,
            "uid": row.auid,
        }
    def surface_water_bodies_common(row):
        """
        Returns common attributes for surface_water_bodies
        """
        return {
            "identifier": row.bezeichnung,
            "remark": row.bemerkung,
        }
    def wastewater_structure_common(row):
        """
        Returns common attributes for wastewater_structure
        """
        return {
            "accessibility__REL": get_vl_instance(
                QGEP.wastewater_structure_accessibility, row.zugaenglichkeit
            ),
            "contract_section": row.baulos,
            "detail_geometry_geometry": ST_Force3D(row.detailgeometrie),
            #-- attribute 3D ---
            #"detail_geometry3d_geometry": ST_Force3D(row.detailgeometrie3d),
            "financing__REL": get_vl_instance(
                QGEP.wastewater_structure_financing, row.finanzierung
            ),
            "fk_operator": get_pk(row.betreiberref__REL),
            "fk_owner": get_pk(row.eigentuemerref__REL),
            "gross_costs": row.bruttokosten,
            "identifier": row.bezeichnung,
            "inspection_interval": row.inspektionsintervall,
            "location_name": row.standortname,
            "records": row.akten,
            "remark": row.bemerkung,
            "renovation_necessity__REL": get_vl_instance(
                QGEP.wastewater_structure_renovation_necessity, row.sanierungsbedarf
            ),
            "replacement_value": row.wiederbeschaffungswert,
            "rv_base_year": row.wbw_basisjahr,
            "rv_construction_type__REL": get_vl_instance(
                QGEP.wastewater_structure_rv_construction_type, row.wbw_bauart
            ),
            "status__REL": get_vl_instance(
                QGEP.wastewater_structure_status, row.astatus
            ),
            "structure_condition__REL": get_vl_instance(
                QGEP.wastewater_structure_structure_condition, row.baulicherzustand
            ),
            "subsidies": row.subventionen,
            "year_of_construction": row.baujahr,
            "year_of_replacement": row.ersatzjahr,
        }
    def maintenance_event_common(row):
        """
        Returns common attributes for maintenance_event
        """
        return {
            "fk_measure": get_pk(row.massnahmeref__REL),
            "fk_operating_company": get_pk(row.ausfuehrende_firmaref__REL),
        }
    def zone_common(row):
        """
        Returns common attributes for zone
        """
        return {
            "identifier": row.bezeichnung,
            "remark": row.bemerkung,
        }
    def water_control_structure_common(row):
        """
        Returns common attributes for water_control_structure
        """
        return {
            "fk_water_course_segment": get_pk(row.gewaesserabschnittref__REL),
            "identifier": row.bezeichnung,
            "remark": row.bemerkung,
            "situation_geometry": (row.lage),
        }
    def wastewater_networkelement_common(row):
        """
        Returns common attributes for wastewater_networkelement
        """
        return {
            "fk_wastewater_structure": get_pk(row.abwasserbauwerkref__REL),
            "identifier": row.bezeichnung,
            "remark": row.bemerkung,
        }
    def structure_part_common(row):
        """
        Returns common attributes for structure_part
        """
        return {
            "fk_wastewater_structure": get_pk(row.abwasserbauwerkref__REL),
            "identifier": row.bezeichnung,
            "remark": row.bemerkung,
            "renovation_demand__REL": get_vl_instance(
                QGEP.structure_part_renovation_demand, row.instandstellung
            ),
        }
    def connection_object_common(row):
        """
        Returns common attributes for connection_object
        """
        return {
            "fk_operator": get_pk(row.betreiberref__REL),
            "fk_owner": get_pk(row.eigentuemerref__REL),
            "fk_wastewater_networkelement": get_pk(row.abwassernetzelementref__REL),
            "identifier": row.bezeichnung,
            "remark": row.bemerkung,
            "sewer_infiltration_water_production": row.fremdwasseranfall,
        }
    def surface_runoff_parameters_common(row):
        """
        Returns common attributes for surface_runoff_parameters
        """
        return {
            "evaporation_loss": row.verdunstungsverlust,
            "fk_catchment_area": get_pk(row.einzugsgebietref__REL),
            "identifier": row.bezeichnung,
            "infiltration_loss": row.versickerungsverlust,
            "remark": row.bemerkung,
            "surface_storage": row.muldenverlust,
            "wetting_loss": row.benetzungsverlust,
        }
    def overflow_common(row):
        """
        Returns common attributes for overflow
        """
        return {
            "actuation__REL": get_vl_instance(
                QGEP.overflow_actuation, row.antrieb
            ),
            "adjustability__REL": get_vl_instance(
                QGEP.overflow_adjustability, row.verstellbarkeit
            ),
            "brand": row.fabrikat,
            "control__REL": get_vl_instance(
                QGEP.overflow_control, row.steuerung
            ),
            "discharge_point": row.einleitstelle,
            "fk_control_center": get_pk(row.steuerungszentraleref__REL),
            "fk_overflow_char": get_pk(row.ueberlaufcharakteristikref__REL),
            "fk_overflow_to": get_pk(row.ueberlaufnachref__REL),
            "fk_wastewater_node": get_pk(row.abwasserknotenref__REL),
            "function__REL": get_vl_instance(
                QGEP.overflow_function, row.funktion
            ),
            "gross_costs": row.bruttokosten,
            "identifier": row.bezeichnung,
            "qon_dim": row.qan_dim,
            "remark": row.bemerkung,
            "signal_transmission__REL": get_vl_instance(
                QGEP.overflow_signal_transmission, row.signaluebermittlung
            ),
            "subsidies": row.subventionen,
        }

    logger.info("Importing ABWASSER.mutation, ABWASSER.metaattribute -> QGEP.mutation")
    for row, metaattribute in abwasser_session.query(ABWASSER.mutation, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        mutation = create_or_update(
            QGEP.mutation,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- mutation ---
            attribute=row.attribut,
            classname=row.klasse,
            date_mutation=row.mutationsdatum,
            date_time=row.aufnahmedatum,
            kind__REL=get_vl_instance(
                QGEP.mutation_kind, row.art
            ),
            last_value=row.letzter_wert,
            object=row.objekt,
            recorded_by=row.aufnehmer,
            remark=row.bemerkung,
            system_user=row.systembenutzer,
        )
        qgep_session.add(mutation)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.grundwasserleiter, ABWASSER.metaattribute -> QGEP.aquifier")
    for row, metaattribute in abwasser_session.query(ABWASSER.grundwasserleiter, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        aquifier = create_or_update(
            QGEP.aquifier,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- aquifier ---
            average_groundwater_level=row.mittlerergwspiegel,
            identifier=row.bezeichnung,
            maximal_groundwater_level=row.maxgwspiegel,
            minimal_groundwater_level=row.mingwspiegel,
            perimeter_geometry=(row.perimeter),
            remark=row.bemerkung,
        )
        qgep_session.add(aquifier)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.fliessgewaesser, ABWASSER.metaattribute -> QGEP.river")
    for row, metaattribute in abwasser_session.query(ABWASSER.fliessgewaesser, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN fliessgewaesser

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- fliessgewaesser ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        river = create_or_update(
            QGEP.river,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- surface_water_bodies ---
            **surface_water_bodies_common(row),
            # --- river ---
            kind__REL=get_vl_instance(
                QGEP.river_kind, row.art
            ),

        )
        qgep_session.add(river)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.see, ABWASSER.metaattribute -> QGEP.lake")
    for row, metaattribute in abwasser_session.query(ABWASSER.see, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN see

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- see ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        lake = create_or_update(
            QGEP.lake,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- surface_water_bodies ---
            **surface_water_bodies_common(row),
            # --- lake ---
            perimeter_geometry=(row.perimeter),

        )
        qgep_session.add(lake)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.gewaesserabschnitt, ABWASSER.metaattribute -> QGEP.water_course_segment")
    for row, metaattribute in abwasser_session.query(ABWASSER.gewaesserabschnitt, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        water_course_segment = create_or_update(
            QGEP.water_course_segment,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- water_course_segment ---
            algae_growth__REL=get_vl_instance(
                QGEP.water_course_segment_algae_growth, row.algenbewuchs
            ),
            altitudinal_zone__REL=get_vl_instance(
                QGEP.water_course_segment_altitudinal_zone, row.hoehenstufe
            ),
            bed_with=row.sohlenbreite,
            dead_wood__REL=get_vl_instance(
                QGEP.water_course_segment_dead_wood, row.totholz
            ),
            depth_variability__REL=get_vl_instance(
                QGEP.water_course_segment_depth_variability, row.tiefenvariabilitaet
            ),
            discharge_regime__REL=get_vl_instance(
                QGEP.water_course_segment_discharge_regime, row.abflussregime
            ),
            ecom_classification__REL=get_vl_instance(
                QGEP.water_course_segment_ecom_classification, row.oekom_klassifizierung
            ),
            fk_watercourse=get_pk(row.fliessgewaesserref__REL),
            from_geometry=(row.von),
            identifier=row.bezeichnung,
            kind__REL=get_vl_instance(
                QGEP.water_course_segment_kind, row.art
            ),
            length_profile__REL=get_vl_instance(
                QGEP.water_course_segment_length_profile, row.laengsprofil
            ),
            macrophyte_coverage__REL=get_vl_instance(
                QGEP.water_course_segment_macrophyte_coverage, row.makrophytenbewuchs
            ),
            remark=row.bemerkung,
            section_morphology__REL=get_vl_instance(
                QGEP.water_course_segment_section_morphology, row.linienfuehrung
            ),
            size=row.groesse,
            slope__REL=get_vl_instance(
                QGEP.water_course_segment_slope, row.gefaelle
            ),
            to_geometry=(row.bis),
            utilisation__REL=get_vl_instance(
                QGEP.water_course_segment_utilisation, row.nutzung
            ),
            water_hardness__REL=get_vl_instance(
                QGEP.water_course_segment_water_hardness, row.wasserhaerte
            ),
            width_variability__REL=get_vl_instance(
                QGEP.water_course_segment_width_variability, row.breitenvariabilitaet
            ),
        )
        qgep_session.add(water_course_segment)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.wasserfassung, ABWASSER.metaattribute -> QGEP.water_catchment")
    for row, metaattribute in abwasser_session.query(ABWASSER.wasserfassung, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        water_catchment = create_or_update(
            QGEP.water_catchment,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- water_catchment ---
            fk_aquifier=get_pk(row.grundwasserleiterref__REL),
            fk_surface_water_bodies=get_pk(row.oberflaechengewaesserref__REL),
            identifier=row.bezeichnung,
            kind__REL=get_vl_instance(
                QGEP.water_catchment_kind, row.art
            ),
            remark=row.bemerkung,
            situation_geometry=(row.lage),
        )
        qgep_session.add(water_catchment)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.ufer, ABWASSER.metaattribute -> QGEP.river_bank")
    for row, metaattribute in abwasser_session.query(ABWASSER.ufer, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        river_bank = create_or_update(
            QGEP.river_bank,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- river_bank ---
            control_grade_of_river__REL=get_vl_instance(
                QGEP.river_bank_control_grade_of_river, row.verbauungsgrad
            ),
            fk_water_course_segment=get_pk(row.gewaesserabschnittref__REL),
            identifier=row.bezeichnung,
            remark=row.bemerkung,
            river_control_type__REL=get_vl_instance(
                QGEP.river_bank_river_control_type, row.verbauungsart
            ),
            shores__REL=get_vl_instance(
                QGEP.river_bank_shores, row.uferbereich
            ),
            side__REL=get_vl_instance(
                QGEP.river_bank_side, row.seite
            ),
            utilisation_of_shore_surroundings__REL=get_vl_instance(
                QGEP.river_bank_utilisation_of_shore_surroundings, row.umlandnutzung
            ),
            vegetation__REL=get_vl_instance(
                QGEP.river_bank_vegetation, row.vegetation
            ),
            width=row.breite,
        )
        qgep_session.add(river_bank)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.gewaessersohle, ABWASSER.metaattribute -> QGEP.river_bed")
    for row, metaattribute in abwasser_session.query(ABWASSER.gewaessersohle, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        river_bed = create_or_update(
            QGEP.river_bed,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- river_bed ---
            control_grade_of_river__REL=get_vl_instance(
                QGEP.river_bed_control_grade_of_river, row.verbauungsgrad
            ),
            fk_water_course_segment=get_pk(row.gewaesserabschnittref__REL),
            identifier=row.bezeichnung,
            kind__REL=get_vl_instance(
                QGEP.river_bed_kind, row.art
            ),
            remark=row.bemerkung,
            river_control_type__REL=get_vl_instance(
                QGEP.river_bed_river_control_type, row.verbauungsart
            ),
            width=row.breite,
        )
        qgep_session.add(river_bed)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.gewaessersektor, ABWASSER.metaattribute -> QGEP.sector_water_body")
    for row, metaattribute in abwasser_session.query(ABWASSER.gewaessersektor, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        sector_water_body = create_or_update(
            QGEP.sector_water_body,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- sector_water_body ---
            code_bwg=row.bwg_code,
            # not supported in qgep datamodel yet, reference on same class
            #fk_sector_previous=get_pk(row.vorherigersektorref__REL),
            fk_surface_water_bodies=get_pk(row.oberflaechengewaesserref__REL),
            identifier=row.bezeichnung,
            kind__REL=get_vl_instance(
                QGEP.sector_water_body_kind, row.art
            ),
            km_down=row.kilomo,
            km_up=row.kilomu,
            progression_geometry=(row.verlauf),
            ref_length=row.reflaenge,
            remark=row.bemerkung,
        )
        qgep_session.add(sector_water_body)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.amt, ABWASSER.metaattribute -> QGEP.administrative_office")
    for row, metaattribute in abwasser_session.query(ABWASSER.amt, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN amt

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- amt ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        
#10.8.2022 not added because no own attributes? add obj_id to work correctly?
        administrative_office = create_or_update(
            QGEP.administrative_office,
            **base_common(row),
            # 11.8.2022 v8 **metaattribute_common(metaattribute), # TODO : currently this fails because organisations are not created yet
            # --- organisation ---
            **organisation_common(row),
            # --- administrative_office ---
            # test 10.8.2022 / 11.8.2022 v9 wieder gelöscht
#            dummy = "dummy"
        )
        qgep_session.add(administrative_office)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.genossenschaft_korporation, ABWASSER.metaattribute -> QGEP.cooperative")
    for row, metaattribute in abwasser_session.query(ABWASSER.genossenschaft_korporation, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN genossenschaft_korporation

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- genossenschaft_korporation ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        cooperative = create_or_update(
            QGEP.cooperative,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- organisation ---
            **organisation_common(row),
            # --- cooperative ---

        )
        qgep_session.add(cooperative)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.kanton, ABWASSER.metaattribute -> QGEP.canton")
    for row, metaattribute in abwasser_session.query(ABWASSER.kanton, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN kanton

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- kanton ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        canton = create_or_update(
            QGEP.canton,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- organisation ---
            **organisation_common(row),
            # --- canton ---
            perimeter_geometry=(row.perimeter),

        )
        qgep_session.add(canton)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.abwasserverband, ABWASSER.metaattribute -> QGEP.waste_water_association")
    for row, metaattribute in abwasser_session.query(ABWASSER.abwasserverband, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN abwasserverband

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- abwasserverband ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        waste_water_association = create_or_update(
            QGEP.waste_water_association,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- organisation ---
            **organisation_common(row),
            # --- waste_water_association ---

        )
        qgep_session.add(waste_water_association)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.gemeinde, ABWASSER.metaattribute -> QGEP.municipality")
    for row, metaattribute in abwasser_session.query(ABWASSER.gemeinde, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN gemeinde

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- gemeinde ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        municipality = create_or_update(
            QGEP.municipality,
            **base_common(row),
            # 11.8.2022 v8**metaattribute_common(metaattribute), # TODO : currently this fails because organisations are not created yet
            # --- organisation ---
            **organisation_common(row),
            # --- municipality ---
            altitude=row.hoehe,
            gwdp_year=row.gep_jahr,
            municipality_number=row.gemeindenummer,
            perimeter_geometry=(row.perimeter),
            population=row.einwohner,
            total_surface=row.flaeche,

        )
        qgep_session.add(municipality)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.abwasserreinigungsanlage, ABWASSER.metaattribute -> QGEP.waste_water_treatment_plant")
    for row, metaattribute in abwasser_session.query(ABWASSER.abwasserreinigungsanlage, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN abwasserreinigungsanlage

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- abwasserreinigungsanlage ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        waste_water_treatment_plant = create_or_update(
            QGEP.waste_water_treatment_plant,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- organisation ---
            **organisation_common(row),
            # --- waste_water_treatment_plant ---
            bod5=row.bsb5,
            cod=row.csb,
            elimination_cod=row.eliminationcsb,
            elimination_n=row.eliminationn,
            elimination_nh4=row.eliminationnh4,
            elimination_p=row.eliminationp,
            installation_number=row.anlagenummer,
            kind=row.art,
            nh4=row.nh4,
            start_year=row.inbetriebnahme,

        )
        qgep_session.add(waste_water_treatment_plant)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.privat, ABWASSER.metaattribute -> QGEP.private")
    for row, metaattribute in abwasser_session.query(ABWASSER.privat, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN privat

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- privat ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        private = create_or_update(
            QGEP.private,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- organisation ---
            **organisation_common(row),
            # --- private ---
            kind=row.art,

        )
        qgep_session.add(private)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.kanal, ABWASSER.metaattribute -> QGEP.channel")
    for row, metaattribute in abwasser_session.query(ABWASSER.kanal, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN kanal

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- kanal ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        channel = create_or_update(
            QGEP.channel,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- wastewater_structure ---
            **wastewater_structure_common(row),
            # --- channel ---
            bedding_encasement__REL=get_vl_instance(
                QGEP.channel_bedding_encasement, row.bettung_umhuellung
            ),
            connection_type__REL=get_vl_instance(
                QGEP.channel_connection_type, row.verbindungsart
            ),
            function_hierarchic__REL=get_vl_instance(
                QGEP.channel_function_hierarchic, row.funktionhierarchisch
            ),
            function_hydraulic__REL=get_vl_instance(
                QGEP.channel_function_hydraulic, row.funktionhydraulisch
            ),
            jetting_interval=row.spuelintervall,
            pipe_length=row.rohrlaenge,
            usage_current__REL=get_vl_instance(
                QGEP.channel_usage_current, row.nutzungsart_ist
            ),
            usage_planned__REL=get_vl_instance(
                QGEP.channel_usage_planned, row.nutzungsart_geplant
            ),

        )
        qgep_session.add(channel)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.normschacht, ABWASSER.metaattribute -> QGEP.manhole")
    for row, metaattribute in abwasser_session.query(ABWASSER.normschacht, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN normschacht

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- normschacht ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        manhole = create_or_update(
            QGEP.manhole,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- wastewater_structure ---
            **wastewater_structure_common(row),
            # --- manhole ---
            #-- attribute 3D ---
            #depth=row.maechtigkeit,
            dimension1=row.dimension1,
            dimension2=row.dimension2,
            function__REL=get_vl_instance(
                QGEP.manhole_function, row.funktion
            ),
            material__REL=get_vl_instance(
                QGEP.manhole_material, row.material
            ),
            surface_inflow__REL=get_vl_instance(
                QGEP.manhole_surface_inflow, row.oberflaechenzulauf
            ),

        )
        qgep_session.add(manhole)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.einleitstelle, ABWASSER.metaattribute -> QGEP.discharge_point")
    for row, metaattribute in abwasser_session.query(ABWASSER.einleitstelle, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN einleitstelle

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- einleitstelle ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        discharge_point = create_or_update(
            QGEP.discharge_point,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- wastewater_structure ---
            **wastewater_structure_common(row),
            # --- discharge_point ---
            #-- attribute 3D ---
            #depth=row.maechtigkeit,
            fk_sector_water_body=get_pk(
                row.gewaessersektorref__REL
            ),
            highwater_level=row.hochwasserkote,
            relevance__REL=get_vl_instance(
                QGEP.discharge_point_relevance, row.relevanz
            ),
            terrain_level=row.terrainkote,
            #-- attribute 3D ---
            #upper_elevation=row.deckenkote,
            waterlevel_hydraulic=row.wasserspiegel_hydraulik,

        )
        qgep_session.add(discharge_point)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.spezialbauwerk, ABWASSER.metaattribute -> QGEP.special_structure")
    for row, metaattribute in abwasser_session.query(ABWASSER.spezialbauwerk, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN spezialbauwerk

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- spezialbauwerk ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        special_structure = create_or_update(
            QGEP.special_structure,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- wastewater_structure ---
            **wastewater_structure_common(row),
            # --- special_structure ---
            bypass__REL=get_vl_instance(
                QGEP.special_structure_bypass, row.bypass
            ),
            #-- attribute 3D ---
            #depth=row.maechtigkeit,
            emergency_spillway__REL=get_vl_instance(
                QGEP.special_structure_emergency_spillway, row.notueberlauf
            ),
            function__REL=get_vl_instance(
                QGEP.special_structure_function, row.funktion
            ),
            stormwater_tank_arrangement__REL=get_vl_instance(
                QGEP.special_structure_stormwater_tank_arrangement, row.regenbecken_anordnung
            ),
            #-- attribute 3D ---
            #upper_elevation=row.deckenkote,

        )
        qgep_session.add(special_structure)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.versickerungsanlage, ABWASSER.metaattribute -> QGEP.infiltration_installation")
    for row, metaattribute in abwasser_session.query(ABWASSER.versickerungsanlage, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN versickerungsanlage

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- versickerungsanlage ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        infiltration_installation = create_or_update(
            QGEP.infiltration_installation,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- wastewater_structure ---
            **wastewater_structure_common(row),
            # --- infiltration_installation ---
            absorption_capacity=row.schluckvermoegen,
            defects__REL=get_vl_instance(
                QGEP.infiltration_installation_defects, row.maengel
            ),
            #-- attribute 3D ---
            #depth=row.maechtigkeit,
            dimension1=row.dimension1,
            dimension2=row.dimension2,
            distance_to_aquifer=row.gwdistanz,
            effective_area=row.wirksameflaeche,
            emergency_spillway__REL=get_vl_instance(
                QGEP.infiltration_installation_emergency_spillway, row.notueberlauf
            ),
            fk_aquifier=get_pk(
                row.grundwasserleiterref__REL
            ),
            kind__REL=get_vl_instance(
                QGEP.infiltration_installation_kind, row.art
            ),
            labeling__REL=get_vl_instance(
                QGEP.infiltration_installation_labeling, row.beschriftung
            ),
            seepage_utilization__REL=get_vl_instance(
                QGEP.infiltration_installation_seepage_utilization, row.versickerungswasser
            ),
            #-- attribute 3D ---
            #upper_elevation=row.deckenkote,
            vehicle_access__REL=get_vl_instance(
                QGEP.infiltration_installation_vehicle_access, row.saugwagen
            ),
            watertightness__REL=get_vl_instance(
                QGEP.infiltration_installation_watertightness, row.wasserdichtheit
            ),

        )
        qgep_session.add(infiltration_installation)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.arabauwerk, ABWASSER.metaattribute -> QGEP.wwtp_structure")
    for row, metaattribute in abwasser_session.query(ABWASSER.arabauwerk, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN arabauwerk

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- arabauwerk ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        wwtp_structure = create_or_update(
            QGEP.wwtp_structure,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- wastewater_structure ---
            **wastewater_structure_common(row),
            # --- wwtp_structure ---
            kind__REL=get_vl_instance(
                QGEP.wwtp_structure_kind, row.art
            ),

        )
        qgep_session.add(wwtp_structure)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.planungszone, ABWASSER.metaattribute -> QGEP.planning_zone")
    for row, metaattribute in abwasser_session.query(ABWASSER.planungszone, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN planungszone

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- planungszone ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        planning_zone = create_or_update(
            QGEP.planning_zone,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- zone ---
            **zone_common(row),
            # --- planning_zone ---
            kind__REL=get_vl_instance(
                QGEP.planning_zone_kind, row.art
            ),
            perimeter_geometry=(row.perimeter),

        )
        qgep_session.add(planning_zone)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.versickerungsbereich, ABWASSER.metaattribute -> QGEP.infiltration_zone")
    for row, metaattribute in abwasser_session.query(ABWASSER.versickerungsbereich, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN versickerungsbereich

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- versickerungsbereich ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        infiltration_zone = create_or_update(
            QGEP.infiltration_zone,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- zone ---
            **zone_common(row),
            # --- infiltration_zone ---
            infiltration_capacity__REL=get_vl_instance(
                QGEP.infiltration_zone_infiltration_capacity, row.versickerungsmoeglichkeit
            ),
            perimeter_geometry=(row.perimeter),

        )
        qgep_session.add(infiltration_zone)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.entwaesserungssystem, ABWASSER.metaattribute -> QGEP.drainage_system")
    for row, metaattribute in abwasser_session.query(ABWASSER.entwaesserungssystem, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN entwaesserungssystem

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- entwaesserungssystem ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        drainage_system = create_or_update(
            QGEP.drainage_system,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- zone ---
            **zone_common(row),
            # --- drainage_system ---
            kind__REL=get_vl_instance(
                QGEP.drainage_system_kind, row.art
            ),
            perimeter_geometry=(row.perimeter),

        )
        qgep_session.add(drainage_system)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.gewaesserschutzbereich, ABWASSER.metaattribute -> QGEP.water_body_protection_sector")
    for row, metaattribute in abwasser_session.query(ABWASSER.gewaesserschutzbereich, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN gewaesserschutzbereich

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- gewaesserschutzbereich ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        water_body_protection_sector = create_or_update(
            QGEP.water_body_protection_sector,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- zone ---
            **zone_common(row),
            # --- water_body_protection_sector ---
            kind__REL=get_vl_instance(
                QGEP.water_body_protection_sector_kind, row.art
            ),
            perimeter_geometry=(row.perimeter),

        )
        qgep_session.add(water_body_protection_sector)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.grundwasserschutzareal, ABWASSER.metaattribute -> QGEP.ground_water_protection_perimeter")
    for row, metaattribute in abwasser_session.query(ABWASSER.grundwasserschutzareal, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN grundwasserschutzareal

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- grundwasserschutzareal ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        ground_water_protection_perimeter = create_or_update(
            QGEP.ground_water_protection_perimeter,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- zone ---
            **zone_common(row),
            # --- ground_water_protection_perimeter ---
            perimeter_geometry=(row.perimeter),

        )
        qgep_session.add(ground_water_protection_perimeter)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.grundwasserschutzzone, ABWASSER.metaattribute -> QGEP.groundwater_protection_zone")
    for row, metaattribute in abwasser_session.query(ABWASSER.grundwasserschutzzone, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN grundwasserschutzzone

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- grundwasserschutzzone ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        groundwater_protection_zone = create_or_update(
            QGEP.groundwater_protection_zone,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- zone ---
            **zone_common(row),
            # --- groundwater_protection_zone ---
            kind__REL=get_vl_instance(
                QGEP.groundwater_protection_zone_kind, row.art
            ),
            perimeter_geometry=(row.perimeter),

        )
        qgep_session.add(groundwater_protection_zone)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.rohrprofil, ABWASSER.metaattribute -> QGEP.pipe_profile")
    for row, metaattribute in abwasser_session.query(ABWASSER.rohrprofil, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        pipe_profile = create_or_update(
            QGEP.pipe_profile,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- pipe_profile ---
            height_width_ratio=row.hoehenbreitenverhaeltnis,
            identifier=row.bezeichnung,
            profile_type__REL=get_vl_instance(
                QGEP.pipe_profile_profile_type, row.profiltyp
            ),
            remark=row.bemerkung,
        )
        qgep_session.add(pipe_profile)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.araenergienutzung, ABWASSER.metaattribute -> QGEP.wwtp_energy_use")
    for row, metaattribute in abwasser_session.query(ABWASSER.araenergienutzung, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        wwtp_energy_use = create_or_update(
            QGEP.wwtp_energy_use,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- wwtp_energy_use ---
            fk_waste_water_treatment_plant=get_pk(row.abwasserreinigungsanlageref__REL),
            gas_motor=row.gasmotor,
            heat_pump=row.waermepumpe,
            identifier=row.bezeichnung,
            remark=row.bemerkung,
            turbining=row.turbinierung,
        )
        qgep_session.add(wwtp_energy_use)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.abwasserbehandlung, ABWASSER.metaattribute -> QGEP.waste_water_treatment")
    for row, metaattribute in abwasser_session.query(ABWASSER.abwasserbehandlung, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        waste_water_treatment = create_or_update(
            QGEP.waste_water_treatment,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- waste_water_treatment ---
            fk_waste_water_treatment_plant=get_pk(row.abwasserreinigungsanlageref__REL),
            identifier=row.bezeichnung,
            kind__REL=get_vl_instance(
                QGEP.waste_water_treatment_kind, row.art
            ),
            remark=row.bemerkung,
        )
        qgep_session.add(waste_water_treatment)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.schlammbehandlung, ABWASSER.metaattribute -> QGEP.sludge_treatment")
    for row, metaattribute in abwasser_session.query(ABWASSER.schlammbehandlung, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        sludge_treatment = create_or_update(
            QGEP.sludge_treatment,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- sludge_treatment ---
            composting=row.kompostierung,
            dehydration=row.entwaesserung,
            digested_sludge_combustion=row.faulschlammverbrennung,
            drying=row.trocknung,
            fk_waste_water_treatment_plant=get_pk(row.abwasserreinigungsanlageref__REL),
            fresh_sludge_combustion=row.frischschlammverbrennung,
            hygenisation=row.hygienisierung,
            identifier=row.bezeichnung,
            predensification_of_excess_sludge=row.ueberschusschlammvoreindickung,
            predensification_of_mixed_sludge=row.mischschlammvoreindickung,
            predensification_of_primary_sludge=row.primaerschlammvoreindickung,
            remark=row.bemerkung,
            stabilisation__REL=get_vl_instance(
                QGEP.sludge_treatment_stabilisation, row.stabilisierung
            ),
            stacking_of_dehydrated_sludge=row.entwaessertklaerschlammstapelung,
            stacking_of_liquid_sludge=row.fluessigklaerschlammstapelung,
        )
        qgep_session.add(sludge_treatment)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.steuerungszentrale, ABWASSER.metaattribute -> QGEP.control_center")
    for row, metaattribute in abwasser_session.query(ABWASSER.steuerungszentrale, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        control_center = create_or_update(
            QGEP.control_center,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- control_center ---
            identifier=row.bezeichnung,
            situation_geometry=(row.lage),
        )
        qgep_session.add(control_center)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.furt, ABWASSER.metaattribute -> QGEP.ford")
    for row, metaattribute in abwasser_session.query(ABWASSER.furt, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN furt

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- furt ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        ford = create_or_update(
            QGEP.ford,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- water_control_structure ---
            **water_control_structure_common(row),
            # --- ford ---

        )
        qgep_session.add(ford)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.gewaesserabsturz, ABWASSER.metaattribute -> QGEP.chute")
    for row, metaattribute in abwasser_session.query(ABWASSER.gewaesserabsturz, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN gewaesserabsturz

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- gewaesserabsturz ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        chute = create_or_update(
            QGEP.chute,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- water_control_structure ---
            **water_control_structure_common(row),
            # --- chute ---
            kind__REL=get_vl_instance(
                QGEP.chute_kind, row.typ
            ),
            material__REL=get_vl_instance(
                QGEP.chute_material, row.material
            ),
            vertical_drop=row.absturzhoehe,

        )
        qgep_session.add(chute)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.schleuse, ABWASSER.metaattribute -> QGEP.lock")
    for row, metaattribute in abwasser_session.query(ABWASSER.schleuse, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN schleuse

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- schleuse ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        lock = create_or_update(
            QGEP.lock,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- water_control_structure ---
            **water_control_structure_common(row),
            # --- lock ---
            vertical_drop=row.absturzhoehe,

        )
        qgep_session.add(lock)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.durchlass, ABWASSER.metaattribute -> QGEP.passage")
    for row, metaattribute in abwasser_session.query(ABWASSER.durchlass, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN durchlass

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- durchlass ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        passage = create_or_update(
            QGEP.passage,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- water_control_structure ---
            **water_control_structure_common(row),
            # --- passage ---

        )
        qgep_session.add(passage)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.geschiebesperre, ABWASSER.metaattribute -> QGEP.blocking_debris")
    for row, metaattribute in abwasser_session.query(ABWASSER.geschiebesperre, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN geschiebesperre

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- geschiebesperre ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        blocking_debris = create_or_update(
            QGEP.blocking_debris,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- water_control_structure ---
            **water_control_structure_common(row),
            # --- blocking_debris ---
            vertical_drop=row.absturzhoehe,

        )
        qgep_session.add(blocking_debris)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.gewaesserwehr, ABWASSER.metaattribute -> QGEP.dam")
    for row, metaattribute in abwasser_session.query(ABWASSER.gewaesserwehr, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN gewaesserwehr

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- gewaesserwehr ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        dam = create_or_update(
            QGEP.dam,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- water_control_structure ---
            **water_control_structure_common(row),
            # --- dam ---
            kind__REL=get_vl_instance(
                QGEP.dam_kind, row.art
            ),
            vertical_drop=row.absturzhoehe,

        )
        qgep_session.add(dam)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.sohlrampe, ABWASSER.metaattribute -> QGEP.rock_ramp")
    for row, metaattribute in abwasser_session.query(ABWASSER.sohlrampe, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN sohlrampe

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- sohlrampe ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        rock_ramp = create_or_update(
            QGEP.rock_ramp,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- water_control_structure ---
            **water_control_structure_common(row),
            # --- rock_ramp ---
            stabilisation__REL=get_vl_instance(
                QGEP.rock_ramp_stabilisation, row.befestigung
            ),
            vertical_drop=row.absturzhoehe,

        )
        qgep_session.add(rock_ramp)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.fischpass, ABWASSER.metaattribute -> QGEP.fish_pass")
    for row, metaattribute in abwasser_session.query(ABWASSER.fischpass, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        fish_pass = create_or_update(
            QGEP.fish_pass,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- fish_pass ---
            fk_water_control_structure=get_pk(row.gewaesserverbauungref__REL),
            identifier=row.bezeichnung,
            remark=row.bemerkung,
            vertical_drop=row.absturzhoehe,
        )
        qgep_session.add(fish_pass)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.badestelle, ABWASSER.metaattribute -> QGEP.bathing_area")
    for row, metaattribute in abwasser_session.query(ABWASSER.badestelle, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        bathing_area = create_or_update(
            QGEP.bathing_area,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- bathing_area ---
            fk_surface_water_bodies=get_pk(row.oberflaechengewaesserref__REL),
            identifier=row.bezeichnung,
            remark=row.bemerkung,
            situation_geometry=(row.lage),
        )
        qgep_session.add(bathing_area)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.hydr_geometrie, ABWASSER.metaattribute -> QGEP.hydr_geometry")
    for row, metaattribute in abwasser_session.query(ABWASSER.hydr_geometrie, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        hydr_geometry = create_or_update(
            QGEP.hydr_geometry,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- hydr_geometry ---
            identifier=row.bezeichnung,
            remark=row.bemerkung,
            storage_volume=row.stauraum,
            usable_capacity_storage=row.nutzinhalt_fangteil,
            usable_capacity_treatment=row.nutzinhalt_klaerteil,
            utilisable_capacity=row.nutzinhalt,
            volume_pump_sump=row.volumen_pumpensumpf,
        )
        qgep_session.add(hydr_geometry)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.haltungspunkt, ABWASSER.metaattribute -> QGEP.reach_point")
    for row, metaattribute in abwasser_session.query(ABWASSER.haltungspunkt, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        reach_point = create_or_update(
            QGEP.reach_point,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- reach_point ---
            elevation_accuracy__REL=get_vl_instance(
                QGEP.reach_point_elevation_accuracy, row.hoehengenauigkeit
            ),
            fk_wastewater_networkelement=get_pk(row.abwassernetzelementref__REL),
            identifier=row.bezeichnung,
            level=row.kote,
            outlet_shape__REL=get_vl_instance(
                QGEP.reach_point_outlet_shape, row.auslaufform
            ),
            position_of_connection=row.lage_anschluss,
            remark=row.bemerkung,
            situation_geometry=ST_Force3D(row.lage),
        )
        qgep_session.add(reach_point)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.abwasserknoten, ABWASSER.metaattribute -> QGEP.wastewater_node")
    for row, metaattribute in abwasser_session.query(ABWASSER.abwasserknoten, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN abwasserknoten

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- abwasserknoten ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        wastewater_node = create_or_update(
            QGEP.wastewater_node,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- wastewater_networkelement ---
            **wastewater_networkelement_common(row),
            # --- wastewater_node ---
            backflow_level=row.rueckstaukote,
            bottom_level=row.sohlenkote,
            fk_hydr_geometry=get_pk(
                row.hydr_geometrieref__REL
            ),
            situation_geometry=ST_Force3D(row.lage),

        )
        qgep_session.add(wastewater_node)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.haltung, ABWASSER.metaattribute -> QGEP.reach")
    for row, metaattribute in abwasser_session.query(ABWASSER.haltung, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN haltung

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- haltung ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        reach = create_or_update(
            QGEP.reach,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- wastewater_networkelement ---
            **wastewater_networkelement_common(row),
            # --- reach ---
            clear_height=row.lichte_hoehe,
            coefficient_of_friction=row.reibungsbeiwert,
            #-- attribute 3D ---            #elevation_determination__REL=get_vl_instance(
                #QGEP.reach_elevation_determination, row.hoehenbestimmung
            #),
            fk_pipe_profile=get_pk(
                row.rohrprofilref__REL
            ),
            fk_reach_point_from=get_pk(
                row.vonhaltungspunktref__REL
            ),
            fk_reach_point_to=get_pk(
                row.nachhaltungspunktref__REL
            ),
            horizontal_positioning__REL=get_vl_instance(
                QGEP.reach_horizontal_positioning, row.lagebestimmung
            ),
            inside_coating__REL=get_vl_instance(
                QGEP.reach_inside_coating, row.innenschutz
            ),
            length_effective=row.laengeeffektiv,
            material__REL=get_vl_instance(
                QGEP.reach_material, row.material
            ),
            progression_geometry=ST_Force3D(row.verlauf),
            #-- attribute 3D ---
            #progression3d=row.verlauf3d,
            reliner_material__REL=get_vl_instance(
                QGEP.reach_reliner_material, row.reliner_material
            ),
            reliner_nominal_size=row.reliner_nennweite,
            relining_construction__REL=get_vl_instance(
                QGEP.reach_relining_construction, row.reliner_bautechnik
            ),
            relining_kind__REL=get_vl_instance(
                QGEP.reach_relining_kind, row.reliner_art
            ),
            ring_stiffness=row.ringsteifigkeit,
            slope_building_plan=row.plangefaelle,
            wall_roughness=row.wandrauhigkeit,

        )
        qgep_session.add(reach)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.rohrprofil_geometrie, ABWASSER.metaattribute -> QGEP.profile_geometry")
    for row, metaattribute in abwasser_session.query(ABWASSER.rohrprofil_geometrie, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        profile_geometry = create_or_update(
            QGEP.profile_geometry,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- profile_geometry ---
            fk_pipe_profile=get_pk(row.rohrprofilref__REL),
            position=row.aposition,
            x=row.x,
            y=row.y,
        )
        qgep_session.add(profile_geometry)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.hydr_geomrelation, ABWASSER.metaattribute -> QGEP.hydr_geom_relation")
    for row, metaattribute in abwasser_session.query(ABWASSER.hydr_geomrelation, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        hydr_geom_relation = create_or_update(
            QGEP.hydr_geom_relation,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- hydr_geom_relation ---
            fk_hydr_geometry=get_pk(row.hydr_geometrieref__REL),
            water_depth=row.wassertiefe,
            water_surface=row.wasseroberflaeche,
            wet_cross_section_area=row.benetztequerschnittsflaeche,
        )
        qgep_session.add(hydr_geom_relation)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.mechanischevorreinigung, ABWASSER.metaattribute -> QGEP.mechanical_pretreatment")
    for row, metaattribute in abwasser_session.query(ABWASSER.mechanischevorreinigung, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        mechanical_pretreatment = create_or_update(
            QGEP.mechanical_pretreatment,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- mechanical_pretreatment ---
            fk_infiltration_installation=get_pk(row.versickerungsanlageref__REL),
            fk_wastewater_structure=get_pk(row.abwasserbauwerkref__REL),
            identifier=row.bezeichnung,
            kind__REL=get_vl_instance(
                QGEP.mechanical_pretreatment_kind, row.art
            ),
            remark=row.bemerkung,
        )
        qgep_session.add(mechanical_pretreatment)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.retentionskoerper, ABWASSER.metaattribute -> QGEP.retention_body")
    for row, metaattribute in abwasser_session.query(ABWASSER.retentionskoerper, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        retention_body = create_or_update(
            QGEP.retention_body,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- retention_body ---
            fk_infiltration_installation=get_pk(row.versickerungsanlageref__REL),
            identifier=row.bezeichnung,
            kind__REL=get_vl_instance(
                QGEP.retention_body_kind, row.art
            ),
            remark=row.bemerkung,
            volume=row.retention_volumen,
        )
        qgep_session.add(retention_body)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.ueberlaufcharakteristik, ABWASSER.metaattribute -> QGEP.overflow_char")
    for row, metaattribute in abwasser_session.query(ABWASSER.ueberlaufcharakteristik, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        overflow_char = create_or_update(
            QGEP.overflow_char,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- overflow_char ---
            identifier=row.bezeichnung,
            kind_overflow_char__REL=get_vl_instance(
                QGEP.overflow_char_kind_overflow_char, row.kennlinie_typ
            ),
            overflow_char_digital__REL=get_vl_instance(
                QGEP.overflow_char_overflow_char_digital, row.kennlinie_digital
            ),
            remark=row.bemerkung,
        )
        qgep_session.add(overflow_char)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.hq_relation, ABWASSER.metaattribute -> QGEP.hq_relation")
    for row, metaattribute in abwasser_session.query(ABWASSER.hq_relation, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        hq_relation = create_or_update(
            QGEP.hq_relation,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- hq_relation ---
            altitude=row.hoehe,
            fk_overflow_char=get_pk(row.ueberlaufcharakteristikref__REL),
            flow=row.abfluss,
            flow_from=row.zufluss,
        )
        qgep_session.add(hq_relation)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.trockenwetterfallrohr, ABWASSER.metaattribute -> QGEP.dryweather_downspout")
    for row, metaattribute in abwasser_session.query(ABWASSER.trockenwetterfallrohr, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN trockenwetterfallrohr

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- trockenwetterfallrohr ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        dryweather_downspout = create_or_update(
            QGEP.dryweather_downspout,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- structure_part ---
            **structure_part_common(row),
            # --- dryweather_downspout ---
            diameter=row.durchmesser,

        )
        qgep_session.add(dryweather_downspout)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.einstiegshilfe, ABWASSER.metaattribute -> QGEP.access_aid")
    for row, metaattribute in abwasser_session.query(ABWASSER.einstiegshilfe, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN einstiegshilfe

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- einstiegshilfe ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        access_aid = create_or_update(
            QGEP.access_aid,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- structure_part ---
            **structure_part_common(row),
            # --- access_aid ---
            kind__REL=get_vl_instance(
                QGEP.access_aid_kind, row.art
            ),

        )
        qgep_session.add(access_aid)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.trockenwetterrinne, ABWASSER.metaattribute -> QGEP.dryweather_flume")
    for row, metaattribute in abwasser_session.query(ABWASSER.trockenwetterrinne, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN trockenwetterrinne

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- trockenwetterrinne ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        dryweather_flume = create_or_update(
            QGEP.dryweather_flume,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- structure_part ---
            **structure_part_common(row),
            # --- dryweather_flume ---
            material__REL=get_vl_instance(
                QGEP.dryweather_flume_material, row.material
            ),

        )
        qgep_session.add(dryweather_flume)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.deckel, ABWASSER.metaattribute -> QGEP.cover")
    for row, metaattribute in abwasser_session.query(ABWASSER.deckel, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN deckel

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- deckel ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        cover = create_or_update(
            QGEP.cover,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- structure_part ---
            **structure_part_common(row),
            # --- cover ---
            brand=row.fabrikat,
            cover_shape__REL=get_vl_instance(
                QGEP.cover_cover_shape, row.deckelform
            ),
            #-- attribute 3D ---
            #depth=row.maechtigkeit,
            diameter=row.durchmesser,
            fastening__REL=get_vl_instance(
                QGEP.cover_fastening, row.verschluss
            ),
            level=row.kote,
            material__REL=get_vl_instance(
                QGEP.cover_material, row.material
            ),
            positional_accuracy__REL=get_vl_instance(
                QGEP.cover_positional_accuracy, row.lagegenauigkeit
            ),
            situation_geometry=ST_Force3D(row.lage),
            sludge_bucket__REL=get_vl_instance(
                QGEP.cover_sludge_bucket, row.schlammeimer
            ),
            venting__REL=get_vl_instance(
                QGEP.cover_venting, row.entlueftung
            ),

        )
        qgep_session.add(cover)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.elektrischeeinrichtung, ABWASSER.metaattribute -> QGEP.electric_equipment")
    for row, metaattribute in abwasser_session.query(ABWASSER.elektrischeeinrichtung, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN elektrischeeinrichtung

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- elektrischeeinrichtung ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        electric_equipment = create_or_update(
            QGEP.electric_equipment,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- structure_part ---
            **structure_part_common(row),
            # --- electric_equipment ---
            gross_costs=row.bruttokosten,
            kind__REL=get_vl_instance(
                QGEP.electric_equipment_kind, row.art
            ),
            year_of_replacement=row.ersatzjahr,

        )
        qgep_session.add(electric_equipment)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.elektromechanischeausruestung, ABWASSER.metaattribute -> QGEP.electromechanical_equipment")
    for row, metaattribute in abwasser_session.query(ABWASSER.elektromechanischeausruestung, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN elektromechanischeausruestung

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- elektromechanischeausruestung ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        electromechanical_equipment = create_or_update(
            QGEP.electromechanical_equipment,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- structure_part ---
            **structure_part_common(row),
            # --- electromechanical_equipment ---
            gross_costs=row.bruttokosten,
            kind__REL=get_vl_instance(
                QGEP.electromechanical_equipment_kind, row.art
            ),
            year_of_replacement=row.ersatzjahr,

        )
        qgep_session.add(electromechanical_equipment)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.bankett, ABWASSER.metaattribute -> QGEP.benching")
    for row, metaattribute in abwasser_session.query(ABWASSER.bankett, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN bankett

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- bankett ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        benching = create_or_update(
            QGEP.benching,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- structure_part ---
            **structure_part_common(row),
            # --- benching ---
            kind__REL=get_vl_instance(
                QGEP.benching_kind, row.art
            ),

        )
        qgep_session.add(benching)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.gebaeude, ABWASSER.metaattribute -> QGEP.building")
    for row, metaattribute in abwasser_session.query(ABWASSER.gebaeude, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN gebaeude

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- gebaeude ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        building = create_or_update(
            QGEP.building,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- connection_object ---
            **connection_object_common(row),
            # --- building ---
            house_number=row.hausnummer,
            location_name=row.standortname,
            perimeter_geometry=(row.perimeter),
            reference_point_geometry=(row.referenzpunkt),

        )
        qgep_session.add(building)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.reservoir, ABWASSER.metaattribute -> QGEP.reservoir")
    for row, metaattribute in abwasser_session.query(ABWASSER.reservoir, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN reservoir

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- reservoir ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        reservoir = create_or_update(
            QGEP.reservoir,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- connection_object ---
            **connection_object_common(row),
            # --- reservoir ---
            location_name=row.standortname,
            situation_geometry=(row.lage),

        )
        qgep_session.add(reservoir)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.einzelflaeche, ABWASSER.metaattribute -> QGEP.individual_surface")
    for row, metaattribute in abwasser_session.query(ABWASSER.einzelflaeche, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN einzelflaeche

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- einzelflaeche ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        individual_surface = create_or_update(
            QGEP.individual_surface,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- connection_object ---
            **connection_object_common(row),
            # --- individual_surface ---
            function__REL=get_vl_instance(
                QGEP.individual_surface_function, row.funktion
            ),
            inclination=row.neigung,
            pavement__REL=get_vl_instance(
                QGEP.individual_surface_pavement, row.befestigung
            ),
            perimeter_geometry=(row.perimeter),

        )
        qgep_session.add(individual_surface)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.brunnen, ABWASSER.metaattribute -> QGEP.fountain")
    for row, metaattribute in abwasser_session.query(ABWASSER.brunnen, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN brunnen

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- brunnen ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        fountain = create_or_update(
            QGEP.fountain,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- connection_object ---
            **connection_object_common(row),
            # --- fountain ---
            location_name=row.standortname,
            situation_geometry=(row.lage),

        )
        qgep_session.add(fountain)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.gefahrenquelle, ABWASSER.metaattribute -> QGEP.hazard_source")
    for row, metaattribute in abwasser_session.query(ABWASSER.gefahrenquelle, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        hazard_source = create_or_update(
            QGEP.hazard_source,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- hazard_source ---
            fk_connection_object=get_pk(row.anschlussobjektref__REL),
            fk_owner=get_pk(row.eigentuemerref__REL),
            identifier=row.bezeichnung,
            remark=row.bemerkung,
            situation_geometry=(row.lage),
        )
        qgep_session.add(hazard_source)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.unfall, ABWASSER.metaattribute -> QGEP.accident")
    for row, metaattribute in abwasser_session.query(ABWASSER.unfall, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        accident = create_or_update(
            QGEP.accident,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- accident ---
            date=row.datum,
            fk_hazard_source=get_pk(row.gefahrenquelleref__REL),
            identifier=row.bezeichnung,
            place=row.ort,
            remark=row.bemerkung,
            responsible=row.verursacher,
            situation_geometry=(row.lage),
        )
        qgep_session.add(accident)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.stoff, ABWASSER.metaattribute -> QGEP.substance")
    for row, metaattribute in abwasser_session.query(ABWASSER.stoff, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        substance = create_or_update(
            QGEP.substance,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- substance ---
            fk_hazard_source=get_pk(row.gefahrenquelleref__REL),
            identifier=row.bezeichnung,
            kind=row.art,
            remark=row.bemerkung,
            stockage=row.lagerung,
        )
        qgep_session.add(substance)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.einzugsgebiet, ABWASSER.metaattribute -> QGEP.catchment_area")
    for row, metaattribute in abwasser_session.query(ABWASSER.einzugsgebiet, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        catchment_area = create_or_update(
            QGEP.catchment_area,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- catchment_area ---
            direct_discharge_current__REL=get_vl_instance(
                QGEP.catchment_area_direct_discharge_current, row.direkteinleitung_in_gewaesser_ist
            ),
            direct_discharge_planned__REL=get_vl_instance(
                QGEP.catchment_area_direct_discharge_planned, row.direkteinleitung_in_gewaesser_geplant
            ),
            discharge_coefficient_rw_current=row.abflussbeiwert_rw_ist,
            discharge_coefficient_rw_planned=row.abflussbeiwert_rw_geplant,
            discharge_coefficient_ww_current=row.abflussbeiwert_sw_ist,
            discharge_coefficient_ww_planned=row.abflussbeiwert_sw_geplant,
            drainage_system_current__REL=get_vl_instance(
                QGEP.catchment_area_drainage_system_current, row.entwaesserungssystem_ist
            ),
            drainage_system_planned__REL=get_vl_instance(
                QGEP.catchment_area_drainage_system_planned, row.entwaesserungssystem_geplant
            ),
            # not supported in qgep datamodel, vsa-dss release 2020 only
            #fk_special_building_rw_current=get_pk(row.sbw_rw_istref__REL),
            # not supported in qgep datamodel, vsa-dss release 2020 only
            #fk_special_building_rw_planned=get_pk(row.sbw_rw_geplantref__REL),
            # not supported in qgep datamodel, vsa-dss release 2020 only
            #fk_special_building_ww_current=get_pk(row.sbw_sw_istref__REL),
            # not supported in qgep datamodel, vsa-dss release 2020 only
            #fk_special_building_ww_planned=get_pk(row.sbw_sw_geplantref__REL),
            fk_wastewater_networkelement_rw_current=get_pk(row.abwassernetzelement_rw_istref__REL),
            fk_wastewater_networkelement_rw_planned=get_pk(row.abwassernetzelement_rw_geplantref__REL),
            fk_wastewater_networkelement_ww_current=get_pk(row.abwassernetzelement_sw_istref__REL),
            fk_wastewater_networkelement_ww_planned=get_pk(row.abwassernetzelement_sw_geplantref__REL),
            identifier=row.bezeichnung,
            infiltration_current__REL=get_vl_instance(
                QGEP.catchment_area_infiltration_current, row.versickerung_ist
            ),
            infiltration_planned__REL=get_vl_instance(
                QGEP.catchment_area_infiltration_planned, row.versickerung_geplant
            ),
            perimeter_geometry=(row.perimeter),
            population_density_current=row.einwohnerdichte_ist,
            population_density_planned=row.einwohnerdichte_geplant,
            remark=row.bemerkung,
            retention_current__REL=get_vl_instance(
                QGEP.catchment_area_retention_current, row.retention_ist
            ),
            retention_planned__REL=get_vl_instance(
                QGEP.catchment_area_retention_planned, row.retention_geplant
            ),
            runoff_limit_current=row.abflussbegrenzung_ist,
            runoff_limit_planned=row.abflussbegrenzung_geplant,
            seal_factor_rw_current=row.befestigungsgrad_rw_ist,
            seal_factor_rw_planned=row.befestigungsgrad_rw_geplant,
            seal_factor_ww_current=row.befestigungsgrad_sw_ist,
            seal_factor_ww_planned=row.befestigungsgrad_sw_geplant,
            sewer_infiltration_water_production_current=row.fremdwasseranfall_ist,
            sewer_infiltration_water_production_planned=row.fremdwasseranfall_geplant,
            surface_area=row.flaeche,
            waste_water_production_current=row.schmutzabwasseranfall_ist,
            waste_water_production_planned=row.schmutzabwasseranfall_geplant,
        )
        qgep_session.add(catchment_area)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.messstelle, ABWASSER.metaattribute -> QGEP.measuring_point")
    for row, metaattribute in abwasser_session.query(ABWASSER.messstelle, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        measuring_point = create_or_update(
            QGEP.measuring_point,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- measuring_point ---
            damming_device__REL=get_vl_instance(
                QGEP.measuring_point_damming_device, row.staukoerper
            ),
            fk_operator=get_pk(row.betreiberref__REL),
            # not supported in qgep datamodel yet, reference on same class
            #fk_reference_station=get_pk(row.referenzstelleref__REL),
            fk_waste_water_treatment_plant=get_pk(row.abwasserreinigungsanlageref__REL),
            fk_wastewater_structure=get_pk(row.abwasserbauwerkref__REL),
            fk_water_course_segment=get_pk(row.gewaesserabschnittref__REL),
            identifier=row.bezeichnung,
            kind=row.art,
            purpose__REL=get_vl_instance(
                QGEP.measuring_point_purpose, row.zweck
            ),
            remark=row.bemerkung,
            situation_geometry=(row.lage),
        )
        qgep_session.add(measuring_point)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.messgeraet, ABWASSER.metaattribute -> QGEP.measuring_device")
    for row, metaattribute in abwasser_session.query(ABWASSER.messgeraet, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        measuring_device = create_or_update(
            QGEP.measuring_device,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- measuring_device ---
            brand=row.fabrikat,
            fk_measuring_point=get_pk(row.messstelleref__REL),
            identifier=row.bezeichnung,
            kind__REL=get_vl_instance(
                QGEP.measuring_device_kind, row.art
            ),
            remark=row.bemerkung,
            serial_number=row.seriennummer,
        )
        qgep_session.add(measuring_device)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.messreihe, ABWASSER.metaattribute -> QGEP.measurement_series")
    for row, metaattribute in abwasser_session.query(ABWASSER.messreihe, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        measurement_series = create_or_update(
            QGEP.measurement_series,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- measurement_series ---
            dimension=row.dimension,
            fk_measuring_point=get_pk(row.messstelleref__REL),
            # not supported in qgep datamodel yet, reference on same class
            #fk_wastewater_networkelement=get_pk(row.abwassernetzelementref__REL),
            identifier=row.bezeichnung,
            kind__REL=get_vl_instance(
                QGEP.measurement_series_kind, row.art
            ),
            remark=row.bemerkung,
        )
        qgep_session.add(measurement_series)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.messresultat, ABWASSER.metaattribute -> QGEP.measurement_result")
    for row, metaattribute in abwasser_session.query(ABWASSER.messresultat, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        measurement_result = create_or_update(
            QGEP.measurement_result,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- measurement_result ---
            fk_measurement_series=get_pk(row.messreiheref__REL),
            fk_measuring_device=get_pk(row.messgeraetref__REL),
            identifier=row.bezeichnung,
            measurement_type__REL=get_vl_instance(
                QGEP.measurement_result_measurement_type, row.messart
            ),
            measuring_duration=row.messdauer,
            remark=row.bemerkung,
            time=row.zeit,
            value=row.wert,
        )
        qgep_session.add(measurement_result)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.absperr_drosselorgan, ABWASSER.metaattribute -> QGEP.throttle_shut_off_unit")
    for row, metaattribute in abwasser_session.query(ABWASSER.absperr_drosselorgan, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        throttle_shut_off_unit = create_or_update(
            QGEP.throttle_shut_off_unit,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- throttle_shut_off_unit ---
            actuation__REL=get_vl_instance(
                QGEP.throttle_shut_off_unit_actuation, row.antrieb
            ),
            adjustability__REL=get_vl_instance(
                QGEP.throttle_shut_off_unit_adjustability, row.verstellbarkeit
            ),
            control__REL=get_vl_instance(
                QGEP.throttle_shut_off_unit_control, row.steuerung
            ),
            cross_section=row.querschnitt,
            effective_cross_section=row.wirksamer_qs,
            fk_control_center=get_pk(row.steuerungszentraleref__REL),
            fk_overflow=get_pk(row.ueberlaufref__REL),
            fk_wastewater_node=get_pk(row.abwasserknotenref__REL),
            gross_costs=row.bruttokosten,
            identifier=row.bezeichnung,
            kind__REL=get_vl_instance(
                QGEP.throttle_shut_off_unit_kind, row.art
            ),
            manufacturer=row.fabrikat,
            remark=row.bemerkung,
            signal_transmission__REL=get_vl_instance(
                QGEP.throttle_shut_off_unit_signal_transmission, row.signaluebermittlung
            ),
            subsidies=row.subventionen,
            throttle_unit_opening_current=row.drosselorgan_oeffnung_ist,
            throttle_unit_opening_current_optimized=row.drosselorgan_oeffnung_ist_optimiert,
        )
        qgep_session.add(throttle_shut_off_unit)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.streichwehr, ABWASSER.metaattribute -> QGEP.prank_weir")
    for row, metaattribute in abwasser_session.query(ABWASSER.streichwehr, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN streichwehr

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- streichwehr ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        prank_weir = create_or_update(
            QGEP.prank_weir,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- overflow ---
            **overflow_common(row),
            # --- prank_weir ---
            hydraulic_overflow_length=row.hydrueberfalllaenge,
            level_max=row.kotemax,
            level_min=row.kotemin,
            weir_edge__REL=get_vl_instance(
                QGEP.prank_weir_weir_edge, row.ueberfallkante
            ),
            weir_kind__REL=get_vl_instance(
                QGEP.prank_weir_weir_kind, row.wehr_art
            ),

        )
        qgep_session.add(prank_weir)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.foerderaggregat, ABWASSER.metaattribute -> QGEP.pump")
    for row, metaattribute in abwasser_session.query(ABWASSER.foerderaggregat, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN foerderaggregat

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- foerderaggregat ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        pump = create_or_update(
            QGEP.pump,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- overflow ---
            **overflow_common(row),
            # --- pump ---
            construction_type__REL=get_vl_instance(
                QGEP.pump_construction_type, row.bauart
            ),
            operating_point=row.arbeitspunkt,
            placement_of_actuation__REL=get_vl_instance(
                QGEP.pump_placement_of_actuation, row.aufstellungantrieb
            ),
            placement_of_pump__REL=get_vl_instance(
                QGEP.pump_placement_of_pump, row.aufstellungfoerderaggregat
            ),
            pump_flow_max_single=row.foerderstrommax_einzel,
            pump_flow_min_single=row.foerderstrommin_einzel,
            start_level=row.kotestart,
            stop_level=row.kotestop,
            usage_current__REL=get_vl_instance(
                QGEP.pump_usage_current, row.nutzungsart_ist
            ),

        )
        qgep_session.add(pump)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.leapingwehr, ABWASSER.metaattribute -> QGEP.leapingweir")
    for row, metaattribute in abwasser_session.query(ABWASSER.leapingwehr, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN leapingwehr

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- leapingwehr ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        leapingweir = create_or_update(
            QGEP.leapingweir,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- overflow ---
            **overflow_common(row),
            # --- leapingweir ---
            length=row.laenge,
            opening_shape__REL=get_vl_instance(
                QGEP.leapingweir_opening_shape, row.oeffnungsform
            ),
            width=row.breite,

        )
        qgep_session.add(leapingweir)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.hydr_kennwerte, ABWASSER.metaattribute -> QGEP.hydraulic_char_data")
    for row, metaattribute in abwasser_session.query(ABWASSER.hydr_kennwerte, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        hydraulic_char_data = create_or_update(
            QGEP.hydraulic_char_data,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- hydraulic_char_data ---
            aggregate_number=row.aggregatezahl,
            delivery_height_geodaetic=row.foerderhoehe_geodaetisch,
            fk_overflow_char=get_pk(row.ueberlaufcharakteristikref__REL),
            # not supported in qgep datamodel, vsa-dss release 2020 only
            #fk_primary_direction=get_pk(row.primaerrichtungref__REL),
            fk_wastewater_node=get_pk(row.abwasserknotenref__REL),
            identifier=row.bezeichnung,
            is_overflowing__REL=get_vl_instance(
                QGEP.hydraulic_char_data_is_overflowing, row.springt_an
            ),
            main_weir_kind__REL=get_vl_instance(
                QGEP.hydraulic_char_data_main_weir_kind, row.hauptwehrart
            ),
            overcharge=row.mehrbelastung,
            overflow_duration=row.ueberlaufdauer,
            overflow_freight=row.ueberlauffracht,
            overflow_frequency=row.ueberlaufhaeufigkeit,
            overflow_volume=row.ueberlaufmenge,
            pump_characteristics__REL=get_vl_instance(
                QGEP.hydraulic_char_data_pump_characteristics, row.pumpenregime
            ),
            pump_flow_max=row.foerderstrommax,
            pump_flow_min=row.foerderstrommin,
            q_discharge=row.qab,
            qon=row.qan,
            remark=row.bemerkung,
            status__REL=get_vl_instance(
                QGEP.hydraulic_char_data_status, row.astatus
            ),
        )
        qgep_session.add(hydraulic_char_data)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.rueckstausicherung, ABWASSER.metaattribute -> QGEP.backflow_prevention")
    for row, metaattribute in abwasser_session.query(ABWASSER.rueckstausicherung, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN rueckstausicherung

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- rueckstausicherung ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        backflow_prevention = create_or_update(
            QGEP.backflow_prevention,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- structure_part ---
            **structure_part_common(row),
            # --- backflow_prevention ---
            fk_pump=get_pk(
                row.foerderaggregatref__REL
            ),
            fk_throttle_shut_off_unit=get_pk(
                row.absperr_drosselorganref__REL
            ),
            gross_costs=row.bruttokosten,
            kind__REL=get_vl_instance(
                QGEP.backflow_prevention_kind, row.art
            ),
            year_of_replacement=row.ersatzjahr,

        )
        qgep_session.add(backflow_prevention)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.feststoffrueckhalt, ABWASSER.metaattribute -> QGEP.solids_retention")
    for row, metaattribute in abwasser_session.query(ABWASSER.feststoffrueckhalt, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN feststoffrueckhalt

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- feststoffrueckhalt ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        solids_retention = create_or_update(
            QGEP.solids_retention,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- structure_part ---
            **structure_part_common(row),
            # --- solids_retention ---
            dimensioning_value=row.dimensionierungswert,
            gross_costs=row.bruttokosten,
            overflow_level=row.anspringkote,
            type__REL=get_vl_instance(
                QGEP.solids_retention_type, row.art
            ),
            year_of_replacement=row.ersatzjahr,

        )
        qgep_session.add(solids_retention)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.beckenreinigung, ABWASSER.metaattribute -> QGEP.tank_cleaning")
    for row, metaattribute in abwasser_session.query(ABWASSER.beckenreinigung, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN beckenreinigung

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- beckenreinigung ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        tank_cleaning = create_or_update(
            QGEP.tank_cleaning,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- structure_part ---
            **structure_part_common(row),
            # --- tank_cleaning ---
            gross_costs=row.bruttokosten,
            type__REL=get_vl_instance(
                QGEP.tank_cleaning_type, row.art
            ),
            year_of_replacement=row.ersatzjahr,

        )
        qgep_session.add(tank_cleaning)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.beckenentleerung, ABWASSER.metaattribute -> QGEP.tank_emptying")
    for row, metaattribute in abwasser_session.query(ABWASSER.beckenentleerung, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN beckenentleerung

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- beckenentleerung ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        tank_emptying = create_or_update(
            QGEP.tank_emptying,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- structure_part ---
            **structure_part_common(row),
            # --- tank_emptying ---
            fk_overflow=get_pk(
                row.ueberlaufref__REL
            ),
            fk_throttle_shut_off_unit=get_pk(
                row.absperr_drosselorganref__REL
            ),
            flow=row.leistung,
            gross_costs=row.bruttokosten,
            type__REL=get_vl_instance(
                QGEP.tank_emptying_type, row.art
            ),
            year_of_replacement=row.ersatzjahr,

        )
        qgep_session.add(tank_emptying)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.ezg_parameter_allg, ABWASSER.metaattribute -> QGEP.param_ca_general")
    for row, metaattribute in abwasser_session.query(ABWASSER.ezg_parameter_allg, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN ezg_parameter_allg

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- ezg_parameter_allg ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        param_ca_general = create_or_update(
            QGEP.param_ca_general,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- surface_runoff_parameters ---
            **surface_runoff_parameters_common(row),
            # --- param_ca_general ---
            dry_wheather_flow=row.trockenwetteranfall,
            flow_path_length=row.fliessweglaenge,
            flow_path_slope=row.fliessweggefaelle,
            population_equivalent=row.einwohnergleichwert,
            surface_ca=row.flaeche,

        )
        qgep_session.add(param_ca_general)
        print(".", end="")
    logger.info("done")

    logger.info("Importing ABWASSER.ezg_parameter_mouse1, ABWASSER.metaattribute -> QGEP.param_ca_mouse1")
    for row, metaattribute in abwasser_session.query(ABWASSER.ezg_parameter_mouse1, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):
        # AVAILABLE FIELDS IN ezg_parameter_mouse1

        # --- baseclass ---
        # t_ili_tid, t_type

        # --- sia405_baseclass ---
        # obj_id

        # --- abwasserbauwerk ---
        # --- to do list of attributes superclass ---
        # --- ezg_parameter_mouse1 ---
        # --- to do list of attributes subclass ---

        # --- _bwrel_ ---
        # --- to do list of bwrel ---


        # --- _rel_ ---
        # --- to do list of rel ---

        # AVAILABLE FIELDS IN metaattribute

        # --- metaattribute ---
        # datenherr, datenlieferant, letzte_aenderung, sia405_baseclass_metaattribute, t_id, t_seq

        # --- _rel_ ---
        # sia405_baseclass_metaattribute__REL
        param_ca_mouse1 = create_or_update(
            QGEP.param_ca_mouse1,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- surface_runoff_parameters ---
            **surface_runoff_parameters_common(row),
            # --- param_ca_mouse1 ---
            dry_wheather_flow=row.trockenwetteranfall,
            flow_path_length=row.fliessweglaenge,
            flow_path_slope=row.fliessweggefaelle,
            population_equivalent=row.einwohnergleichwert,
            surface_ca_mouse=row.flaeche,
            usage=row.nutzungsart,

        )
        qgep_session.add(param_ca_mouse1)
        print(".", end="")
    logger.info("done")

# added logger info
    logger.info("Importing ABWASSER.erhaltungsereignis, ABWASSER.metaattribute -> QGEP.maintenance_event")
    for row, metaattribute in abwasser_session.query(ABWASSER.erhaltungsereignis, ABWASSER.metaattribute).join(
        ABWASSER.metaattribute
    ):

        logger.warning(
            "QGEP maintenance_event.active_zone has no equivalent in the interlis model. This field will be null."
        )
        maintenance_event = create_or_update(
            QGEP.maintenance_event,
            **base_common(row),
            **metaattribute_common(metaattribute),
            # --- maintenance_event ---
            # active_zone=row.REPLACE_ME,  # TODO : found no matching field for this in interlis, confirm this is ok
            base_data=row.datengrundlage,
            cost=row.kosten,
            data_details=row.detaildaten,
            duration=row.dauer,
            fk_operating_company=row.ausfuehrende_firmaref__REL.obj_id if row.ausfuehrende_firmaref__REL else None,
            identifier=row.bezeichnung,
            kind__REL=get_vl_instance(QGEP.maintenance_event_kind, row.art),
            operator=row.ausfuehrender,
            reason=row.grund,
            remark=row.bemerkung,
            result=row.ergebnis,
            status__REL=get_vl_instance(QGEP.maintenance_event_status, row.astatus),
            time_point=row.zeitpunkt,
        )
        qgep_session.add(maintenance_event)
        print(".", end="")
    logger.info("done")


    logger.info("Importing ABWASSER.erhaltungsereignis_abwasserbauwerkassoc -> QGEP.re_maintenance_event_wastewater_structure")
    for row in abwasser_session.query(ABWASSER.erhaltungsereignis_abwasserbauwerkassoc
    ):
        re_maintenance_event_wastewater_structure = create_or_update(
            QGEP.re_maintenance_event_wastewater_structure,
            #**base_common(row),
            #**metaattribute_common(metaattribute),
            # --- maintenance_event_wastewater_structure ---
            fk_maintenance_event=get_pk(row.erhaltungsereignis_abwasserbauwerkassocref__REL),
            fk_wastewater_structure=get_pk(row.abwasserbauwerkref__REL),
        )
        qgep_session.add(re_maintenance_event_wastewater_structure)
        print(".", end="")
    logger.info("done")



    # Calling the precommit callback if provided, allowing to filter before final import
    if precommit_callback:
        precommit_callback(qgep_session)
        logger.info("precommit_callback(qgep_session)")
        # improve user feedback
        logger.info("Comitting qgep_session (precommit_callback) - please be patient ...")
    else:
        # improve user feedback
        logger.info("Comitting qgep_session - please be patient (else) ...")
        qgep_session.commit()
        logger.info("qgep_session sucessfully committed")
        qgep_session.close()
        logger.info("qgep_session closed")
    abwasser_session.close()
    logger.info("abwasser_session closed")
