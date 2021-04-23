from sqlalchemy.orm import Session
from geoalchemy2.functions import ST_Transform, ST_Force2D

from .. import utils

from .model_qgep import get_qgep_model
from .model_abwasser import get_abwasser_model


def qgep_export():

    QGEP = get_qgep_model()
    ABWASSER = get_abwasser_model()

    qgep_session = Session(utils.sqlalchemy.create_engine(), autocommit=False, autoflush=False)
    abwasser_session = Session(utils.sqlalchemy.create_engine(), autocommit=False, autoflush=False)
    tid_maker = utils.ili2db.TidMaker(id_attribute='obj_id')

    def create_metaattributes(row, session):
        metaattribute = ABWASSER.metaattribute(
            # FIELDS TO MAP TO ABWASSER.metaattribute
            # --- metaattribute ---
            # datenherr=row.REPLACE_ME,
            # datenlieferant=row.REPLACE_ME,
            # letzte_aenderung=row.REPLACE_ME,
            # sia405_baseclass_metaattribute=row.REPLACE_ME,
            # t_id=row.REPLACE_ME
            # t_seq=row.REPLACE_ME,
        )
        session.add(metaattribute)

    print("Exporting QGEP.organisation -> ABWASSER.organisation, ABWASSER.metaattribute")
    for row in qgep_session.query(QGEP.organisation):

        # organisation --- organisation.obj_id, organisation.identifier, organisation.remark, organisation.uid, organisation.last_modification, organisation.fk_dataowner, organisation.fk_provider
        # _bwrel_ --- organisation.measuring_device__BWREL_fk_dataowner, organisation.measuring_device__BWREL_fk_provider, organisation.fish_pass__BWREL_fk_dataowner, organisation.fish_pass__BWREL_fk_provider, organisation.hazard_source__BWREL_fk_owner, organisation.hazard_source__BWREL_fk_dataowner, organisation.hazard_source__BWREL_fk_provider, organisation.water_catchment__BWREL_fk_dataowner, organisation.water_catchment__BWREL_fk_provider, organisation.accident__BWREL_fk_provider, organisation.accident__BWREL_fk_dataowner, organisation.waste_water_treatment_plant__BWREL_obj_id, organisation.overflow_char__BWREL_fk_dataowner, organisation.overflow_char__BWREL_fk_provider, organisation.hydr_geom_relation__BWREL_fk_provider, organisation.hydr_geom_relation__BWREL_fk_dataowner, organisation.sector_water_body__BWREL_fk_provider, organisation.sector_water_body__BWREL_fk_dataowner, organisation.waste_water_association__BWREL_obj_id, organisation.canton__BWREL_obj_id, organisation.wwtp_energy_use__BWREL_fk_provider, organisation.wwtp_energy_use__BWREL_fk_dataowner, organisation.river_bed__BWREL_fk_dataowner, organisation.river_bed__BWREL_fk_provider, organisation.aquifier__BWREL_fk_dataowner, organisation.aquifier__BWREL_fk_provider, organisation.retention_body__BWREL_fk_dataowner, organisation.retention_body__BWREL_fk_provider, organisation.substance__BWREL_fk_provider, organisation.substance__BWREL_fk_dataowner, organisation.river_bank__BWREL_fk_provider, organisation.river_bank__BWREL_fk_dataowner, organisation.zone__BWREL_fk_dataowner, organisation.zone__BWREL_fk_provider, organisation.throttle_shut_off_unit__BWREL_fk_dataowner, organisation.throttle_shut_off_unit__BWREL_fk_provider, organisation.waste_water_treatment__BWREL_fk_provider, organisation.waste_water_treatment__BWREL_fk_dataowner, organisation.control_center__BWREL_fk_dataowner, organisation.control_center__BWREL_fk_provider, organisation.organisation__BWREL_fk_dataowner, organisation.organisation__BWREL_fk_provider, organisation.surface_runoff_parameters__BWREL_fk_provider, organisation.surface_runoff_parameters__BWREL_fk_dataowner, organisation.pipe_profile__BWREL_fk_dataowner, organisation.pipe_profile__BWREL_fk_provider, organisation.surface_water_bodies__BWREL_fk_dataowner, organisation.surface_water_bodies__BWREL_fk_provider, organisation.txt_symbol__BWREL_fk_provider, organisation.txt_symbol__BWREL_fk_dataowner, organisation.wastewater_structure__BWREL_fk_operator, organisation.wastewater_structure__BWREL_fk_dataowner, organisation.wastewater_structure__BWREL_fk_owner, organisation.wastewater_structure__BWREL_fk_provider, organisation.wastewater_networkelement__BWREL_fk_dataowner, organisation.wastewater_networkelement__BWREL_fk_provider, organisation.administrative_office__BWREL_obj_id, organisation.hydraulic_char_data__BWREL_fk_provider, organisation.hydraulic_char_data__BWREL_fk_dataowner, organisation.overflow__BWREL_fk_dataowner, organisation.overflow__BWREL_fk_provider, organisation.measurement_result__BWREL_fk_provider, organisation.measurement_result__BWREL_fk_dataowner, organisation.reach_point__BWREL_fk_dataowner, organisation.reach_point__BWREL_fk_provider, organisation.data_media__BWREL_fk_dataowner, organisation.data_media__BWREL_fk_provider, organisation.measurement_series__BWREL_fk_dataowner, organisation.measurement_series__BWREL_fk_provider, organisation.municipality__BWREL_obj_id, organisation.bathing_area__BWREL_fk_dataowner, organisation.bathing_area__BWREL_fk_provider, organisation.connection_object__BWREL_fk_dataowner, organisation.connection_object__BWREL_fk_operator, organisation.connection_object__BWREL_fk_owner, organisation.connection_object__BWREL_fk_provider, organisation.file__BWREL_fk_provider, organisation.file__BWREL_fk_dataowner, organisation.measuring_point__BWREL_fk_provider, organisation.measuring_point__BWREL_fk_dataowner, organisation.measuring_point__BWREL_fk_operator, organisation.hydr_geometry__BWREL_fk_provider, organisation.hydr_geometry__BWREL_fk_dataowner, organisation.water_control_structure__BWREL_fk_provider, organisation.water_control_structure__BWREL_fk_dataowner, organisation.hq_relation__BWREL_fk_provider, organisation.hq_relation__BWREL_fk_dataowner, organisation.maintenance_event__BWREL_fk_operating_company, organisation.maintenance_event__BWREL_fk_provider, organisation.maintenance_event__BWREL_fk_dataowner, organisation.wastewater_structure_symbol__BWREL_fk_provider, organisation.wastewater_structure_symbol__BWREL_fk_dataowner, organisation.structure_part__BWREL_fk_provider, organisation.structure_part__BWREL_fk_dataowner, organisation.catchment_area__BWREL_fk_provider, organisation.catchment_area__BWREL_fk_dataowner, organisation.cooperative__BWREL_obj_id, organisation.mutation__BWREL_fk_provider, organisation.mutation__BWREL_fk_dataowner, organisation.damage__BWREL_fk_provider, organisation.damage__BWREL_fk_dataowner, organisation.profile_geometry__BWREL_fk_provider, organisation.profile_geometry__BWREL_fk_dataowner, organisation.private__BWREL_obj_id, organisation.water_course_segment__BWREL_fk_provider, organisation.water_course_segment__BWREL_fk_dataowner, organisation.sludge_treatment__BWREL_fk_provider, organisation.sludge_treatment__BWREL_fk_dataowner, organisation.mechanical_pretreatment__BWREL_fk_provider, organisation.mechanical_pretreatment__BWREL_fk_dataowner
        # _rel_ --- organisation.fk_dataowner__REL, organisation.fk_provider__REL

        organisation = ABWASSER.organisation(

            # --- baseclass ---
            # t_type=row.REPLACE_ME,  # VARCHAR(60)
            # t_ili_tid=row.REPLACE_ME,  # VARCHAR(200)

            # --- sia405_baseclass ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)

            # --- organisation ---
            # t_id=row.REPLACE_ME,  # BIGINT
            # bemerkung=row.REPLACE_ME,  # VARCHAR(80)
            # bezeichnung=row.REPLACE_ME,  # VARCHAR(80)
            # auid=row.REPLACE_ME,  # VARCHAR(12)
        )
        abwasser_session.add(organisation)
        create_metaattributes(row, session)
        print(".", end="")
    print("done")

    print("Exporting QGEP.channel -> ABWASSER.kanal, ABWASSER.metaattribute")
    for row in qgep_session.query(QGEP.channel):

        # wastewater_structure --- channel.accessibility, channel.contract_section, channel.detail_geometry_geometry, channel.financing, channel.gross_costs, channel.identifier, channel.inspection_interval, channel.location_name, channel.records, channel.remark, channel.renovation_necessity, channel.replacement_value, channel.rv_base_year, channel.rv_construction_type, channel.status, channel.structure_condition, channel.subsidies, channel.year_of_construction, channel.year_of_replacement, channel.last_modification, channel.fk_dataowner, channel.fk_provider, channel.fk_owner, channel.fk_operator, channel._usage_current, channel._function_hierarchic, channel._label, channel.fk_main_cover, channel._depth, channel.fk_main_wastewater_node, channel._cover_label, channel._input_label, channel._output_label, channel._bottom_label
        # channel --- channel.obj_id, channel.bedding_encasement, channel.connection_type, channel.function_hierarchic, channel.function_hydraulic, channel.jetting_interval, channel.pipe_length, channel.usage_current, channel.usage_planned
        # _bwrel_ --- channel.wwtp_structure_kind__BWREL_obj_id, channel.re_maintenance_event_wastewater_structure__BWREL_fk_wastewater_structure, channel.wastewater_structure_text__BWREL_fk_wastewater_structure, channel.txt_text__BWREL_fk_wastewater_structure, channel.txt_symbol__BWREL_fk_wastewater_structure, channel.wastewater_networkelement__BWREL_fk_wastewater_structure, channel.measuring_point__BWREL_fk_wastewater_structure, channel.wastewater_structure_symbol__BWREL_fk_wastewater_structure, channel.structure_part__BWREL_fk_wastewater_structure, channel.mechanical_pretreatment__BWREL_fk_wastewater_structure
        # _rel_ --- channel.function_hierarchic__REL, channel.bedding_encasement__REL, channel.usage_current__REL, channel.function_hydraulic__REL, channel.usage_planned__REL, channel.connection_type__REL, channel.fk_operator__REL, channel.accessibility__REL, channel.status__REL, channel.structure_condition__REL, channel.renovation_necessity__REL, channel.fk_main_wastewater_node__REL, channel.fk_dataowner__REL, channel.fk_owner__REL, channel.rv_construction_type__REL, channel.fk_main_cover__REL, channel.fk_provider__REL, channel.financing__REL

        kanal = ABWASSER.kanal(

            # --- baseclass ---
            # t_type=row.REPLACE_ME,  # VARCHAR(60)
            # t_ili_tid=row.REPLACE_ME,  # VARCHAR(200)

            # --- sia405_baseclass ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)

            # --- abwasserbauwerk ---
            # akten=row.REPLACE_ME,  # VARCHAR(255)
            # baujahr=row.REPLACE_ME,  # INTEGER
            # baulicherzustand=row.REPLACE_ME,  # VARCHAR(255)
            # baulos=row.REPLACE_ME,  # VARCHAR(50)
            # bemerkung=row.REPLACE_ME,  # VARCHAR(80)
            # bezeichnung=row.REPLACE_ME,  # VARCHAR(20)
            # bruttokosten=row.REPLACE_ME,  # NUMERIC(10, 2)
            # detailgeometrie=row.REPLACE_ME,  # geometry(CURVEPOLYGON,2056)
            # ersatzjahr=row.REPLACE_ME,  # INTEGER
            # finanzierung=row.REPLACE_ME,  # VARCHAR(255)
            # inspektionsintervall=row.REPLACE_ME,  # NUMERIC(4, 2)
            # sanierungsbedarf=row.REPLACE_ME,  # VARCHAR(255)
            # standortname=row.REPLACE_ME,  # VARCHAR(50)
            # astatus=row.REPLACE_ME,  # VARCHAR(255)
            # subventionen=row.REPLACE_ME,  # NUMERIC(10, 2)
            # wbw_basisjahr=row.REPLACE_ME,  # INTEGER
            # wbw_bauart=row.REPLACE_ME,  # VARCHAR(255)
            # wiederbeschaffungswert=row.REPLACE_ME,  # NUMERIC(10, 2)
            # zugaenglichkeit=row.REPLACE_ME,  # VARCHAR(255)
            # betreiberref=row.REPLACE_ME,  # BIGINT
            # eigentuemerref=row.REPLACE_ME,  # BIGINT

            # --- kanal ---
            # t_id=row.REPLACE_ME,  # BIGINT
            # bettung_umhuellung=row.REPLACE_ME,  # VARCHAR(255)
            # funktionhierarchisch=row.REPLACE_ME,  # VARCHAR(255)
            # funktionhydraulisch=row.REPLACE_ME,  # VARCHAR(255)
            # nutzungsart_geplant=row.REPLACE_ME,  # VARCHAR(255)
            # nutzungsart_ist=row.REPLACE_ME,  # VARCHAR(255)
            # rohrlaenge=row.REPLACE_ME,  # NUMERIC(7, 2)
            # spuelintervall=row.REPLACE_ME,  # NUMERIC(4, 2)
            # verbindungsart=row.REPLACE_ME,  # VARCHAR(255)
        )
        abwasser_session.add(kanal)
        create_metaattributes(row, session)
        print(".", end="")
    print("done")

    print("Exporting QGEP.manhole -> ABWASSER.normschacht, ABWASSER.metaattribute")
    for row in qgep_session.query(QGEP.manhole):

        # wastewater_structure --- manhole.accessibility, manhole.contract_section, manhole.detail_geometry_geometry, manhole.financing, manhole.gross_costs, manhole.identifier, manhole.inspection_interval, manhole.location_name, manhole.records, manhole.remark, manhole.renovation_necessity, manhole.replacement_value, manhole.rv_base_year, manhole.rv_construction_type, manhole.status, manhole.structure_condition, manhole.subsidies, manhole.year_of_construction, manhole.year_of_replacement, manhole.last_modification, manhole.fk_dataowner, manhole.fk_provider, manhole.fk_owner, manhole.fk_operator, manhole._usage_current, manhole._function_hierarchic, manhole._label, manhole.fk_main_cover, manhole._depth, manhole.fk_main_wastewater_node, manhole._cover_label, manhole._input_label, manhole._output_label, manhole._bottom_label
        # manhole --- manhole.obj_id, manhole.dimension1, manhole.dimension2, manhole.function, manhole.material, manhole.surface_inflow, manhole._orientation
        # _bwrel_ --- manhole.wwtp_structure_kind__BWREL_obj_id, manhole.re_maintenance_event_wastewater_structure__BWREL_fk_wastewater_structure, manhole.wastewater_structure_text__BWREL_fk_wastewater_structure, manhole.txt_text__BWREL_fk_wastewater_structure, manhole.txt_symbol__BWREL_fk_wastewater_structure, manhole.wastewater_networkelement__BWREL_fk_wastewater_structure, manhole.measuring_point__BWREL_fk_wastewater_structure, manhole.wastewater_structure_symbol__BWREL_fk_wastewater_structure, manhole.structure_part__BWREL_fk_wastewater_structure, manhole.mechanical_pretreatment__BWREL_fk_wastewater_structure
        # _rel_ --- manhole.material__REL, manhole.function__REL, manhole.surface_inflow__REL, manhole.fk_operator__REL, manhole.accessibility__REL, manhole.status__REL, manhole.structure_condition__REL, manhole.renovation_necessity__REL, manhole.fk_main_wastewater_node__REL, manhole.fk_dataowner__REL, manhole.fk_owner__REL, manhole.rv_construction_type__REL, manhole.fk_main_cover__REL, manhole.fk_provider__REL, manhole.financing__REL

        normschacht = ABWASSER.normschacht(

            # --- baseclass ---
            # t_type=row.REPLACE_ME,  # VARCHAR(60)
            # t_ili_tid=row.REPLACE_ME,  # VARCHAR(200)

            # --- sia405_baseclass ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)

            # --- abwasserbauwerk ---
            # akten=row.REPLACE_ME,  # VARCHAR(255)
            # baujahr=row.REPLACE_ME,  # INTEGER
            # baulicherzustand=row.REPLACE_ME,  # VARCHAR(255)
            # baulos=row.REPLACE_ME,  # VARCHAR(50)
            # bemerkung=row.REPLACE_ME,  # VARCHAR(80)
            # bezeichnung=row.REPLACE_ME,  # VARCHAR(20)
            # bruttokosten=row.REPLACE_ME,  # NUMERIC(10, 2)
            # detailgeometrie=row.REPLACE_ME,  # geometry(CURVEPOLYGON,2056)
            # ersatzjahr=row.REPLACE_ME,  # INTEGER
            # finanzierung=row.REPLACE_ME,  # VARCHAR(255)
            # inspektionsintervall=row.REPLACE_ME,  # NUMERIC(4, 2)
            # sanierungsbedarf=row.REPLACE_ME,  # VARCHAR(255)
            # standortname=row.REPLACE_ME,  # VARCHAR(50)
            # astatus=row.REPLACE_ME,  # VARCHAR(255)
            # subventionen=row.REPLACE_ME,  # NUMERIC(10, 2)
            # wbw_basisjahr=row.REPLACE_ME,  # INTEGER
            # wbw_bauart=row.REPLACE_ME,  # VARCHAR(255)
            # wiederbeschaffungswert=row.REPLACE_ME,  # NUMERIC(10, 2)
            # zugaenglichkeit=row.REPLACE_ME,  # VARCHAR(255)
            # betreiberref=row.REPLACE_ME,  # BIGINT
            # eigentuemerref=row.REPLACE_ME,  # BIGINT

            # --- normschacht ---
            # t_id=row.REPLACE_ME,  # BIGINT
            # dimension1=row.REPLACE_ME,  # INTEGER
            # dimension2=row.REPLACE_ME,  # INTEGER
            # funktion=row.REPLACE_ME,  # VARCHAR(255)
            # material=row.REPLACE_ME,  # VARCHAR(255)
            # oberflaechenzulauf=row.REPLACE_ME,  # VARCHAR(255)
        )
        abwasser_session.add(normschacht)
        create_metaattributes(row, session)
        print(".", end="")
    print("done")

    print("Exporting QGEP.discharge_point -> ABWASSER.einleitstelle, ABWASSER.metaattribute")
    for row in qgep_session.query(QGEP.discharge_point):

        # wastewater_structure --- discharge_point.accessibility, discharge_point.contract_section, discharge_point.detail_geometry_geometry, discharge_point.financing, discharge_point.gross_costs, discharge_point.identifier, discharge_point.inspection_interval, discharge_point.location_name, discharge_point.records, discharge_point.remark, discharge_point.renovation_necessity, discharge_point.replacement_value, discharge_point.rv_base_year, discharge_point.rv_construction_type, discharge_point.status, discharge_point.structure_condition, discharge_point.subsidies, discharge_point.year_of_construction, discharge_point.year_of_replacement, discharge_point.last_modification, discharge_point.fk_dataowner, discharge_point.fk_provider, discharge_point.fk_owner, discharge_point.fk_operator, discharge_point._usage_current, discharge_point._function_hierarchic, discharge_point._label, discharge_point.fk_main_cover, discharge_point._depth, discharge_point.fk_main_wastewater_node, discharge_point._cover_label, discharge_point._input_label, discharge_point._output_label, discharge_point._bottom_label
        # discharge_point --- discharge_point.obj_id, discharge_point.highwater_level, discharge_point.relevance, discharge_point.terrain_level, discharge_point.upper_elevation, discharge_point.waterlevel_hydraulic, discharge_point.fk_sector_water_body
        # _bwrel_ --- discharge_point.wwtp_structure_kind__BWREL_obj_id, discharge_point.re_maintenance_event_wastewater_structure__BWREL_fk_wastewater_structure, discharge_point.wastewater_structure_text__BWREL_fk_wastewater_structure, discharge_point.txt_text__BWREL_fk_wastewater_structure, discharge_point.txt_symbol__BWREL_fk_wastewater_structure, discharge_point.wastewater_networkelement__BWREL_fk_wastewater_structure, discharge_point.measuring_point__BWREL_fk_wastewater_structure, discharge_point.wastewater_structure_symbol__BWREL_fk_wastewater_structure, discharge_point.structure_part__BWREL_fk_wastewater_structure, discharge_point.mechanical_pretreatment__BWREL_fk_wastewater_structure
        # _rel_ --- discharge_point.relevance__REL, discharge_point.fk_sector_water_body__REL, discharge_point.fk_operator__REL, discharge_point.accessibility__REL, discharge_point.status__REL, discharge_point.structure_condition__REL, discharge_point.renovation_necessity__REL, discharge_point.fk_main_wastewater_node__REL, discharge_point.fk_dataowner__REL, discharge_point.fk_owner__REL, discharge_point.rv_construction_type__REL, discharge_point.fk_main_cover__REL, discharge_point.fk_provider__REL, discharge_point.financing__REL

        einleitstelle = ABWASSER.einleitstelle(

            # --- baseclass ---
            # t_type=row.REPLACE_ME,  # VARCHAR(60)
            # t_ili_tid=row.REPLACE_ME,  # VARCHAR(200)

            # --- sia405_baseclass ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)

            # --- abwasserbauwerk ---
            # akten=row.REPLACE_ME,  # VARCHAR(255)
            # baujahr=row.REPLACE_ME,  # INTEGER
            # baulicherzustand=row.REPLACE_ME,  # VARCHAR(255)
            # baulos=row.REPLACE_ME,  # VARCHAR(50)
            # bemerkung=row.REPLACE_ME,  # VARCHAR(80)
            # bezeichnung=row.REPLACE_ME,  # VARCHAR(20)
            # bruttokosten=row.REPLACE_ME,  # NUMERIC(10, 2)
            # detailgeometrie=row.REPLACE_ME,  # geometry(CURVEPOLYGON,2056)
            # ersatzjahr=row.REPLACE_ME,  # INTEGER
            # finanzierung=row.REPLACE_ME,  # VARCHAR(255)
            # inspektionsintervall=row.REPLACE_ME,  # NUMERIC(4, 2)
            # sanierungsbedarf=row.REPLACE_ME,  # VARCHAR(255)
            # standortname=row.REPLACE_ME,  # VARCHAR(50)
            # astatus=row.REPLACE_ME,  # VARCHAR(255)
            # subventionen=row.REPLACE_ME,  # NUMERIC(10, 2)
            # wbw_basisjahr=row.REPLACE_ME,  # INTEGER
            # wbw_bauart=row.REPLACE_ME,  # VARCHAR(255)
            # wiederbeschaffungswert=row.REPLACE_ME,  # NUMERIC(10, 2)
            # zugaenglichkeit=row.REPLACE_ME,  # VARCHAR(255)
            # betreiberref=row.REPLACE_ME,  # BIGINT
            # eigentuemerref=row.REPLACE_ME,  # BIGINT

            # --- einleitstelle ---
            # t_id=row.REPLACE_ME,  # BIGINT
            # hochwasserkote=row.REPLACE_ME,  # NUMERIC(7, 3)
            # relevanz=row.REPLACE_ME,  # VARCHAR(255)
            # terrainkote=row.REPLACE_ME,  # NUMERIC(7, 3)
            # wasserspiegel_hydraulik=row.REPLACE_ME,  # NUMERIC(7, 3)
        )
        abwasser_session.add(einleitstelle)
        create_metaattributes(row, session)
        print(".", end="")
    print("done")

    print("Exporting QGEP.special_structure -> ABWASSER.spezialbauwerk, ABWASSER.metaattribute")
    for row in qgep_session.query(QGEP.special_structure):

        # wastewater_structure --- special_structure.accessibility, special_structure.contract_section, special_structure.detail_geometry_geometry, special_structure.financing, special_structure.gross_costs, special_structure.identifier, special_structure.inspection_interval, special_structure.location_name, special_structure.records, special_structure.remark, special_structure.renovation_necessity, special_structure.replacement_value, special_structure.rv_base_year, special_structure.rv_construction_type, special_structure.status, special_structure.structure_condition, special_structure.subsidies, special_structure.year_of_construction, special_structure.year_of_replacement, special_structure.last_modification, special_structure.fk_dataowner, special_structure.fk_provider, special_structure.fk_owner, special_structure.fk_operator, special_structure._usage_current, special_structure._function_hierarchic, special_structure._label, special_structure.fk_main_cover, special_structure._depth, special_structure.fk_main_wastewater_node, special_structure._cover_label, special_structure._input_label, special_structure._output_label, special_structure._bottom_label
        # special_structure --- special_structure.obj_id, special_structure.bypass, special_structure.emergency_spillway, special_structure.function, special_structure.stormwater_tank_arrangement, special_structure.upper_elevation
        # _bwrel_ --- special_structure.wwtp_structure_kind__BWREL_obj_id, special_structure.re_maintenance_event_wastewater_structure__BWREL_fk_wastewater_structure, special_structure.wastewater_structure_text__BWREL_fk_wastewater_structure, special_structure.txt_text__BWREL_fk_wastewater_structure, special_structure.txt_symbol__BWREL_fk_wastewater_structure, special_structure.wastewater_networkelement__BWREL_fk_wastewater_structure, special_structure.measuring_point__BWREL_fk_wastewater_structure, special_structure.wastewater_structure_symbol__BWREL_fk_wastewater_structure, special_structure.structure_part__BWREL_fk_wastewater_structure, special_structure.mechanical_pretreatment__BWREL_fk_wastewater_structure
        # _rel_ --- special_structure.emergency_spillway__REL, special_structure.function__REL, special_structure.bypass__REL, special_structure.stormwater_tank_arrangement__REL, special_structure.fk_operator__REL, special_structure.accessibility__REL, special_structure.status__REL, special_structure.structure_condition__REL, special_structure.renovation_necessity__REL, special_structure.fk_main_wastewater_node__REL, special_structure.fk_dataowner__REL, special_structure.fk_owner__REL, special_structure.rv_construction_type__REL, special_structure.fk_main_cover__REL, special_structure.fk_provider__REL, special_structure.financing__REL

        spezialbauwerk = ABWASSER.spezialbauwerk(

            # --- baseclass ---
            # t_type=row.REPLACE_ME,  # VARCHAR(60)
            # t_ili_tid=row.REPLACE_ME,  # VARCHAR(200)

            # --- sia405_baseclass ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)

            # --- abwasserbauwerk ---
            # akten=row.REPLACE_ME,  # VARCHAR(255)
            # baujahr=row.REPLACE_ME,  # INTEGER
            # baulicherzustand=row.REPLACE_ME,  # VARCHAR(255)
            # baulos=row.REPLACE_ME,  # VARCHAR(50)
            # bemerkung=row.REPLACE_ME,  # VARCHAR(80)
            # bezeichnung=row.REPLACE_ME,  # VARCHAR(20)
            # bruttokosten=row.REPLACE_ME,  # NUMERIC(10, 2)
            # detailgeometrie=row.REPLACE_ME,  # geometry(CURVEPOLYGON,2056)
            # ersatzjahr=row.REPLACE_ME,  # INTEGER
            # finanzierung=row.REPLACE_ME,  # VARCHAR(255)
            # inspektionsintervall=row.REPLACE_ME,  # NUMERIC(4, 2)
            # sanierungsbedarf=row.REPLACE_ME,  # VARCHAR(255)
            # standortname=row.REPLACE_ME,  # VARCHAR(50)
            # astatus=row.REPLACE_ME,  # VARCHAR(255)
            # subventionen=row.REPLACE_ME,  # NUMERIC(10, 2)
            # wbw_basisjahr=row.REPLACE_ME,  # INTEGER
            # wbw_bauart=row.REPLACE_ME,  # VARCHAR(255)
            # wiederbeschaffungswert=row.REPLACE_ME,  # NUMERIC(10, 2)
            # zugaenglichkeit=row.REPLACE_ME,  # VARCHAR(255)
            # betreiberref=row.REPLACE_ME,  # BIGINT
            # eigentuemerref=row.REPLACE_ME,  # BIGINT

            # --- spezialbauwerk ---
            # t_id=row.REPLACE_ME,  # BIGINT
            # bypass=row.REPLACE_ME,  # VARCHAR(255)
            # funktion=row.REPLACE_ME,  # VARCHAR(255)
            # notueberlauf=row.REPLACE_ME,  # VARCHAR(255)
            # regenbecken_anordnung=row.REPLACE_ME,  # VARCHAR(255)
        )
        abwasser_session.add(spezialbauwerk)
        create_metaattributes(row, session)
        print(".", end="")
    print("done")

    print("Exporting QGEP.infiltration_installation -> ABWASSER.versickerungsanlage, ABWASSER.metaattribute")
    for row in qgep_session.query(QGEP.infiltration_installation):

        # wastewater_structure --- infiltration_installation.accessibility, infiltration_installation.contract_section, infiltration_installation.detail_geometry_geometry, infiltration_installation.financing, infiltration_installation.gross_costs, infiltration_installation.identifier, infiltration_installation.inspection_interval, infiltration_installation.location_name, infiltration_installation.records, infiltration_installation.remark, infiltration_installation.renovation_necessity, infiltration_installation.replacement_value, infiltration_installation.rv_base_year, infiltration_installation.rv_construction_type, infiltration_installation.status, infiltration_installation.structure_condition, infiltration_installation.subsidies, infiltration_installation.year_of_construction, infiltration_installation.year_of_replacement, infiltration_installation.last_modification, infiltration_installation.fk_dataowner, infiltration_installation.fk_provider, infiltration_installation.fk_owner, infiltration_installation.fk_operator, infiltration_installation._usage_current, infiltration_installation._function_hierarchic, infiltration_installation._label, infiltration_installation.fk_main_cover, infiltration_installation._depth, infiltration_installation.fk_main_wastewater_node, infiltration_installation._cover_label, infiltration_installation._input_label, infiltration_installation._output_label, infiltration_installation._bottom_label
        # infiltration_installation --- infiltration_installation.obj_id, infiltration_installation.absorption_capacity, infiltration_installation.defects, infiltration_installation.dimension1, infiltration_installation.dimension2, infiltration_installation.distance_to_aquifer, infiltration_installation.effective_area, infiltration_installation.emergency_spillway, infiltration_installation.kind, infiltration_installation.labeling, infiltration_installation.seepage_utilization, infiltration_installation.upper_elevation, infiltration_installation.vehicle_access, infiltration_installation.watertightness, infiltration_installation.fk_aquifier
        # _bwrel_ --- infiltration_installation.wwtp_structure_kind__BWREL_obj_id, infiltration_installation.re_maintenance_event_wastewater_structure__BWREL_fk_wastewater_structure, infiltration_installation.retention_body__BWREL_fk_infiltration_installation, infiltration_installation.wastewater_structure_text__BWREL_fk_wastewater_structure, infiltration_installation.txt_text__BWREL_fk_wastewater_structure, infiltration_installation.txt_symbol__BWREL_fk_wastewater_structure, infiltration_installation.wastewater_networkelement__BWREL_fk_wastewater_structure, infiltration_installation.measuring_point__BWREL_fk_wastewater_structure, infiltration_installation.wastewater_structure_symbol__BWREL_fk_wastewater_structure, infiltration_installation.structure_part__BWREL_fk_wastewater_structure, infiltration_installation.mechanical_pretreatment__BWREL_fk_infiltration_installation, infiltration_installation.mechanical_pretreatment__BWREL_fk_wastewater_structure
        # _rel_ --- infiltration_installation.defects__REL, infiltration_installation.emergency_spillway__REL, infiltration_installation.vehicle_access__REL, infiltration_installation.watertightness__REL, infiltration_installation.kind__REL, infiltration_installation.labeling__REL, infiltration_installation.fk_aquifier__REL, infiltration_installation.seepage_utilization__REL, infiltration_installation.fk_operator__REL, infiltration_installation.accessibility__REL, infiltration_installation.status__REL, infiltration_installation.structure_condition__REL, infiltration_installation.renovation_necessity__REL, infiltration_installation.fk_main_wastewater_node__REL, infiltration_installation.fk_dataowner__REL, infiltration_installation.fk_owner__REL, infiltration_installation.rv_construction_type__REL, infiltration_installation.fk_main_cover__REL, infiltration_installation.fk_provider__REL, infiltration_installation.financing__REL

        versickerungsanlage = ABWASSER.versickerungsanlage(

            # --- baseclass ---
            # t_type=row.REPLACE_ME,  # VARCHAR(60)
            # t_ili_tid=row.REPLACE_ME,  # VARCHAR(200)

            # --- sia405_baseclass ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)

            # --- abwasserbauwerk ---
            # akten=row.REPLACE_ME,  # VARCHAR(255)
            # baujahr=row.REPLACE_ME,  # INTEGER
            # baulicherzustand=row.REPLACE_ME,  # VARCHAR(255)
            # baulos=row.REPLACE_ME,  # VARCHAR(50)
            # bemerkung=row.REPLACE_ME,  # VARCHAR(80)
            # bezeichnung=row.REPLACE_ME,  # VARCHAR(20)
            # bruttokosten=row.REPLACE_ME,  # NUMERIC(10, 2)
            # detailgeometrie=row.REPLACE_ME,  # geometry(CURVEPOLYGON,2056)
            # ersatzjahr=row.REPLACE_ME,  # INTEGER
            # finanzierung=row.REPLACE_ME,  # VARCHAR(255)
            # inspektionsintervall=row.REPLACE_ME,  # NUMERIC(4, 2)
            # sanierungsbedarf=row.REPLACE_ME,  # VARCHAR(255)
            # standortname=row.REPLACE_ME,  # VARCHAR(50)
            # astatus=row.REPLACE_ME,  # VARCHAR(255)
            # subventionen=row.REPLACE_ME,  # NUMERIC(10, 2)
            # wbw_basisjahr=row.REPLACE_ME,  # INTEGER
            # wbw_bauart=row.REPLACE_ME,  # VARCHAR(255)
            # wiederbeschaffungswert=row.REPLACE_ME,  # NUMERIC(10, 2)
            # zugaenglichkeit=row.REPLACE_ME,  # VARCHAR(255)
            # betreiberref=row.REPLACE_ME,  # BIGINT
            # eigentuemerref=row.REPLACE_ME,  # BIGINT

            # --- versickerungsanlage ---
            # t_id=row.REPLACE_ME,  # BIGINT
            # art=row.REPLACE_ME,  # VARCHAR(255)
            # beschriftung=row.REPLACE_ME,  # VARCHAR(255)
            # dimension1=row.REPLACE_ME,  # INTEGER
            # dimension2=row.REPLACE_ME,  # INTEGER
            # gwdistanz=row.REPLACE_ME,  # NUMERIC(7, 2)
            # maengel=row.REPLACE_ME,  # VARCHAR(255)
            # notueberlauf=row.REPLACE_ME,  # VARCHAR(255)
            # saugwagen=row.REPLACE_ME,  # VARCHAR(255)
            # schluckvermoegen=row.REPLACE_ME,  # NUMERIC(9, 3)
            # versickerungswasser=row.REPLACE_ME,  # VARCHAR(255)
            # wasserdichtheit=row.REPLACE_ME,  # VARCHAR(255)
            # wirksameflaeche=row.REPLACE_ME,  # NUMERIC(8, 2)
        )
        abwasser_session.add(versickerungsanlage)
        create_metaattributes(row, session)
        print(".", end="")
    print("done")

    print("Exporting QGEP.pipe_profile -> ABWASSER.rohrprofil, ABWASSER.metaattribute")
    for row in qgep_session.query(QGEP.pipe_profile):

        # pipe_profile --- pipe_profile.obj_id, pipe_profile.height_width_ratio, pipe_profile.identifier, pipe_profile.profile_type, pipe_profile.remark, pipe_profile.last_modification, pipe_profile.fk_dataowner, pipe_profile.fk_provider
        # _bwrel_ --- pipe_profile.profile_geometry__BWREL_fk_pipe_profile, pipe_profile.reach__BWREL_fk_pipe_profile
        # _rel_ --- pipe_profile.fk_dataowner__REL, pipe_profile.fk_provider__REL, pipe_profile.profile_type__REL

        rohrprofil = ABWASSER.rohrprofil(

            # --- baseclass ---
            # t_type=row.REPLACE_ME,  # VARCHAR(60)
            # t_ili_tid=row.REPLACE_ME,  # VARCHAR(200)

            # --- sia405_baseclass ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)

            # --- rohrprofil ---
            # t_id=row.REPLACE_ME,  # BIGINT
            # bemerkung=row.REPLACE_ME,  # VARCHAR(80)
            # bezeichnung=row.REPLACE_ME,  # VARCHAR(20)
            # hoehenbreitenverhaeltnis=row.REPLACE_ME,  # NUMERIC(5, 2)
            # profiltyp=row.REPLACE_ME,  # VARCHAR(255)
        )
        abwasser_session.add(rohrprofil)
        create_metaattributes(row, session)
        print(".", end="")
    print("done")

    print("Exporting QGEP.reach_point -> ABWASSER.haltungspunkt, ABWASSER.metaattribute")
    for row in qgep_session.query(QGEP.reach_point):

        # reach_point --- reach_point.obj_id, reach_point.elevation_accuracy, reach_point.identifier, reach_point.level, reach_point.outlet_shape, reach_point.position_of_connection, reach_point.remark, reach_point.situation_geometry, reach_point.last_modification, reach_point.fk_dataowner, reach_point.fk_provider, reach_point.fk_wastewater_networkelement
        # _bwrel_ --- reach_point.reach__BWREL_fk_reach_point_to, reach_point.reach__BWREL_fk_reach_point_from, reach_point.examination__BWREL_fk_reach_point
        # _rel_ --- reach_point.outlet_shape__REL, reach_point.elevation_accuracy__REL, reach_point.fk_wastewater_networkelement__REL, reach_point.fk_dataowner__REL, reach_point.fk_provider__REL

        haltungspunkt = ABWASSER.haltungspunkt(

            # --- baseclass ---
            # t_type=row.REPLACE_ME,  # VARCHAR(60)
            # t_ili_tid=row.REPLACE_ME,  # VARCHAR(200)

            # --- sia405_baseclass ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)

            # --- haltungspunkt ---
            # t_id=row.REPLACE_ME,  # BIGINT
            # auslaufform=row.REPLACE_ME,  # VARCHAR(255)
            # bemerkung=row.REPLACE_ME,  # VARCHAR(80)
            # bezeichnung=row.REPLACE_ME,  # VARCHAR(20)
            # hoehengenauigkeit=row.REPLACE_ME,  # VARCHAR(255)
            # kote=row.REPLACE_ME,  # NUMERIC(7, 3)
            # lage=row.REPLACE_ME,  # geometry(POINT,2056)
            # lage_anschluss=row.REPLACE_ME,  # INTEGER
            # abwassernetzelementref=row.REPLACE_ME,  # BIGINT
        )
        abwasser_session.add(haltungspunkt)
        create_metaattributes(row, session)
        print(".", end="")
    print("done")

    print("Exporting QGEP.wastewater_node -> ABWASSER.abwasserknoten, ABWASSER.metaattribute")
    for row in qgep_session.query(QGEP.wastewater_node):

        # wastewater_networkelement --- wastewater_node.identifier, wastewater_node.remark, wastewater_node.last_modification, wastewater_node.fk_dataowner, wastewater_node.fk_provider, wastewater_node.fk_wastewater_structure
        # wastewater_node --- wastewater_node.obj_id, wastewater_node.backflow_level, wastewater_node.bottom_level, wastewater_node.situation_geometry, wastewater_node.fk_hydr_geometry
        # _bwrel_ --- wastewater_node.throttle_shut_off_unit__BWREL_fk_wastewater_node, wastewater_node.wastewater_structure__BWREL_fk_main_wastewater_node, wastewater_node.hydraulic_char_data__BWREL_fk_wastewater_node, wastewater_node.overflow__BWREL_fk_overflow_to, wastewater_node.overflow__BWREL_fk_wastewater_node, wastewater_node.reach_point__BWREL_fk_wastewater_networkelement, wastewater_node.connection_object__BWREL_fk_wastewater_networkelement, wastewater_node.catchment_area__BWREL_fk_wastewater_networkelement_rw_planned, wastewater_node.catchment_area__BWREL_fk_wastewater_networkelement_ww_planned, wastewater_node.catchment_area__BWREL_fk_wastewater_networkelement_rw_current, wastewater_node.catchment_area__BWREL_fk_wastewater_networkelement_ww_current
        # _rel_ --- wastewater_node.fk_hydr_geometry__REL, wastewater_node.fk_wastewater_structure__REL, wastewater_node.fk_dataowner__REL, wastewater_node.fk_provider__REL

        abwasserknoten = ABWASSER.abwasserknoten(

            # --- baseclass ---
            # t_type=row.REPLACE_ME,  # VARCHAR(60)
            # t_ili_tid=row.REPLACE_ME,  # VARCHAR(200)

            # --- sia405_baseclass ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)

            # --- abwassernetzelement ---
            # bemerkung=row.REPLACE_ME,  # VARCHAR(80)
            # bezeichnung=row.REPLACE_ME,  # VARCHAR(20)
            # abwasserbauwerkref=row.REPLACE_ME,  # BIGINT

            # --- abwasserknoten ---
            # t_id=row.REPLACE_ME,  # BIGINT
            # lage=row.REPLACE_ME,  # geometry(POINT,2056)
            # rueckstaukote=row.REPLACE_ME,  # NUMERIC(7, 3)
            # sohlenkote=row.REPLACE_ME,  # NUMERIC(7, 3)
        )
        abwasser_session.add(abwasserknoten)
        create_metaattributes(row, session)
        print(".", end="")
    print("done")

    print("Exporting QGEP.reach -> ABWASSER.haltung, ABWASSER.metaattribute")
    for row in qgep_session.query(QGEP.reach):

        # wastewater_networkelement --- reach.identifier, reach.remark, reach.last_modification, reach.fk_dataowner, reach.fk_provider, reach.fk_wastewater_structure
        # reach --- reach.obj_id, reach.clear_height, reach.coefficient_of_friction, reach.elevation_determination, reach.horizontal_positioning, reach.inside_coating, reach.length_effective, reach.material, reach.progression_geometry, reach.reliner_material, reach.reliner_nominal_size, reach.relining_construction, reach.relining_kind, reach.ring_stiffness, reach.slope_building_plan, reach.wall_roughness, reach.fk_reach_point_from, reach.fk_reach_point_to, reach.fk_pipe_profile
        # _bwrel_ --- reach.reach_text__BWREL_fk_reach, reach.txt_text__BWREL_fk_reach, reach.reach_point__BWREL_fk_wastewater_networkelement, reach.connection_object__BWREL_fk_wastewater_networkelement, reach.catchment_area__BWREL_fk_wastewater_networkelement_rw_planned, reach.catchment_area__BWREL_fk_wastewater_networkelement_ww_planned, reach.catchment_area__BWREL_fk_wastewater_networkelement_rw_current, reach.catchment_area__BWREL_fk_wastewater_networkelement_ww_current
        # _rel_ --- reach.relining_construction__REL, reach.horizontal_positioning__REL, reach.inside_coating__REL, reach.reliner_material__REL, reach.fk_reach_point_to__REL, reach.fk_reach_point_from__REL, reach.fk_pipe_profile__REL, reach.relining_kind__REL, reach.elevation_determination__REL, reach.material__REL, reach.fk_wastewater_structure__REL, reach.fk_dataowner__REL, reach.fk_provider__REL

        haltung = ABWASSER.haltung(

            # --- baseclass ---
            # t_type=row.REPLACE_ME,  # VARCHAR(60)
            # t_ili_tid=row.REPLACE_ME,  # VARCHAR(200)

            # --- sia405_baseclass ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)

            # --- abwassernetzelement ---
            # bemerkung=row.REPLACE_ME,  # VARCHAR(80)
            # bezeichnung=row.REPLACE_ME,  # VARCHAR(20)
            # abwasserbauwerkref=row.REPLACE_ME,  # BIGINT

            # --- haltung ---
            # t_id=row.REPLACE_ME,  # BIGINT
            # innenschutz=row.REPLACE_ME,  # VARCHAR(255)
            # laengeeffektiv=row.REPLACE_ME,  # NUMERIC(7, 2)
            # lagebestimmung=row.REPLACE_ME,  # VARCHAR(255)
            # lichte_hoehe=row.REPLACE_ME,  # INTEGER
            # material=row.REPLACE_ME,  # VARCHAR(255)
            # plangefaelle=row.REPLACE_ME,  # INTEGER
            # reibungsbeiwert=row.REPLACE_ME,  # INTEGER
            # reliner_art=row.REPLACE_ME,  # VARCHAR(255)
            # reliner_bautechnik=row.REPLACE_ME,  # VARCHAR(255)
            # reliner_material=row.REPLACE_ME,  # VARCHAR(255)
            # reliner_nennweite=row.REPLACE_ME,  # INTEGER
            # ringsteifigkeit=row.REPLACE_ME,  # INTEGER
            # verlauf=row.REPLACE_ME,  # geometry(COMPOUNDCURVE,2056)
            # wandrauhigkeit=row.REPLACE_ME,  # NUMERIC(5, 2)
            # rohrprofilref=row.REPLACE_ME,  # BIGINT
            # nachhaltungspunktref=row.REPLACE_ME,  # BIGINT
            # vonhaltungspunktref=row.REPLACE_ME,  # BIGINT
        )
        abwasser_session.add(haltung)
        create_metaattributes(row, session)
        print(".", end="")
    print("done")

    print("Exporting QGEP.dryweather_downspout -> ABWASSER.trockenwetterfallrohr, ABWASSER.metaattribute")
    for row in qgep_session.query(QGEP.dryweather_downspout):

        # structure_part --- dryweather_downspout.identifier, dryweather_downspout.remark, dryweather_downspout.renovation_demand, dryweather_downspout.last_modification, dryweather_downspout.fk_dataowner, dryweather_downspout.fk_provider, dryweather_downspout.fk_wastewater_structure
        # dryweather_downspout --- dryweather_downspout.obj_id, dryweather_downspout.diameter
        # _bwrel_ --- dryweather_downspout.access_aid_kind__BWREL_obj_id, dryweather_downspout.dryweather_flume_material__BWREL_obj_id, dryweather_downspout.electric_equipment__BWREL_obj_id, dryweather_downspout.electromechanical_equipment__BWREL_obj_id, dryweather_downspout.benching_kind__BWREL_obj_id, dryweather_downspout.tank_emptying__BWREL_obj_id, dryweather_downspout.backflow_prevention__BWREL_obj_id, dryweather_downspout.tank_cleaning__BWREL_obj_id, dryweather_downspout.solids_retention__BWREL_obj_id
        # _rel_ --- dryweather_downspout.fk_wastewater_structure__REL, dryweather_downspout.fk_provider__REL, dryweather_downspout.fk_dataowner__REL, dryweather_downspout.renovation_demand__REL

        trockenwetterfallrohr = ABWASSER.trockenwetterfallrohr(

            # --- baseclass ---
            # t_type=row.REPLACE_ME,  # VARCHAR(60)
            # t_ili_tid=row.REPLACE_ME,  # VARCHAR(200)

            # --- sia405_baseclass ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)

            # --- bauwerksteil ---
            # bemerkung=row.REPLACE_ME,  # VARCHAR(80)
            # bezeichnung=row.REPLACE_ME,  # VARCHAR(20)
            # instandstellung=row.REPLACE_ME,  # VARCHAR(255)
            # abwasserbauwerkref=row.REPLACE_ME,  # BIGINT

            # --- trockenwetterfallrohr ---
            # t_id=row.REPLACE_ME,  # BIGINT
            # durchmesser=row.REPLACE_ME,  # INTEGER
        )
        abwasser_session.add(trockenwetterfallrohr)
        create_metaattributes(row, session)
        print(".", end="")
    print("done")

    print("Exporting QGEP.access_aid -> ABWASSER.einstiegshilfe, ABWASSER.metaattribute")
    for row in qgep_session.query(QGEP.access_aid):

        # structure_part --- access_aid.identifier, access_aid.remark, access_aid.renovation_demand, access_aid.last_modification, access_aid.fk_dataowner, access_aid.fk_provider, access_aid.fk_wastewater_structure
        # access_aid --- access_aid.obj_id, access_aid.kind
        # _bwrel_ --- access_aid.access_aid_kind__BWREL_obj_id, access_aid.dryweather_flume_material__BWREL_obj_id, access_aid.electric_equipment__BWREL_obj_id, access_aid.electromechanical_equipment__BWREL_obj_id, access_aid.benching_kind__BWREL_obj_id, access_aid.tank_emptying__BWREL_obj_id, access_aid.backflow_prevention__BWREL_obj_id, access_aid.tank_cleaning__BWREL_obj_id, access_aid.solids_retention__BWREL_obj_id
        # _rel_ --- access_aid.kind__REL, access_aid.fk_wastewater_structure__REL, access_aid.fk_provider__REL, access_aid.fk_dataowner__REL, access_aid.renovation_demand__REL

        einstiegshilfe = ABWASSER.einstiegshilfe(

            # --- baseclass ---
            # t_type=row.REPLACE_ME,  # VARCHAR(60)
            # t_ili_tid=row.REPLACE_ME,  # VARCHAR(200)

            # --- sia405_baseclass ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)

            # --- bauwerksteil ---
            # bemerkung=row.REPLACE_ME,  # VARCHAR(80)
            # bezeichnung=row.REPLACE_ME,  # VARCHAR(20)
            # instandstellung=row.REPLACE_ME,  # VARCHAR(255)
            # abwasserbauwerkref=row.REPLACE_ME,  # BIGINT

            # --- einstiegshilfe ---
            # t_id=row.REPLACE_ME,  # BIGINT
            # art=row.REPLACE_ME,  # VARCHAR(255)
        )
        abwasser_session.add(einstiegshilfe)
        create_metaattributes(row, session)
        print(".", end="")
    print("done")

    print("Exporting QGEP.dryweather_flume -> ABWASSER.trockenwetterrinne, ABWASSER.metaattribute")
    for row in qgep_session.query(QGEP.dryweather_flume):

        # structure_part --- dryweather_flume.identifier, dryweather_flume.remark, dryweather_flume.renovation_demand, dryweather_flume.last_modification, dryweather_flume.fk_dataowner, dryweather_flume.fk_provider, dryweather_flume.fk_wastewater_structure
        # dryweather_flume --- dryweather_flume.obj_id, dryweather_flume.material
        # _bwrel_ --- dryweather_flume.access_aid_kind__BWREL_obj_id, dryweather_flume.dryweather_flume_material__BWREL_obj_id, dryweather_flume.electric_equipment__BWREL_obj_id, dryweather_flume.electromechanical_equipment__BWREL_obj_id, dryweather_flume.benching_kind__BWREL_obj_id, dryweather_flume.tank_emptying__BWREL_obj_id, dryweather_flume.backflow_prevention__BWREL_obj_id, dryweather_flume.tank_cleaning__BWREL_obj_id, dryweather_flume.solids_retention__BWREL_obj_id
        # _rel_ --- dryweather_flume.material__REL, dryweather_flume.fk_wastewater_structure__REL, dryweather_flume.fk_provider__REL, dryweather_flume.fk_dataowner__REL, dryweather_flume.renovation_demand__REL

        trockenwetterrinne = ABWASSER.trockenwetterrinne(

            # --- baseclass ---
            # t_type=row.REPLACE_ME,  # VARCHAR(60)
            # t_ili_tid=row.REPLACE_ME,  # VARCHAR(200)

            # --- sia405_baseclass ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)

            # --- bauwerksteil ---
            # bemerkung=row.REPLACE_ME,  # VARCHAR(80)
            # bezeichnung=row.REPLACE_ME,  # VARCHAR(20)
            # instandstellung=row.REPLACE_ME,  # VARCHAR(255)
            # abwasserbauwerkref=row.REPLACE_ME,  # BIGINT

            # --- trockenwetterrinne ---
            # t_id=row.REPLACE_ME,  # BIGINT
            # material=row.REPLACE_ME,  # VARCHAR(255)
        )
        abwasser_session.add(trockenwetterrinne)
        create_metaattributes(row, session)
        print(".", end="")
    print("done")

    print("Exporting QGEP.cover -> ABWASSER.deckel, ABWASSER.metaattribute")
    for row in qgep_session.query(QGEP.cover):

        # structure_part --- cover.identifier, cover.remark, cover.renovation_demand, cover.last_modification, cover.fk_dataowner, cover.fk_provider, cover.fk_wastewater_structure
        # cover --- cover.obj_id, cover.brand, cover.cover_shape, cover.diameter, cover.fastening, cover.level, cover.material, cover.positional_accuracy, cover.situation_geometry, cover.sludge_bucket, cover.venting
        # _bwrel_ --- cover.access_aid_kind__BWREL_obj_id, cover.dryweather_flume_material__BWREL_obj_id, cover.electric_equipment__BWREL_obj_id, cover.electromechanical_equipment__BWREL_obj_id, cover.benching_kind__BWREL_obj_id, cover.tank_emptying__BWREL_obj_id, cover.wastewater_structure__BWREL_fk_main_cover, cover.backflow_prevention__BWREL_obj_id, cover.tank_cleaning__BWREL_obj_id, cover.solids_retention__BWREL_obj_id
        # _rel_ --- cover.positional_accuracy__REL, cover.sludge_bucket__REL, cover.cover_shape__REL, cover.fastening__REL, cover.venting__REL, cover.material__REL, cover.fk_wastewater_structure__REL, cover.fk_provider__REL, cover.fk_dataowner__REL, cover.renovation_demand__REL

        deckel = ABWASSER.deckel(

            # --- baseclass ---
            # t_type=row.REPLACE_ME,  # VARCHAR(60)
            # t_ili_tid=row.REPLACE_ME,  # VARCHAR(200)

            # --- sia405_baseclass ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)

            # --- bauwerksteil ---
            # bemerkung=row.REPLACE_ME,  # VARCHAR(80)
            # bezeichnung=row.REPLACE_ME,  # VARCHAR(20)
            # instandstellung=row.REPLACE_ME,  # VARCHAR(255)
            # abwasserbauwerkref=row.REPLACE_ME,  # BIGINT

            # --- deckel ---
            # t_id=row.REPLACE_ME,  # BIGINT
            # deckelform=row.REPLACE_ME,  # VARCHAR(255)
            # durchmesser=row.REPLACE_ME,  # INTEGER
            # entlueftung=row.REPLACE_ME,  # VARCHAR(255)
            # fabrikat=row.REPLACE_ME,  # VARCHAR(50)
            # kote=row.REPLACE_ME,  # NUMERIC(7, 3)
            # lage=row.REPLACE_ME,  # geometry(POINT,2056)
            # lagegenauigkeit=row.REPLACE_ME,  # VARCHAR(255)
            # material=row.REPLACE_ME,  # VARCHAR(255)
            # schlammeimer=row.REPLACE_ME,  # VARCHAR(255)
            # verschluss=row.REPLACE_ME,  # VARCHAR(255)
        )
        abwasser_session.add(deckel)
        create_metaattributes(row, session)
        print(".", end="")
    print("done")

    print("Exporting QGEP.benching -> ABWASSER.bankett, ABWASSER.metaattribute")
    for row in qgep_session.query(QGEP.benching):

        # structure_part --- benching.identifier, benching.remark, benching.renovation_demand, benching.last_modification, benching.fk_dataowner, benching.fk_provider, benching.fk_wastewater_structure
        # benching --- benching.obj_id, benching.kind
        # _bwrel_ --- benching.access_aid_kind__BWREL_obj_id, benching.dryweather_flume_material__BWREL_obj_id, benching.electric_equipment__BWREL_obj_id, benching.electromechanical_equipment__BWREL_obj_id, benching.benching_kind__BWREL_obj_id, benching.tank_emptying__BWREL_obj_id, benching.backflow_prevention__BWREL_obj_id, benching.tank_cleaning__BWREL_obj_id, benching.solids_retention__BWREL_obj_id
        # _rel_ --- benching.kind__REL, benching.fk_wastewater_structure__REL, benching.fk_provider__REL, benching.fk_dataowner__REL, benching.renovation_demand__REL

        bankett = ABWASSER.bankett(

            # --- baseclass ---
            # t_type=row.REPLACE_ME,  # VARCHAR(60)
            # t_ili_tid=row.REPLACE_ME,  # VARCHAR(200)

            # --- sia405_baseclass ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)

            # --- bauwerksteil ---
            # bemerkung=row.REPLACE_ME,  # VARCHAR(80)
            # bezeichnung=row.REPLACE_ME,  # VARCHAR(20)
            # instandstellung=row.REPLACE_ME,  # VARCHAR(255)
            # abwasserbauwerkref=row.REPLACE_ME,  # BIGINT

            # --- bankett ---
            # t_id=row.REPLACE_ME,  # BIGINT
            # art=row.REPLACE_ME,  # VARCHAR(255)
        )
        abwasser_session.add(bankett)
        create_metaattributes(row, session)
        print(".", end="")
    print("done")

    print("Exporting QGEP.examination -> ABWASSER.untersuchung, ABWASSER.metaattribute")
    for row in qgep_session.query(QGEP.examination):

        # maintenance_event --- examination.base_data, examination.cost, examination.data_details, examination.duration, examination.identifier, examination.kind, examination.operator, examination.reason, examination.remark, examination.result, examination.status, examination.time_point, examination.last_modification, examination.fk_dataowner, examination.fk_provider, examination.fk_operating_company, examination.active_zone
        # examination --- examination.obj_id, examination.equipment, examination.from_point_identifier, examination.inspected_length, examination.recording_type, examination.to_point_identifier, examination.vehicle, examination.videonumber, examination.weather, examination.fk_reach_point
        # _bwrel_ --- examination.re_maintenance_event_wastewater_structure__BWREL_fk_maintenance_event, examination.damage__BWREL_fk_examination
        # _rel_ --- examination.fk_reach_point__REL, examination.recording_type__REL, examination.weather__REL, examination.kind__REL, examination.fk_operating_company__REL, examination.fk_provider__REL, examination.status__REL, examination.fk_dataowner__REL

        untersuchung = ABWASSER.untersuchung(

            # --- baseclass ---
            # t_type=row.REPLACE_ME,  # VARCHAR(60)
            # t_ili_tid=row.REPLACE_ME,  # VARCHAR(200)

            # --- sia405_baseclass ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)

            # --- erhaltungsereignis ---
            # art=row.REPLACE_ME,  # VARCHAR(255)
            # ausfuehrender=row.REPLACE_ME,  # VARCHAR(50)
            # bemerkung=row.REPLACE_ME,  # VARCHAR(80)
            # bezeichnung=row.REPLACE_ME,  # VARCHAR(20)
            # datengrundlage=row.REPLACE_ME,  # VARCHAR(50)
            # dauer=row.REPLACE_ME,  # INTEGER
            # detaildaten=row.REPLACE_ME,  # VARCHAR(50)
            # ergebnis=row.REPLACE_ME,  # VARCHAR(50)
            # grund=row.REPLACE_ME,  # VARCHAR(50)
            # kosten=row.REPLACE_ME,  # NUMERIC(10, 2)
            # astatus=row.REPLACE_ME,  # VARCHAR(255)
            # zeitpunkt=row.REPLACE_ME,  # DATE
            # abwasserbauwerkref=row.REPLACE_ME,  # BIGINT
            # ausfuehrende_firmaref=row.REPLACE_ME,  # BIGINT

            # --- untersuchung ---
            # t_id=row.REPLACE_ME,  # BIGINT
            # bispunktbezeichnung=row.REPLACE_ME,  # VARCHAR(20)
            # erfassungsart=row.REPLACE_ME,  # VARCHAR(255)
            # fahrzeug=row.REPLACE_ME,  # VARCHAR(50)
            # geraet=row.REPLACE_ME,  # VARCHAR(50)
            # inspizierte_laenge=row.REPLACE_ME,  # NUMERIC(7, 2)
            # videonummer=row.REPLACE_ME,  # VARCHAR(20)
            # vonpunktbezeichnung=row.REPLACE_ME,  # VARCHAR(20)
            # witterung=row.REPLACE_ME,  # VARCHAR(255)
            # haltungspunktref=row.REPLACE_ME,  # BIGINT
        )
        abwasser_session.add(untersuchung)
        create_metaattributes(row, session)
        print(".", end="")
    print("done")

    print("Exporting QGEP.damage_manhole -> ABWASSER.normschachtschaden, ABWASSER.metaattribute")
    for row in qgep_session.query(QGEP.damage_manhole):

        # damage --- damage_manhole.comments, damage_manhole.connection, damage_manhole.damage_begin, damage_manhole.damage_end, damage_manhole.damage_reach, damage_manhole.distance, damage_manhole.quantification1, damage_manhole.quantification2, damage_manhole.single_damage_class, damage_manhole.video_counter, damage_manhole.view_parameters, damage_manhole.last_modification, damage_manhole.fk_dataowner, damage_manhole.fk_provider, damage_manhole.fk_examination
        # damage_manhole --- damage_manhole.obj_id, damage_manhole.manhole_damage_code, damage_manhole.manhole_shaft_area
        # _bwrel_ --- damage_manhole.damage_channel_channel_damage_code__BWREL_obj_id
        # _rel_ --- damage_manhole.manhole_shaft_area__REL, damage_manhole.manhole_damage_code__REL, damage_manhole.single_damage_class__REL, damage_manhole.fk_provider__REL, damage_manhole.fk_examination__REL, damage_manhole.fk_dataowner__REL, damage_manhole.connection__REL

        normschachtschaden = ABWASSER.normschachtschaden(

            # --- baseclass ---
            # t_type=row.REPLACE_ME,  # VARCHAR(60)
            # t_ili_tid=row.REPLACE_ME,  # VARCHAR(200)

            # --- sia405_baseclass ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)

            # --- schaden ---
            # anmerkung=row.REPLACE_ME,  # VARCHAR(100)
            # ansichtsparameter=row.REPLACE_ME,  # VARCHAR(200)
            # einzelschadenklasse=row.REPLACE_ME,  # VARCHAR(255)
            # streckenschaden=row.REPLACE_ME,  # VARCHAR(3)
            # verbindung=row.REPLACE_ME,  # VARCHAR(255)
            # videozaehlerstand=row.REPLACE_ME,  # VARCHAR(255)
            # untersuchungref=row.REPLACE_ME,  # BIGINT

            # --- normschachtschaden ---
            # t_id=row.REPLACE_ME,  # BIGINT
            # distanz=row.REPLACE_ME,  # NUMERIC(7, 2)
            # quantifizierung1=row.REPLACE_ME,  # VARCHAR(20)
            # quantifizierung2=row.REPLACE_ME,  # VARCHAR(20)
            # schachtbereich=row.REPLACE_ME,  # VARCHAR(255)
            # schachtschadencode=row.REPLACE_ME,  # VARCHAR(255)
            # schadenlageanfang=row.REPLACE_ME,  # INTEGER
            # schadenlageende=row.REPLACE_ME,  # INTEGER
        )
        abwasser_session.add(normschachtschaden)
        create_metaattributes(row, session)
        print(".", end="")
    print("done")

    print("Exporting QGEP.damage_channel -> ABWASSER.kanalschaden, ABWASSER.metaattribute")
    for row in qgep_session.query(QGEP.damage_channel):

        # damage --- damage_channel.comments, damage_channel.connection, damage_channel.damage_begin, damage_channel.damage_end, damage_channel.damage_reach, damage_channel.distance, damage_channel.quantification1, damage_channel.quantification2, damage_channel.single_damage_class, damage_channel.video_counter, damage_channel.view_parameters, damage_channel.last_modification, damage_channel.fk_dataowner, damage_channel.fk_provider, damage_channel.fk_examination
        # damage_channel --- damage_channel.obj_id, damage_channel.channel_damage_code
        # _bwrel_ --- damage_channel.damage_channel_channel_damage_code__BWREL_obj_id
        # _rel_ --- damage_channel.channel_damage_code__REL, damage_channel.single_damage_class__REL, damage_channel.fk_provider__REL, damage_channel.fk_examination__REL, damage_channel.fk_dataowner__REL, damage_channel.connection__REL

        kanalschaden = ABWASSER.kanalschaden(

            # --- baseclass ---
            # t_type=row.REPLACE_ME,  # VARCHAR(60)
            # t_ili_tid=row.REPLACE_ME,  # VARCHAR(200)

            # --- sia405_baseclass ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)

            # --- schaden ---
            # anmerkung=row.REPLACE_ME,  # VARCHAR(100)
            # ansichtsparameter=row.REPLACE_ME,  # VARCHAR(200)
            # einzelschadenklasse=row.REPLACE_ME,  # VARCHAR(255)
            # streckenschaden=row.REPLACE_ME,  # VARCHAR(3)
            # verbindung=row.REPLACE_ME,  # VARCHAR(255)
            # videozaehlerstand=row.REPLACE_ME,  # VARCHAR(255)
            # untersuchungref=row.REPLACE_ME,  # BIGINT

            # --- kanalschaden ---
            # t_id=row.REPLACE_ME,  # BIGINT
            # distanz=row.REPLACE_ME,  # NUMERIC(7, 2)
            # kanalschadencode=row.REPLACE_ME,  # VARCHAR(255)
            # quantifizierung1=row.REPLACE_ME,  # INTEGER
            # quantifizierung2=row.REPLACE_ME,  # INTEGER
            # schadenlageanfang=row.REPLACE_ME,  # INTEGER
            # schadenlageende=row.REPLACE_ME,  # INTEGER
        )
        abwasser_session.add(kanalschaden)
        create_metaattributes(row, session)
        print(".", end="")
    print("done")

    print("Exporting QGEP.data_media -> ABWASSER.datentraeger, ABWASSER.metaattribute")
    for row in qgep_session.query(QGEP.data_media):

        # data_media --- data_media.obj_id, data_media.identifier, data_media.kind, data_media.location, data_media.path, data_media.remark, data_media.last_modification, data_media.fk_dataowner, data_media.fk_provider
        # _rel_ --- data_media.kind__REL, data_media.fk_dataowner__REL, data_media.fk_provider__REL

        datentraeger = ABWASSER.datentraeger(

            # --- baseclass ---
            # t_type=row.REPLACE_ME,  # VARCHAR(60)
            # t_ili_tid=row.REPLACE_ME,  # VARCHAR(200)

            # --- sia405_baseclass ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)

            # --- datentraeger ---
            # t_id=row.REPLACE_ME,  # BIGINT
            # art=row.REPLACE_ME,  # VARCHAR(255)
            # bemerkung=row.REPLACE_ME,  # VARCHAR(80)
            # bezeichnung=row.REPLACE_ME,  # VARCHAR(60)
            # pfad=row.REPLACE_ME,  # VARCHAR(100)
            # standort=row.REPLACE_ME,  # VARCHAR(50)
        )
        abwasser_session.add(datentraeger)
        create_metaattributes(row, session)
        print(".", end="")
    print("done")

    print("Exporting QGEP.file -> ABWASSER.datei, ABWASSER.metaattribute")
    for row in qgep_session.query(QGEP.file):

        # file --- file.obj_id, file.class, file.identifier, file.kind, file.object, file.path_relative, file.remark, file.last_modification, file.fk_dataowner, file.fk_provider, file.fk_data_media
        # _rel_ --- file.kind__REL, file.fk_provider__REL, file.fk_dataowner__REL, file.class__REL

        datei = ABWASSER.datei(

            # --- baseclass ---
            # t_type=row.REPLACE_ME,  # VARCHAR(60)
            # t_ili_tid=row.REPLACE_ME,  # VARCHAR(200)

            # --- sia405_baseclass ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)

            # --- datei ---
            # t_id=row.REPLACE_ME,  # BIGINT
            # art=row.REPLACE_ME,  # VARCHAR(255)
            # bemerkung=row.REPLACE_ME,  # VARCHAR(80)
            # bezeichnung=row.REPLACE_ME,  # VARCHAR(60)
            # klasse=row.REPLACE_ME,  # VARCHAR(255)
            # objekt=row.REPLACE_ME,  # VARCHAR(16)
            # relativpfad=row.REPLACE_ME,  # VARCHAR(200)
            # datentraegerref=row.REPLACE_ME,  # BIGINT
        )
        abwasser_session.add(datei)
        create_metaattributes(row, session)
        print(".", end="")
    print("done")

    abwasser_session.commit()

    qgep_session.close()
    abwasser_session.close()
