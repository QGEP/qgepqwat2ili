from .qgep import Classes as QGEP
from .abwasser import Classes as ABWASSER

QGEP_TO_ABWASSER = {
    # ALREADY MAPPED
    QGEP.organisation: [ABWASSER.organisation, ABWASSER.metaattribute],
    QGEP.channel: [ABWASSER.kanal, ABWASSER.metaattribute],
    QGEP.manhole: [ABWASSER.normschacht, ABWASSER.metaattribute],
    QGEP.discharge_point: [ABWASSER.einleitstelle, ABWASSER.metaattribute],
    QGEP.special_structure: [ABWASSER.spezialbauwerk, ABWASSER.metaattribute],
    QGEP.infiltration_installation: [ABWASSER.versickerungsanlage, ABWASSER.metaattribute],
    QGEP.pipe_profile: [ABWASSER.rohrprofil, ABWASSER.metaattribute],
    QGEP.reach_point: [ABWASSER.haltungspunkt, ABWASSER.metaattribute],
    QGEP.wastewater_node: [ABWASSER.abwasserknoten, ABWASSER.metaattribute],
    QGEP.reach: [ABWASSER.haltung, ABWASSER.metaattribute],
    QGEP.dryweather_downspout: [ABWASSER.trockenwetterfallrohr, ABWASSER.metaattribute],
    QGEP.access_aid: [ABWASSER.einstiegshilfe, ABWASSER.metaattribute],
    QGEP.dryweather_flume: [ABWASSER.trockenwetterrinne, ABWASSER.metaattribute],
    QGEP.cover: [ABWASSER.deckel, ABWASSER.metaattribute],
    QGEP.benching: [ABWASSER.bankett, ABWASSER.metaattribute],
    QGEP.examination: [ABWASSER.untersuchung, ABWASSER.metaattribute],
    QGEP.damage_manhole: [ABWASSER.normschachtschaden, ABWASSER.metaattribute],
    QGEP.damage_channel: [ABWASSER.kanalschaden, ABWASSER.metaattribute],
    QGEP.data_media: [ABWASSER.datentraeger, ABWASSER.metaattribute],
    QGEP.file: [ABWASSER.datei, ABWASSER.metaattribute],

    # AVAILABLE TABLES
    # ABWASSER.abwasserbauwerk, ABWASSER.abwasserknoten, ABWASSER.abwassernetzelement, ABWASSER.bankett, ABWASSER.baseclass, ABWASSER.bauwerksteil, ABWASSER.datei, ABWASSER.datentraeger, ABWASSER.deckel, ABWASSER.einleitstelle, ABWASSER.einstiegshilfe, ABWASSER.erhaltungsereignis, ABWASSER.haltung, ABWASSER.haltung_alternativverlauf, ABWASSER.haltungspunkt, ABWASSER.kanal, ABWASSER.kanalschaden, ABWASSER.metaattribute, ABWASSER.normschacht, ABWASSER.normschachtschaden, ABWASSER.organisation, ABWASSER.organisation_teil_vonassoc, ABWASSER.rohrprofil, ABWASSER.schaden, ABWASSER.sia405_baseclass, ABWASSER.sia405_symbolpos, ABWASSER.sia405_textpos, ABWASSER.spezialbauwerk, ABWASSER.symbolpos, ABWASSER.t_ili2db_attrname, ABWASSER.t_ili2db_basket, ABWASSER.t_ili2db_classname, ABWASSER.t_ili2db_dataset, ABWASSER.t_ili2db_inheritance, ABWASSER.t_ili2db_model, ABWASSER.t_ili2db_settings, ABWASSER.textpos, ABWASSER.trockenwetterfallrohr, ABWASSER.trockenwetterrinne, ABWASSER.untersuchung, ABWASSER.versickerungsanlage, ABWASSER.videozaehlerstand

    # NOT YET MAPPED
    # QGEP.access_aid_kind: [ABWASSER.REPLACE_ME],
    # QGEP.accident: [ABWASSER.REPLACE_ME],
    # QGEP.administrative_office: [ABWASSER.REPLACE_ME],
    # QGEP.aquifier: [ABWASSER.REPLACE_ME],
    # QGEP.backflow_prevention: [ABWASSER.REPLACE_ME],
    # QGEP.backflow_prevention_kind: [ABWASSER.REPLACE_ME],
    # QGEP.bathing_area: [ABWASSER.REPLACE_ME],
    # QGEP.benching_kind: [ABWASSER.REPLACE_ME],
    # QGEP.blocking_debris: [ABWASSER.REPLACE_ME],
    # QGEP.building: [ABWASSER.REPLACE_ME],
    # QGEP.canton: [ABWASSER.REPLACE_ME],
    # QGEP.catchment_area: [ABWASSER.REPLACE_ME],
    # QGEP.catchment_area_direct_discharge_current: [ABWASSER.REPLACE_ME],
    # QGEP.catchment_area_direct_discharge_planned: [ABWASSER.REPLACE_ME],
    # QGEP.catchment_area_drainage_system_current: [ABWASSER.REPLACE_ME],
    # QGEP.catchment_area_drainage_system_planned: [ABWASSER.REPLACE_ME],
    # QGEP.catchment_area_infiltration_current: [ABWASSER.REPLACE_ME],
    # QGEP.catchment_area_infiltration_planned: [ABWASSER.REPLACE_ME],
    # QGEP.catchment_area_retention_current: [ABWASSER.REPLACE_ME],
    # QGEP.catchment_area_retention_planned: [ABWASSER.REPLACE_ME],
    # QGEP.catchment_area_text: [ABWASSER.REPLACE_ME],
    # QGEP.catchment_area_text_plantype: [ABWASSER.REPLACE_ME],
    # QGEP.catchment_area_text_texthali: [ABWASSER.REPLACE_ME],
    # QGEP.catchment_area_text_textvali: [ABWASSER.REPLACE_ME],
    # QGEP.channel_bedding_encasement: [ABWASSER.REPLACE_ME],
    # QGEP.channel_connection_type: [ABWASSER.REPLACE_ME],
    # QGEP.channel_function_hierarchic: [ABWASSER.REPLACE_ME],
    # QGEP.channel_function_hydraulic: [ABWASSER.REPLACE_ME],
    # QGEP.channel_usage_current: [ABWASSER.REPLACE_ME],
    # QGEP.channel_usage_planned: [ABWASSER.REPLACE_ME],
    # QGEP.chute: [ABWASSER.REPLACE_ME],
    # QGEP.chute_kind: [ABWASSER.REPLACE_ME],
    # QGEP.chute_material: [ABWASSER.REPLACE_ME],
    # QGEP.connection_object: [ABWASSER.REPLACE_ME],
    # QGEP.control_center: [ABWASSER.REPLACE_ME],
    # QGEP.cooperative: [ABWASSER.REPLACE_ME],
    # QGEP.cover_cover_shape: [ABWASSER.REPLACE_ME],
    # QGEP.cover_fastening: [ABWASSER.REPLACE_ME],
    # QGEP.cover_material: [ABWASSER.REPLACE_ME],
    # QGEP.cover_positional_accuracy: [ABWASSER.REPLACE_ME],
    # QGEP.cover_sludge_bucket: [ABWASSER.REPLACE_ME],
    # QGEP.cover_venting: [ABWASSER.REPLACE_ME],
    # QGEP.dam: [ABWASSER.REPLACE_ME],
    # QGEP.dam_kind: [ABWASSER.REPLACE_ME],
    # QGEP.damage: [ABWASSER.REPLACE_ME],
    # QGEP.damage_channel_channel_damage_code: [ABWASSER.REPLACE_ME],
    # QGEP.damage_connection: [ABWASSER.REPLACE_ME],
    # QGEP.damage_manhole_manhole_damage_code: [ABWASSER.REPLACE_ME],
    # QGEP.damage_manhole_manhole_shaft_area: [ABWASSER.REPLACE_ME],
    # QGEP.damage_single_damage_class: [ABWASSER.REPLACE_ME],
    # QGEP.data_media_kind: [ABWASSER.REPLACE_ME],
    # QGEP.discharge_point_relevance: [ABWASSER.REPLACE_ME],
    # QGEP.drainage_system: [ABWASSER.REPLACE_ME],
    # QGEP.drainage_system_kind: [ABWASSER.REPLACE_ME],
    # QGEP.dryweather_flume_material: [ABWASSER.REPLACE_ME],
    # QGEP.electric_equipment: [ABWASSER.REPLACE_ME],
    # QGEP.electric_equipment_kind: [ABWASSER.REPLACE_ME],
    # QGEP.electromechanical_equipment: [ABWASSER.REPLACE_ME],
    # QGEP.electromechanical_equipment_kind: [ABWASSER.REPLACE_ME],
    # QGEP.examination_recording_type: [ABWASSER.REPLACE_ME],
    # QGEP.examination_weather: [ABWASSER.REPLACE_ME],
    # QGEP.file_class: [ABWASSER.REPLACE_ME],
    # QGEP.file_kind: [ABWASSER.REPLACE_ME],
    # QGEP.fish_pass: [ABWASSER.REPLACE_ME],
    # QGEP.ford: [ABWASSER.REPLACE_ME],
    # QGEP.fountain: [ABWASSER.REPLACE_ME],
    # QGEP.ground_water_protection_perimeter: [ABWASSER.REPLACE_ME],
    # QGEP.groundwater_protection_zone: [ABWASSER.REPLACE_ME],
    # QGEP.groundwater_protection_zone_kind: [ABWASSER.REPLACE_ME],
    # QGEP.hazard_source: [ABWASSER.REPLACE_ME],
    # QGEP.hq_relation: [ABWASSER.REPLACE_ME],
    # QGEP.hydr_geom_relation: [ABWASSER.REPLACE_ME],
    # QGEP.hydr_geometry: [ABWASSER.REPLACE_ME],
    # QGEP.hydraulic_char_data: [ABWASSER.REPLACE_ME],
    # QGEP.hydraulic_char_data_is_overflowing: [ABWASSER.REPLACE_ME],
    # QGEP.hydraulic_char_data_main_weir_kind: [ABWASSER.REPLACE_ME],
    # QGEP.hydraulic_char_data_pump_characteristics: [ABWASSER.REPLACE_ME],
    # QGEP.hydraulic_char_data_pump_usage_current: [ABWASSER.REPLACE_ME],
    # QGEP.hydraulic_char_data_status: [ABWASSER.REPLACE_ME],
    # QGEP.individual_surface: [ABWASSER.REPLACE_ME],
    # QGEP.individual_surface_function: [ABWASSER.REPLACE_ME],
    # QGEP.individual_surface_pavement: [ABWASSER.REPLACE_ME],
    # QGEP.infiltration_installation_defects: [ABWASSER.REPLACE_ME],
    # QGEP.infiltration_installation_emergency_spillway: [ABWASSER.REPLACE_ME],
    # QGEP.infiltration_installation_kind: [ABWASSER.REPLACE_ME],
    # QGEP.infiltration_installation_labeling: [ABWASSER.REPLACE_ME],
    # QGEP.infiltration_installation_seepage_utilization: [ABWASSER.REPLACE_ME],
    # QGEP.infiltration_installation_vehicle_access: [ABWASSER.REPLACE_ME],
    # QGEP.infiltration_installation_watertightness: [ABWASSER.REPLACE_ME],
    # QGEP.infiltration_zone: [ABWASSER.REPLACE_ME],
    # QGEP.infiltration_zone_infiltration_capacity: [ABWASSER.REPLACE_ME],
    # QGEP.lake: [ABWASSER.REPLACE_ME],
    # QGEP.leapingweir: [ABWASSER.REPLACE_ME],
    # QGEP.leapingweir_opening_shape: [ABWASSER.REPLACE_ME],
    # QGEP.lock: [ABWASSER.REPLACE_ME],
    # QGEP.maintenance_event: [ABWASSER.REPLACE_ME],
    # QGEP.maintenance_event_kind: [ABWASSER.REPLACE_ME],
    # QGEP.maintenance_event_status: [ABWASSER.REPLACE_ME],
    # QGEP.manhole_function: [ABWASSER.REPLACE_ME],
    # QGEP.manhole_material: [ABWASSER.REPLACE_ME],
    # QGEP.manhole_surface_inflow: [ABWASSER.REPLACE_ME],
    # QGEP.measurement_result: [ABWASSER.REPLACE_ME],
    # QGEP.measurement_result_measurement_type: [ABWASSER.REPLACE_ME],
    # QGEP.measurement_series: [ABWASSER.REPLACE_ME],
    # QGEP.measurement_series_kind: [ABWASSER.REPLACE_ME],
    # QGEP.measuring_device: [ABWASSER.REPLACE_ME],
    # QGEP.measuring_device_kind: [ABWASSER.REPLACE_ME],
    # QGEP.measuring_point: [ABWASSER.REPLACE_ME],
    # QGEP.measuring_point_damming_device: [ABWASSER.REPLACE_ME],
    # QGEP.measuring_point_purpose: [ABWASSER.REPLACE_ME],
    # QGEP.mechanical_pretreatment: [ABWASSER.REPLACE_ME],
    # QGEP.mechanical_pretreatment_kind: [ABWASSER.REPLACE_ME],
    # QGEP.municipality: [ABWASSER.REPLACE_ME],
    # QGEP.mutation: [ABWASSER.REPLACE_ME],
    # QGEP.mutation_kind: [ABWASSER.REPLACE_ME],
    # QGEP.overflow: [ABWASSER.REPLACE_ME],
    # QGEP.overflow_actuation: [ABWASSER.REPLACE_ME],
    # QGEP.overflow_adjustability: [ABWASSER.REPLACE_ME],
    # QGEP.overflow_char: [ABWASSER.REPLACE_ME],
    # QGEP.overflow_char_kind_overflow_characteristic: [ABWASSER.REPLACE_ME],
    # QGEP.overflow_char_overflow_characteristic_digital: [ABWASSER.REPLACE_ME],
    # QGEP.overflow_control: [ABWASSER.REPLACE_ME],
    # QGEP.overflow_function: [ABWASSER.REPLACE_ME],
    # QGEP.overflow_signal_transmission: [ABWASSER.REPLACE_ME],
    # QGEP.param_ca_general: [ABWASSER.REPLACE_ME],
    # QGEP.param_ca_mouse1: [ABWASSER.REPLACE_ME],
    # QGEP.passage: [ABWASSER.REPLACE_ME],
    # QGEP.pipe_profile_profile_type: [ABWASSER.REPLACE_ME],
    # QGEP.planning_zone: [ABWASSER.REPLACE_ME],
    # QGEP.planning_zone_kind: [ABWASSER.REPLACE_ME],
    # QGEP.prank_weir: [ABWASSER.REPLACE_ME],
    # QGEP.prank_weir_weir_edge: [ABWASSER.REPLACE_ME],
    # QGEP.prank_weir_weir_kind: [ABWASSER.REPLACE_ME],
    # QGEP.private: [ABWASSER.REPLACE_ME],
    # QGEP.profile_geometry: [ABWASSER.REPLACE_ME],
    # QGEP.pump: [ABWASSER.REPLACE_ME],
    # QGEP.pump_contruction_type: [ABWASSER.REPLACE_ME],
    # QGEP.pump_placement_of_actuation: [ABWASSER.REPLACE_ME],
    # QGEP.pump_placement_of_pump: [ABWASSER.REPLACE_ME],
    # QGEP.pump_usage_current: [ABWASSER.REPLACE_ME],
    # QGEP.re_maintenance_event_wastewater_structure: [ABWASSER.REPLACE_ME],
    # QGEP.reach_elevation_determination: [ABWASSER.REPLACE_ME],
    # QGEP.reach_horizontal_positioning: [ABWASSER.REPLACE_ME],
    # QGEP.reach_inside_coating: [ABWASSER.REPLACE_ME],
    # QGEP.reach_material: [ABWASSER.REPLACE_ME],
    # QGEP.reach_point_elevation_accuracy: [ABWASSER.REPLACE_ME],
    # QGEP.reach_point_outlet_shape: [ABWASSER.REPLACE_ME],
    # QGEP.reach_reliner_material: [ABWASSER.REPLACE_ME],
    # QGEP.reach_relining_construction: [ABWASSER.REPLACE_ME],
    # QGEP.reach_relining_kind: [ABWASSER.REPLACE_ME],
    # QGEP.reach_text: [ABWASSER.REPLACE_ME],
    # QGEP.reach_text_plantype: [ABWASSER.REPLACE_ME],
    # QGEP.reach_text_texthali: [ABWASSER.REPLACE_ME],
    # QGEP.reach_text_textvali: [ABWASSER.REPLACE_ME],
    # QGEP.reservoir: [ABWASSER.REPLACE_ME],
    # QGEP.retention_body: [ABWASSER.REPLACE_ME],
    # QGEP.retention_body_kind: [ABWASSER.REPLACE_ME],
    # QGEP.river_bank: [ABWASSER.REPLACE_ME],
    # QGEP.river_bank_control_grade_of_river: [ABWASSER.REPLACE_ME],
    # QGEP.river_bank_river_control_type: [ABWASSER.REPLACE_ME],
    # QGEP.river_bank_shores: [ABWASSER.REPLACE_ME],
    # QGEP.river_bank_side: [ABWASSER.REPLACE_ME],
    # QGEP.river_bank_utilisation_of_shore_surroundings: [ABWASSER.REPLACE_ME],
    # QGEP.river_bank_vegetation: [ABWASSER.REPLACE_ME],
    # QGEP.river_bed: [ABWASSER.REPLACE_ME],
    # QGEP.river_bed_control_grade_of_river: [ABWASSER.REPLACE_ME],
    # QGEP.river_bed_kind: [ABWASSER.REPLACE_ME],
    # QGEP.river_bed_river_control_type: [ABWASSER.REPLACE_ME],
    # QGEP.river_kind: [ABWASSER.REPLACE_ME],
    # QGEP.rock_ramp: [ABWASSER.REPLACE_ME],
    # QGEP.rock_ramp_stabilisation: [ABWASSER.REPLACE_ME],
    # QGEP.sector_water_body: [ABWASSER.REPLACE_ME],
    # QGEP.sector_water_body_kind: [ABWASSER.REPLACE_ME],
    # QGEP.sludge_treatment: [ABWASSER.REPLACE_ME],
    # QGEP.sludge_treatment_stabilisation: [ABWASSER.REPLACE_ME],
    # QGEP.solids_retention: [ABWASSER.REPLACE_ME],
    # QGEP.solids_retention_type: [ABWASSER.REPLACE_ME],
    # QGEP.special_structure_bypass: [ABWASSER.REPLACE_ME],
    # QGEP.special_structure_emergency_spillway: [ABWASSER.REPLACE_ME],
    # QGEP.special_structure_function: [ABWASSER.REPLACE_ME],
    # QGEP.special_structure_stormwater_tank_arrangement: [ABWASSER.REPLACE_ME],
    # QGEP.structure_part: [ABWASSER.REPLACE_ME],
    # QGEP.structure_part_renovation_demand: [ABWASSER.REPLACE_ME],
    # QGEP.substance: [ABWASSER.REPLACE_ME],
    # QGEP.surface_runoff_parameters: [ABWASSER.REPLACE_ME],
    # QGEP.surface_water_bodies: [ABWASSER.REPLACE_ME],
    # QGEP.symbol_plantype: [ABWASSER.REPLACE_ME],
    # QGEP.tank_cleaning: [ABWASSER.REPLACE_ME],
    # QGEP.tank_cleaning_type: [ABWASSER.REPLACE_ME],
    # QGEP.tank_emptying: [ABWASSER.REPLACE_ME],
    # QGEP.tank_emptying_type: [ABWASSER.REPLACE_ME],
    # QGEP.text_plantype: [ABWASSER.REPLACE_ME],
    # QGEP.text_texthali: [ABWASSER.REPLACE_ME],
    # QGEP.text_textvali: [ABWASSER.REPLACE_ME],
    # QGEP.throttle_shut_off_unit: [ABWASSER.REPLACE_ME],
    # QGEP.throttle_shut_off_unit_actuation: [ABWASSER.REPLACE_ME],
    # QGEP.throttle_shut_off_unit_adjustability: [ABWASSER.REPLACE_ME],
    # QGEP.throttle_shut_off_unit_control: [ABWASSER.REPLACE_ME],
    # QGEP.throttle_shut_off_unit_kind: [ABWASSER.REPLACE_ME],
    # QGEP.throttle_shut_off_unit_signal_transmission: [ABWASSER.REPLACE_ME],
    # QGEP.txt_symbol: [ABWASSER.REPLACE_ME],
    # QGEP.txt_text: [ABWASSER.REPLACE_ME],
    # QGEP.waste_water_association: [ABWASSER.REPLACE_ME],
    # QGEP.waste_water_treatment: [ABWASSER.REPLACE_ME],
    # QGEP.waste_water_treatment_kind: [ABWASSER.REPLACE_ME],
    # QGEP.waste_water_treatment_plant: [ABWASSER.REPLACE_ME],
    # QGEP.wastewater_networkelement: [ABWASSER.REPLACE_ME],
    # QGEP.wastewater_structure: [ABWASSER.REPLACE_ME],
    # QGEP.wastewater_structure_accessibility: [ABWASSER.REPLACE_ME],
    # QGEP.wastewater_structure_financing: [ABWASSER.REPLACE_ME],
    # QGEP.wastewater_structure_renovation_necessity: [ABWASSER.REPLACE_ME],
    # QGEP.wastewater_structure_rv_construction_type: [ABWASSER.REPLACE_ME],
    # QGEP.wastewater_structure_status: [ABWASSER.REPLACE_ME],
    # QGEP.wastewater_structure_structure_condition: [ABWASSER.REPLACE_ME],
    # QGEP.wastewater_structure_symbol: [ABWASSER.REPLACE_ME],
    # QGEP.wastewater_structure_symbol_plantype: [ABWASSER.REPLACE_ME],
    # QGEP.wastewater_structure_text: [ABWASSER.REPLACE_ME],
    # QGEP.wastewater_structure_text_plantype: [ABWASSER.REPLACE_ME],
    # QGEP.wastewater_structure_text_texthali: [ABWASSER.REPLACE_ME],
    # QGEP.wastewater_structure_text_textvali: [ABWASSER.REPLACE_ME],
    # QGEP.water_body_protection_sector: [ABWASSER.REPLACE_ME],
    # QGEP.water_body_protection_sector_kind: [ABWASSER.REPLACE_ME],
    # QGEP.water_catchment: [ABWASSER.REPLACE_ME],
    # QGEP.water_catchment_kind: [ABWASSER.REPLACE_ME],
    # QGEP.water_control_structure: [ABWASSER.REPLACE_ME],
    # QGEP.water_course_segment: [ABWASSER.REPLACE_ME],
    # QGEP.water_course_segment_algae_growth: [ABWASSER.REPLACE_ME],
    # QGEP.water_course_segment_altitudinal_zone: [ABWASSER.REPLACE_ME],
    # QGEP.water_course_segment_dead_wood: [ABWASSER.REPLACE_ME],
    # QGEP.water_course_segment_depth_variability: [ABWASSER.REPLACE_ME],
    # QGEP.water_course_segment_discharge_regime: [ABWASSER.REPLACE_ME],
    # QGEP.water_course_segment_ecom_classification: [ABWASSER.REPLACE_ME],
    # QGEP.water_course_segment_kind: [ABWASSER.REPLACE_ME],
    # QGEP.water_course_segment_length_profile: [ABWASSER.REPLACE_ME],
    # QGEP.water_course_segment_macrophyte_coverage: [ABWASSER.REPLACE_ME],
    # QGEP.water_course_segment_section_morphology: [ABWASSER.REPLACE_ME],
    # QGEP.water_course_segment_slope: [ABWASSER.REPLACE_ME],
    # QGEP.water_course_segment_utilisation: [ABWASSER.REPLACE_ME],
    # QGEP.water_course_segment_water_hardness: [ABWASSER.REPLACE_ME],
    # QGEP.water_course_segment_width_variability: [ABWASSER.REPLACE_ME],
    # QGEP.wwtp_energy_use: [ABWASSER.REPLACE_ME],
    # QGEP.wwtp_structure_kind: [ABWASSER.REPLACE_ME],
    # QGEP.zone: [ABWASSER.REPLACE_ME],
}
