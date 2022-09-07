from sqlalchemy.ext.automap import automap_base

from .. import config, utils

###############################################
# QGEP datamodel
# All tables will be loaded from the QGEP schema as a SqlAlchemy ORM class.
# Only table specific relationships (e.g. inheritance) need to be manually
# defined here. Other attributes will be loaded automatically.
###############################################

Base = automap_base()

SCHEMA = config.QGEP_SCHEMA


# class maintenance_event_wastewater_structure(Base):
    # __tablename__ = "maintenance_event_wastewater_structure"
    # __table_args__ = {"schema": SCHEMA}


class organisation(Base):
    __tablename__ = "organisation"
    __table_args__ = {"schema": SCHEMA}


class mutation(Base):
    __tablename__ = "mutation"
    __table_args__ = {"schema": SCHEMA}


class waste_water_treatment_plant(Base):
    __tablename__ = "waste_water_treatment_plant"
    __table_args__ = {"schema": SCHEMA}


class wastewater_structure(Base):
    __tablename__ = "wastewater_structure"
    __table_args__ = {"schema": SCHEMA}


class channel(wastewater_structure):
    __tablename__ = "channel"
    __table_args__ = {"schema": SCHEMA}


class manhole(wastewater_structure):
    __tablename__ = "manhole"
    __table_args__ = {"schema": SCHEMA}


class discharge_point(wastewater_structure):
    __tablename__ = "discharge_point"
    __table_args__ = {"schema": SCHEMA}


class special_structure(wastewater_structure):
    __tablename__ = "special_structure"
    __table_args__ = {"schema": SCHEMA}


class infiltration_installation(wastewater_structure):
    __tablename__ = "infiltration_installation"
    __table_args__ = {"schema": SCHEMA}


class wwtp_structure(wastewater_structure):
    __tablename__ = "wwtp_structure"
    __table_args__ = {"schema": SCHEMA}


class maintenance_event(Base):
    __tablename__ = "maintenance_event"
    __table_args__ = {"schema": SCHEMA}


class zone(Base):
    __tablename__ = "zone"
    __table_args__ = {"schema": SCHEMA}


class infiltration_zone(zone):
    __tablename__ = "infiltration_zone"
    __table_args__ = {"schema": SCHEMA}


class drainage_system(zone):
    __tablename__ = "drainage_system"
    __table_args__ = {"schema": SCHEMA}


class pipe_profile(Base):
    __tablename__ = "pipe_profile"
    __table_args__ = {"schema": SCHEMA}


class wwtp_energy_use(Base):
    __tablename__ = "wwtp_energy_use"
    __table_args__ = {"schema": SCHEMA}


class waste_water_treatment(Base):
    __tablename__ = "waste_water_treatment"
    __table_args__ = {"schema": SCHEMA}


class sludge_treatment(Base):
    __tablename__ = "sludge_treatment"
    __table_args__ = {"schema": SCHEMA}


class control_center(Base):
    __tablename__ = "control_center"
    __table_args__ = {"schema": SCHEMA}


class hydr_geometry(Base):
    __tablename__ = "hydr_geometry"
    __table_args__ = {"schema": SCHEMA}


class wastewater_networkelement(Base):
    __tablename__ = "wastewater_networkelement"
    __table_args__ = {"schema": SCHEMA}


class reach_point(Base):
    __tablename__ = "reach_point"
    __table_args__ = {"schema": SCHEMA}


class wastewater_node(wastewater_networkelement):
    __tablename__ = "wastewater_node"
    __table_args__ = {"schema": SCHEMA}


class reach(wastewater_networkelement):
    __tablename__ = "reach"
    __table_args__ = {"schema": SCHEMA}


class profile_geometry(Base):
    __tablename__ = "profile_geometry"
    __table_args__ = {"schema": SCHEMA}


class hydr_geom_relation(Base):
    __tablename__ = "hydr_geom_relation"
    __table_args__ = {"schema": SCHEMA}


class mechanical_pretreatment(Base):
    __tablename__ = "mechanical_pretreatment"
    __table_args__ = {"schema": SCHEMA}


class retention_body(Base):
    __tablename__ = "retention_body"
    __table_args__ = {"schema": SCHEMA}


class overflow_char(Base):
    __tablename__ = "overflow_char"
    __table_args__ = {"schema": SCHEMA}


class hq_relation(Base):
    __tablename__ = "hq_relation"
    __table_args__ = {"schema": SCHEMA}


class structure_part(Base):
    __tablename__ = "structure_part"
    __table_args__ = {"schema": SCHEMA}


class dryweather_downspout(structure_part):
    __tablename__ = "dryweather_downspout"
    __table_args__ = {"schema": SCHEMA}


class access_aid(structure_part):
    __tablename__ = "access_aid"
    __table_args__ = {"schema": SCHEMA}


class dryweather_flume(structure_part):
    __tablename__ = "dryweather_flume"
    __table_args__ = {"schema": SCHEMA}


class cover(structure_part):
    __tablename__ = "cover"
    __table_args__ = {"schema": SCHEMA}


class electric_equipment(structure_part):
    __tablename__ = "electric_equipment"
    __table_args__ = {"schema": SCHEMA}


class electromechanical_equipment(structure_part):
    __tablename__ = "electromechanical_equipment"
    __table_args__ = {"schema": SCHEMA}


class benching(structure_part):
    __tablename__ = "benching"
    __table_args__ = {"schema": SCHEMA}


class connection_object(Base):
    __tablename__ = "connection_object"
    __table_args__ = {"schema": SCHEMA}


class building(connection_object):
    __tablename__ = "building"
    __table_args__ = {"schema": SCHEMA}


class reservoir(connection_object):
    __tablename__ = "reservoir"
    __table_args__ = {"schema": SCHEMA}


class individual_surface(connection_object):
    __tablename__ = "individual_surface"
    __table_args__ = {"schema": SCHEMA}


class fountain(connection_object):
    __tablename__ = "fountain"
    __table_args__ = {"schema": SCHEMA}


class catchment_area(Base):
    __tablename__ = "catchment_area"
    __table_args__ = {"schema": SCHEMA}


class surface_runoff_parameters(Base):
    __tablename__ = "surface_runoff_parameters"
    __table_args__ = {"schema": SCHEMA}


class measuring_point(Base):
    __tablename__ = "measuring_point"
    __table_args__ = {"schema": SCHEMA}


class measuring_device(Base):
    __tablename__ = "measuring_device"
    __table_args__ = {"schema": SCHEMA}


class measurement_series(Base):
    __tablename__ = "measurement_series"
    __table_args__ = {"schema": SCHEMA}


class measurement_result(Base):
    __tablename__ = "measurement_result"
    __table_args__ = {"schema": SCHEMA}


class overflow(Base):
    __tablename__ = "overflow"
    __table_args__ = {"schema": SCHEMA}


class throttle_shut_off_unit(Base):
    __tablename__ = "throttle_shut_off_unit"
    __table_args__ = {"schema": SCHEMA}


class prank_weir(overflow):
    __tablename__ = "prank_weir"
    __table_args__ = {"schema": SCHEMA}


class pump(overflow):
    __tablename__ = "pump"
    __table_args__ = {"schema": SCHEMA}


class leapingweir(overflow):
    __tablename__ = "leapingweir"
    __table_args__ = {"schema": SCHEMA}


class hydraulic_char_data(Base):
    __tablename__ = "hydraulic_char_data"
    __table_args__ = {"schema": SCHEMA}


class backflow_prevention(structure_part):
    __tablename__ = "backflow_prevention"
    __table_args__ = {"schema": SCHEMA}


class solids_retention(structure_part):
    __tablename__ = "solids_retention"
    __table_args__ = {"schema": SCHEMA}


class tank_cleaning(structure_part):
    __tablename__ = "tank_cleaning"
    __table_args__ = {"schema": SCHEMA}


class tank_emptying(structure_part):
    __tablename__ = "tank_emptying"
    __table_args__ = {"schema": SCHEMA}


class param_ca_general(surface_runoff_parameters):
    __tablename__ = "param_ca_general"
    __table_args__ = {"schema": SCHEMA}


class param_ca_mouse1(surface_runoff_parameters):
    __tablename__ = "param_ca_mouse1"
    __table_args__ = {"schema": SCHEMA}


_prepared = False


def get_qgep_model():
    global _prepared
    if not _prepared:
        utils.sqlalchemy.prepare_automap_base(Base, SCHEMA)
        _prepared = True
    return Base.classes
