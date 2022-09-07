from .model_abwasser import get_abwasser_model
from .model_qgep import get_qgep_model


def get_qgep_mapping():
    QGEP = get_qgep_model()
    ABWASSER = get_abwasser_model()

    return {
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
     }
