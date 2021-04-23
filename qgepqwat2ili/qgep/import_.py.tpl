from sqlalchemy.orm import Session
from geoalchemy2.functions import ST_Transform, ST_Force2D

from .. import utils

from .model_qgep import get_qgep_model
from .model_abwasser import get_abwasser_model


def qgep_import():

    QGEP = get_qgep_model()
    ABWASSER = get_abwasser_model()

    abwasser_session = Session(utils.sqlalchemy.create_engine(), autocommit=False, autoflush=False)
    qgep_session = Session(utils.sqlalchemy.create_engine(), autocommit=False, autoflush=False)

    print("Importing ABWASSER.organisation, ABWASSER.metaattribute -> QGEP.organisation")
    for row, metaattribute in abwasser_session.query(ABWASSER.organisation, ABWASSER.metaattribute).join(ABWASSER.metaattribute):

        # baseclass --- organisation.t_type, organisation.t_ili_tid
        # sia405_baseclass --- organisation.obj_id
        # organisation --- organisation.t_id, organisation.bemerkung, organisation.bezeichnung, organisation.auid

        # metaattribute --- metaattribute.t_id, metaattribute.t_ili_tid, metaattribute.t_seq, metaattribute.datenherr, metaattribute.datenlieferant, metaattribute.letzte_aenderung, metaattribute.sia405_baseclass_metaattribute
        # _rel_ --- metaattribute.sia405_baseclass_metaattribute__REL

        organisation = QGEP.organisation(

            # --- organisation ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)
            # identifier=row.REPLACE_ME,  # VARCHAR(80)
            # remark=row.REPLACE_ME,  # VARCHAR(80)
            # uid=row.REPLACE_ME,  # VARCHAR(12)
            # last_modification=row.REPLACE_ME,  # TIMESTAMP WITHOUT TIME ZONE
            # fk_dataowner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_provider=row.REPLACE_ME,  # VARCHAR(16)
        )
        qgep_session.add(organisation)
        print(".", end="")
    print("done")

    print("Importing ABWASSER.kanal, ABWASSER.metaattribute -> QGEP.channel")
    for row, metaattribute in abwasser_session.query(ABWASSER.kanal, ABWASSER.metaattribute).join(ABWASSER.metaattribute):

        # baseclass --- kanal.t_type, kanal.t_ili_tid
        # sia405_baseclass --- kanal.obj_id
        # abwasserbauwerk --- kanal.akten, kanal.baujahr, kanal.baulicherzustand, kanal.baulos, kanal.bemerkung, kanal.bezeichnung, kanal.bruttokosten, kanal.detailgeometrie, kanal.ersatzjahr, kanal.finanzierung, kanal.inspektionsintervall, kanal.sanierungsbedarf, kanal.standortname, kanal.astatus, kanal.subventionen, kanal.wbw_basisjahr, kanal.wbw_bauart, kanal.wiederbeschaffungswert, kanal.zugaenglichkeit, kanal.betreiberref, kanal.eigentuemerref
        # kanal --- kanal.t_id, kanal.bettung_umhuellung, kanal.funktionhierarchisch, kanal.funktionhydraulisch, kanal.nutzungsart_geplant, kanal.nutzungsart_ist, kanal.rohrlaenge, kanal.spuelintervall, kanal.verbindungsart
        # _bwrel_ --- kanal.sia405_textpos__BWREL_abwasserbauwerkref, kanal.sia405_symbolpos__BWREL_abwasserbauwerkref, kanal.haltung_alternativverlauf__BWREL_t_id, kanal.symbolpos__BWREL_t_id, kanal.metaattribute__BWREL_sia405_baseclass_metaattribute, kanal.textpos__BWREL_t_id, kanal.abwassernetzelement__BWREL_abwasserbauwerkref, kanal.erhaltungsereignis__BWREL_abwasserbauwerkref, kanal.bauwerksteil__BWREL_abwasserbauwerkref
        # _rel_ --- kanal.betreiberref__REL, kanal.eigentuemerref__REL

        # metaattribute --- metaattribute.t_id, metaattribute.t_ili_tid, metaattribute.t_seq, metaattribute.datenherr, metaattribute.datenlieferant, metaattribute.letzte_aenderung, metaattribute.sia405_baseclass_metaattribute
        # _rel_ --- metaattribute.sia405_baseclass_metaattribute__REL

        channel = QGEP.channel(

            # --- wastewater_structure ---
            # accessibility=row.REPLACE_ME,  # INTEGER
            # contract_section=row.REPLACE_ME,  # VARCHAR(50)
            # detail_geometry_geometry=row.REPLACE_ME,  # geometry(CURVEPOLYGONZ,2056)
            # financing=row.REPLACE_ME,  # INTEGER
            # gross_costs=row.REPLACE_ME,  # NUMERIC(10, 2)
            # identifier=row.REPLACE_ME,  # VARCHAR(20)
            # inspection_interval=row.REPLACE_ME,  # NUMERIC(4, 2)
            # location_name=row.REPLACE_ME,  # VARCHAR(50)
            # records=row.REPLACE_ME,  # VARCHAR(255)
            # remark=row.REPLACE_ME,  # VARCHAR(80)
            # renovation_necessity=row.REPLACE_ME,  # INTEGER
            # replacement_value=row.REPLACE_ME,  # NUMERIC(10, 2)
            # rv_base_year=row.REPLACE_ME,  # SMALLINT
            # rv_construction_type=row.REPLACE_ME,  # INTEGER
            # status=row.REPLACE_ME,  # INTEGER
            # structure_condition=row.REPLACE_ME,  # INTEGER
            # subsidies=row.REPLACE_ME,  # NUMERIC(10, 2)
            # year_of_construction=row.REPLACE_ME,  # SMALLINT
            # year_of_replacement=row.REPLACE_ME,  # SMALLINT
            # last_modification=row.REPLACE_ME,  # TIMESTAMP WITHOUT TIME ZONE
            # fk_dataowner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_provider=row.REPLACE_ME,  # VARCHAR(16)
            # fk_owner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_operator=row.REPLACE_ME,  # VARCHAR(16)
            # _usage_current=row.REPLACE_ME,  # INTEGER
            # _function_hierarchic=row.REPLACE_ME,  # INTEGER
            # _label=row.REPLACE_ME,  # TEXT
            # fk_main_cover=row.REPLACE_ME,  # VARCHAR(16)
            # _depth=row.REPLACE_ME,  # NUMERIC(6, 3)
            # fk_main_wastewater_node=row.REPLACE_ME,  # VARCHAR(16)
            # _cover_label=row.REPLACE_ME,  # TEXT
            # _input_label=row.REPLACE_ME,  # TEXT
            # _output_label=row.REPLACE_ME,  # TEXT
            # _bottom_label=row.REPLACE_ME,  # TEXT

            # --- channel ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)
            # bedding_encasement=row.REPLACE_ME,  # INTEGER
            # connection_type=row.REPLACE_ME,  # INTEGER
            # function_hierarchic=row.REPLACE_ME,  # INTEGER
            # function_hydraulic=row.REPLACE_ME,  # INTEGER
            # jetting_interval=row.REPLACE_ME,  # NUMERIC(4, 2)
            # pipe_length=row.REPLACE_ME,  # NUMERIC(7, 2)
            # usage_current=row.REPLACE_ME,  # INTEGER
            # usage_planned=row.REPLACE_ME,  # INTEGER
        )
        qgep_session.add(channel)
        print(".", end="")
    print("done")

    print("Importing ABWASSER.normschacht, ABWASSER.metaattribute -> QGEP.manhole")
    for row, metaattribute in abwasser_session.query(ABWASSER.normschacht, ABWASSER.metaattribute).join(ABWASSER.metaattribute):

        # baseclass --- normschacht.t_type, normschacht.t_ili_tid
        # sia405_baseclass --- normschacht.obj_id
        # abwasserbauwerk --- normschacht.akten, normschacht.baujahr, normschacht.baulicherzustand, normschacht.baulos, normschacht.bemerkung, normschacht.bezeichnung, normschacht.bruttokosten, normschacht.detailgeometrie, normschacht.ersatzjahr, normschacht.finanzierung, normschacht.inspektionsintervall, normschacht.sanierungsbedarf, normschacht.standortname, normschacht.astatus, normschacht.subventionen, normschacht.wbw_basisjahr, normschacht.wbw_bauart, normschacht.wiederbeschaffungswert, normschacht.zugaenglichkeit, normschacht.betreiberref, normschacht.eigentuemerref
        # normschacht --- normschacht.t_id, normschacht.dimension1, normschacht.dimension2, normschacht.funktion, normschacht.material, normschacht.oberflaechenzulauf
        # _bwrel_ --- normschacht.sia405_textpos__BWREL_abwasserbauwerkref, normschacht.sia405_symbolpos__BWREL_abwasserbauwerkref, normschacht.haltung_alternativverlauf__BWREL_t_id, normschacht.symbolpos__BWREL_t_id, normschacht.metaattribute__BWREL_sia405_baseclass_metaattribute, normschacht.textpos__BWREL_t_id, normschacht.abwassernetzelement__BWREL_abwasserbauwerkref, normschacht.erhaltungsereignis__BWREL_abwasserbauwerkref, normschacht.bauwerksteil__BWREL_abwasserbauwerkref
        # _rel_ --- normschacht.betreiberref__REL, normschacht.eigentuemerref__REL

        # metaattribute --- metaattribute.t_id, metaattribute.t_ili_tid, metaattribute.t_seq, metaattribute.datenherr, metaattribute.datenlieferant, metaattribute.letzte_aenderung, metaattribute.sia405_baseclass_metaattribute
        # _rel_ --- metaattribute.sia405_baseclass_metaattribute__REL

        manhole = QGEP.manhole(

            # --- wastewater_structure ---
            # accessibility=row.REPLACE_ME,  # INTEGER
            # contract_section=row.REPLACE_ME,  # VARCHAR(50)
            # detail_geometry_geometry=row.REPLACE_ME,  # geometry(CURVEPOLYGONZ,2056)
            # financing=row.REPLACE_ME,  # INTEGER
            # gross_costs=row.REPLACE_ME,  # NUMERIC(10, 2)
            # identifier=row.REPLACE_ME,  # VARCHAR(20)
            # inspection_interval=row.REPLACE_ME,  # NUMERIC(4, 2)
            # location_name=row.REPLACE_ME,  # VARCHAR(50)
            # records=row.REPLACE_ME,  # VARCHAR(255)
            # remark=row.REPLACE_ME,  # VARCHAR(80)
            # renovation_necessity=row.REPLACE_ME,  # INTEGER
            # replacement_value=row.REPLACE_ME,  # NUMERIC(10, 2)
            # rv_base_year=row.REPLACE_ME,  # SMALLINT
            # rv_construction_type=row.REPLACE_ME,  # INTEGER
            # status=row.REPLACE_ME,  # INTEGER
            # structure_condition=row.REPLACE_ME,  # INTEGER
            # subsidies=row.REPLACE_ME,  # NUMERIC(10, 2)
            # year_of_construction=row.REPLACE_ME,  # SMALLINT
            # year_of_replacement=row.REPLACE_ME,  # SMALLINT
            # last_modification=row.REPLACE_ME,  # TIMESTAMP WITHOUT TIME ZONE
            # fk_dataowner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_provider=row.REPLACE_ME,  # VARCHAR(16)
            # fk_owner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_operator=row.REPLACE_ME,  # VARCHAR(16)
            # _usage_current=row.REPLACE_ME,  # INTEGER
            # _function_hierarchic=row.REPLACE_ME,  # INTEGER
            # _label=row.REPLACE_ME,  # TEXT
            # fk_main_cover=row.REPLACE_ME,  # VARCHAR(16)
            # _depth=row.REPLACE_ME,  # NUMERIC(6, 3)
            # fk_main_wastewater_node=row.REPLACE_ME,  # VARCHAR(16)
            # _cover_label=row.REPLACE_ME,  # TEXT
            # _input_label=row.REPLACE_ME,  # TEXT
            # _output_label=row.REPLACE_ME,  # TEXT
            # _bottom_label=row.REPLACE_ME,  # TEXT

            # --- manhole ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)
            # dimension1=row.REPLACE_ME,  # SMALLINT
            # dimension2=row.REPLACE_ME,  # SMALLINT
            # function=row.REPLACE_ME,  # INTEGER
            # material=row.REPLACE_ME,  # INTEGER
            # surface_inflow=row.REPLACE_ME,  # INTEGER
            # _orientation=row.REPLACE_ME,  # NUMERIC
        )
        qgep_session.add(manhole)
        print(".", end="")
    print("done")

    print("Importing ABWASSER.einleitstelle, ABWASSER.metaattribute -> QGEP.discharge_point")
    for row, metaattribute in abwasser_session.query(ABWASSER.einleitstelle, ABWASSER.metaattribute).join(ABWASSER.metaattribute):

        # baseclass --- einleitstelle.t_type, einleitstelle.t_ili_tid
        # sia405_baseclass --- einleitstelle.obj_id
        # abwasserbauwerk --- einleitstelle.akten, einleitstelle.baujahr, einleitstelle.baulicherzustand, einleitstelle.baulos, einleitstelle.bemerkung, einleitstelle.bezeichnung, einleitstelle.bruttokosten, einleitstelle.detailgeometrie, einleitstelle.ersatzjahr, einleitstelle.finanzierung, einleitstelle.inspektionsintervall, einleitstelle.sanierungsbedarf, einleitstelle.standortname, einleitstelle.astatus, einleitstelle.subventionen, einleitstelle.wbw_basisjahr, einleitstelle.wbw_bauart, einleitstelle.wiederbeschaffungswert, einleitstelle.zugaenglichkeit, einleitstelle.betreiberref, einleitstelle.eigentuemerref
        # einleitstelle --- einleitstelle.t_id, einleitstelle.hochwasserkote, einleitstelle.relevanz, einleitstelle.terrainkote, einleitstelle.wasserspiegel_hydraulik
        # _bwrel_ --- einleitstelle.sia405_textpos__BWREL_abwasserbauwerkref, einleitstelle.sia405_symbolpos__BWREL_abwasserbauwerkref, einleitstelle.haltung_alternativverlauf__BWREL_t_id, einleitstelle.symbolpos__BWREL_t_id, einleitstelle.metaattribute__BWREL_sia405_baseclass_metaattribute, einleitstelle.textpos__BWREL_t_id, einleitstelle.abwassernetzelement__BWREL_abwasserbauwerkref, einleitstelle.erhaltungsereignis__BWREL_abwasserbauwerkref, einleitstelle.bauwerksteil__BWREL_abwasserbauwerkref
        # _rel_ --- einleitstelle.betreiberref__REL, einleitstelle.eigentuemerref__REL

        # metaattribute --- metaattribute.t_id, metaattribute.t_ili_tid, metaattribute.t_seq, metaattribute.datenherr, metaattribute.datenlieferant, metaattribute.letzte_aenderung, metaattribute.sia405_baseclass_metaattribute
        # _rel_ --- metaattribute.sia405_baseclass_metaattribute__REL

        discharge_point = QGEP.discharge_point(

            # --- wastewater_structure ---
            # accessibility=row.REPLACE_ME,  # INTEGER
            # contract_section=row.REPLACE_ME,  # VARCHAR(50)
            # detail_geometry_geometry=row.REPLACE_ME,  # geometry(CURVEPOLYGONZ,2056)
            # financing=row.REPLACE_ME,  # INTEGER
            # gross_costs=row.REPLACE_ME,  # NUMERIC(10, 2)
            # identifier=row.REPLACE_ME,  # VARCHAR(20)
            # inspection_interval=row.REPLACE_ME,  # NUMERIC(4, 2)
            # location_name=row.REPLACE_ME,  # VARCHAR(50)
            # records=row.REPLACE_ME,  # VARCHAR(255)
            # remark=row.REPLACE_ME,  # VARCHAR(80)
            # renovation_necessity=row.REPLACE_ME,  # INTEGER
            # replacement_value=row.REPLACE_ME,  # NUMERIC(10, 2)
            # rv_base_year=row.REPLACE_ME,  # SMALLINT
            # rv_construction_type=row.REPLACE_ME,  # INTEGER
            # status=row.REPLACE_ME,  # INTEGER
            # structure_condition=row.REPLACE_ME,  # INTEGER
            # subsidies=row.REPLACE_ME,  # NUMERIC(10, 2)
            # year_of_construction=row.REPLACE_ME,  # SMALLINT
            # year_of_replacement=row.REPLACE_ME,  # SMALLINT
            # last_modification=row.REPLACE_ME,  # TIMESTAMP WITHOUT TIME ZONE
            # fk_dataowner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_provider=row.REPLACE_ME,  # VARCHAR(16)
            # fk_owner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_operator=row.REPLACE_ME,  # VARCHAR(16)
            # _usage_current=row.REPLACE_ME,  # INTEGER
            # _function_hierarchic=row.REPLACE_ME,  # INTEGER
            # _label=row.REPLACE_ME,  # TEXT
            # fk_main_cover=row.REPLACE_ME,  # VARCHAR(16)
            # _depth=row.REPLACE_ME,  # NUMERIC(6, 3)
            # fk_main_wastewater_node=row.REPLACE_ME,  # VARCHAR(16)
            # _cover_label=row.REPLACE_ME,  # TEXT
            # _input_label=row.REPLACE_ME,  # TEXT
            # _output_label=row.REPLACE_ME,  # TEXT
            # _bottom_label=row.REPLACE_ME,  # TEXT

            # --- discharge_point ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)
            # highwater_level=row.REPLACE_ME,  # NUMERIC(7, 3)
            # relevance=row.REPLACE_ME,  # INTEGER
            # terrain_level=row.REPLACE_ME,  # NUMERIC(7, 3)
            # upper_elevation=row.REPLACE_ME,  # NUMERIC(7, 3)
            # waterlevel_hydraulic=row.REPLACE_ME,  # NUMERIC(7, 3)
            # fk_sector_water_body=row.REPLACE_ME,  # VARCHAR(16)
        )
        qgep_session.add(discharge_point)
        print(".", end="")
    print("done")

    print("Importing ABWASSER.spezialbauwerk, ABWASSER.metaattribute -> QGEP.special_structure")
    for row, metaattribute in abwasser_session.query(ABWASSER.spezialbauwerk, ABWASSER.metaattribute).join(ABWASSER.metaattribute):

        # baseclass --- spezialbauwerk.t_type, spezialbauwerk.t_ili_tid
        # sia405_baseclass --- spezialbauwerk.obj_id
        # abwasserbauwerk --- spezialbauwerk.akten, spezialbauwerk.baujahr, spezialbauwerk.baulicherzustand, spezialbauwerk.baulos, spezialbauwerk.bemerkung, spezialbauwerk.bezeichnung, spezialbauwerk.bruttokosten, spezialbauwerk.detailgeometrie, spezialbauwerk.ersatzjahr, spezialbauwerk.finanzierung, spezialbauwerk.inspektionsintervall, spezialbauwerk.sanierungsbedarf, spezialbauwerk.standortname, spezialbauwerk.astatus, spezialbauwerk.subventionen, spezialbauwerk.wbw_basisjahr, spezialbauwerk.wbw_bauart, spezialbauwerk.wiederbeschaffungswert, spezialbauwerk.zugaenglichkeit, spezialbauwerk.betreiberref, spezialbauwerk.eigentuemerref
        # spezialbauwerk --- spezialbauwerk.t_id, spezialbauwerk.bypass, spezialbauwerk.funktion, spezialbauwerk.notueberlauf, spezialbauwerk.regenbecken_anordnung
        # _bwrel_ --- spezialbauwerk.sia405_textpos__BWREL_abwasserbauwerkref, spezialbauwerk.sia405_symbolpos__BWREL_abwasserbauwerkref, spezialbauwerk.haltung_alternativverlauf__BWREL_t_id, spezialbauwerk.symbolpos__BWREL_t_id, spezialbauwerk.metaattribute__BWREL_sia405_baseclass_metaattribute, spezialbauwerk.textpos__BWREL_t_id, spezialbauwerk.abwassernetzelement__BWREL_abwasserbauwerkref, spezialbauwerk.erhaltungsereignis__BWREL_abwasserbauwerkref, spezialbauwerk.bauwerksteil__BWREL_abwasserbauwerkref
        # _rel_ --- spezialbauwerk.betreiberref__REL, spezialbauwerk.eigentuemerref__REL

        # metaattribute --- metaattribute.t_id, metaattribute.t_ili_tid, metaattribute.t_seq, metaattribute.datenherr, metaattribute.datenlieferant, metaattribute.letzte_aenderung, metaattribute.sia405_baseclass_metaattribute
        # _rel_ --- metaattribute.sia405_baseclass_metaattribute__REL

        special_structure = QGEP.special_structure(

            # --- wastewater_structure ---
            # accessibility=row.REPLACE_ME,  # INTEGER
            # contract_section=row.REPLACE_ME,  # VARCHAR(50)
            # detail_geometry_geometry=row.REPLACE_ME,  # geometry(CURVEPOLYGONZ,2056)
            # financing=row.REPLACE_ME,  # INTEGER
            # gross_costs=row.REPLACE_ME,  # NUMERIC(10, 2)
            # identifier=row.REPLACE_ME,  # VARCHAR(20)
            # inspection_interval=row.REPLACE_ME,  # NUMERIC(4, 2)
            # location_name=row.REPLACE_ME,  # VARCHAR(50)
            # records=row.REPLACE_ME,  # VARCHAR(255)
            # remark=row.REPLACE_ME,  # VARCHAR(80)
            # renovation_necessity=row.REPLACE_ME,  # INTEGER
            # replacement_value=row.REPLACE_ME,  # NUMERIC(10, 2)
            # rv_base_year=row.REPLACE_ME,  # SMALLINT
            # rv_construction_type=row.REPLACE_ME,  # INTEGER
            # status=row.REPLACE_ME,  # INTEGER
            # structure_condition=row.REPLACE_ME,  # INTEGER
            # subsidies=row.REPLACE_ME,  # NUMERIC(10, 2)
            # year_of_construction=row.REPLACE_ME,  # SMALLINT
            # year_of_replacement=row.REPLACE_ME,  # SMALLINT
            # last_modification=row.REPLACE_ME,  # TIMESTAMP WITHOUT TIME ZONE
            # fk_dataowner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_provider=row.REPLACE_ME,  # VARCHAR(16)
            # fk_owner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_operator=row.REPLACE_ME,  # VARCHAR(16)
            # _usage_current=row.REPLACE_ME,  # INTEGER
            # _function_hierarchic=row.REPLACE_ME,  # INTEGER
            # _label=row.REPLACE_ME,  # TEXT
            # fk_main_cover=row.REPLACE_ME,  # VARCHAR(16)
            # _depth=row.REPLACE_ME,  # NUMERIC(6, 3)
            # fk_main_wastewater_node=row.REPLACE_ME,  # VARCHAR(16)
            # _cover_label=row.REPLACE_ME,  # TEXT
            # _input_label=row.REPLACE_ME,  # TEXT
            # _output_label=row.REPLACE_ME,  # TEXT
            # _bottom_label=row.REPLACE_ME,  # TEXT

            # --- special_structure ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)
            # bypass=row.REPLACE_ME,  # INTEGER
            # emergency_spillway=row.REPLACE_ME,  # INTEGER
            # function=row.REPLACE_ME,  # INTEGER
            # stormwater_tank_arrangement=row.REPLACE_ME,  # INTEGER
            # upper_elevation=row.REPLACE_ME,  # NUMERIC(7, 3)
        )
        qgep_session.add(special_structure)
        print(".", end="")
    print("done")

    print("Importing ABWASSER.versickerungsanlage, ABWASSER.metaattribute -> QGEP.infiltration_installation")
    for row, metaattribute in abwasser_session.query(ABWASSER.versickerungsanlage, ABWASSER.metaattribute).join(ABWASSER.metaattribute):

        # baseclass --- versickerungsanlage.t_type, versickerungsanlage.t_ili_tid
        # sia405_baseclass --- versickerungsanlage.obj_id
        # abwasserbauwerk --- versickerungsanlage.akten, versickerungsanlage.baujahr, versickerungsanlage.baulicherzustand, versickerungsanlage.baulos, versickerungsanlage.bemerkung, versickerungsanlage.bezeichnung, versickerungsanlage.bruttokosten, versickerungsanlage.detailgeometrie, versickerungsanlage.ersatzjahr, versickerungsanlage.finanzierung, versickerungsanlage.inspektionsintervall, versickerungsanlage.sanierungsbedarf, versickerungsanlage.standortname, versickerungsanlage.astatus, versickerungsanlage.subventionen, versickerungsanlage.wbw_basisjahr, versickerungsanlage.wbw_bauart, versickerungsanlage.wiederbeschaffungswert, versickerungsanlage.zugaenglichkeit, versickerungsanlage.betreiberref, versickerungsanlage.eigentuemerref
        # versickerungsanlage --- versickerungsanlage.t_id, versickerungsanlage.art, versickerungsanlage.beschriftung, versickerungsanlage.dimension1, versickerungsanlage.dimension2, versickerungsanlage.gwdistanz, versickerungsanlage.maengel, versickerungsanlage.notueberlauf, versickerungsanlage.saugwagen, versickerungsanlage.schluckvermoegen, versickerungsanlage.versickerungswasser, versickerungsanlage.wasserdichtheit, versickerungsanlage.wirksameflaeche
        # _bwrel_ --- versickerungsanlage.sia405_textpos__BWREL_abwasserbauwerkref, versickerungsanlage.sia405_symbolpos__BWREL_abwasserbauwerkref, versickerungsanlage.haltung_alternativverlauf__BWREL_t_id, versickerungsanlage.symbolpos__BWREL_t_id, versickerungsanlage.metaattribute__BWREL_sia405_baseclass_metaattribute, versickerungsanlage.textpos__BWREL_t_id, versickerungsanlage.abwassernetzelement__BWREL_abwasserbauwerkref, versickerungsanlage.erhaltungsereignis__BWREL_abwasserbauwerkref, versickerungsanlage.bauwerksteil__BWREL_abwasserbauwerkref
        # _rel_ --- versickerungsanlage.betreiberref__REL, versickerungsanlage.eigentuemerref__REL

        # metaattribute --- metaattribute.t_id, metaattribute.t_ili_tid, metaattribute.t_seq, metaattribute.datenherr, metaattribute.datenlieferant, metaattribute.letzte_aenderung, metaattribute.sia405_baseclass_metaattribute
        # _rel_ --- metaattribute.sia405_baseclass_metaattribute__REL

        infiltration_installation = QGEP.infiltration_installation(

            # --- wastewater_structure ---
            # accessibility=row.REPLACE_ME,  # INTEGER
            # contract_section=row.REPLACE_ME,  # VARCHAR(50)
            # detail_geometry_geometry=row.REPLACE_ME,  # geometry(CURVEPOLYGONZ,2056)
            # financing=row.REPLACE_ME,  # INTEGER
            # gross_costs=row.REPLACE_ME,  # NUMERIC(10, 2)
            # identifier=row.REPLACE_ME,  # VARCHAR(20)
            # inspection_interval=row.REPLACE_ME,  # NUMERIC(4, 2)
            # location_name=row.REPLACE_ME,  # VARCHAR(50)
            # records=row.REPLACE_ME,  # VARCHAR(255)
            # remark=row.REPLACE_ME,  # VARCHAR(80)
            # renovation_necessity=row.REPLACE_ME,  # INTEGER
            # replacement_value=row.REPLACE_ME,  # NUMERIC(10, 2)
            # rv_base_year=row.REPLACE_ME,  # SMALLINT
            # rv_construction_type=row.REPLACE_ME,  # INTEGER
            # status=row.REPLACE_ME,  # INTEGER
            # structure_condition=row.REPLACE_ME,  # INTEGER
            # subsidies=row.REPLACE_ME,  # NUMERIC(10, 2)
            # year_of_construction=row.REPLACE_ME,  # SMALLINT
            # year_of_replacement=row.REPLACE_ME,  # SMALLINT
            # last_modification=row.REPLACE_ME,  # TIMESTAMP WITHOUT TIME ZONE
            # fk_dataowner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_provider=row.REPLACE_ME,  # VARCHAR(16)
            # fk_owner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_operator=row.REPLACE_ME,  # VARCHAR(16)
            # _usage_current=row.REPLACE_ME,  # INTEGER
            # _function_hierarchic=row.REPLACE_ME,  # INTEGER
            # _label=row.REPLACE_ME,  # TEXT
            # fk_main_cover=row.REPLACE_ME,  # VARCHAR(16)
            # _depth=row.REPLACE_ME,  # NUMERIC(6, 3)
            # fk_main_wastewater_node=row.REPLACE_ME,  # VARCHAR(16)
            # _cover_label=row.REPLACE_ME,  # TEXT
            # _input_label=row.REPLACE_ME,  # TEXT
            # _output_label=row.REPLACE_ME,  # TEXT
            # _bottom_label=row.REPLACE_ME,  # TEXT

            # --- infiltration_installation ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)
            # absorption_capacity=row.REPLACE_ME,  # NUMERIC(9, 3)
            # defects=row.REPLACE_ME,  # INTEGER
            # dimension1=row.REPLACE_ME,  # SMALLINT
            # dimension2=row.REPLACE_ME,  # SMALLINT
            # distance_to_aquifer=row.REPLACE_ME,  # NUMERIC(7, 2)
            # effective_area=row.REPLACE_ME,  # NUMERIC(8, 2)
            # emergency_spillway=row.REPLACE_ME,  # INTEGER
            # kind=row.REPLACE_ME,  # INTEGER
            # labeling=row.REPLACE_ME,  # INTEGER
            # seepage_utilization=row.REPLACE_ME,  # INTEGER
            # upper_elevation=row.REPLACE_ME,  # NUMERIC(7, 3)
            # vehicle_access=row.REPLACE_ME,  # INTEGER
            # watertightness=row.REPLACE_ME,  # INTEGER
            # fk_aquifier=row.REPLACE_ME,  # VARCHAR(16)
        )
        qgep_session.add(infiltration_installation)
        print(".", end="")
    print("done")

    print("Importing ABWASSER.rohrprofil, ABWASSER.metaattribute -> QGEP.pipe_profile")
    for row, metaattribute in abwasser_session.query(ABWASSER.rohrprofil, ABWASSER.metaattribute).join(ABWASSER.metaattribute):

        # baseclass --- rohrprofil.t_type, rohrprofil.t_ili_tid
        # sia405_baseclass --- rohrprofil.obj_id
        # rohrprofil --- rohrprofil.t_id, rohrprofil.bemerkung, rohrprofil.bezeichnung, rohrprofil.hoehenbreitenverhaeltnis, rohrprofil.profiltyp
        # _bwrel_ --- rohrprofil.haltung_alternativverlauf__BWREL_t_id, rohrprofil.symbolpos__BWREL_t_id, rohrprofil.metaattribute__BWREL_sia405_baseclass_metaattribute, rohrprofil.textpos__BWREL_t_id, rohrprofil.haltung__BWREL_rohrprofilref

        # metaattribute --- metaattribute.t_id, metaattribute.t_ili_tid, metaattribute.t_seq, metaattribute.datenherr, metaattribute.datenlieferant, metaattribute.letzte_aenderung, metaattribute.sia405_baseclass_metaattribute
        # _rel_ --- metaattribute.sia405_baseclass_metaattribute__REL

        pipe_profile = QGEP.pipe_profile(

            # --- pipe_profile ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)
            # height_width_ratio=row.REPLACE_ME,  # NUMERIC(5, 2)
            # identifier=row.REPLACE_ME,  # VARCHAR(20)
            # profile_type=row.REPLACE_ME,  # INTEGER
            # remark=row.REPLACE_ME,  # VARCHAR(80)
            # last_modification=row.REPLACE_ME,  # TIMESTAMP WITHOUT TIME ZONE
            # fk_dataowner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_provider=row.REPLACE_ME,  # VARCHAR(16)
        )
        qgep_session.add(pipe_profile)
        print(".", end="")
    print("done")

    print("Importing ABWASSER.haltungspunkt, ABWASSER.metaattribute -> QGEP.reach_point")
    for row, metaattribute in abwasser_session.query(ABWASSER.haltungspunkt, ABWASSER.metaattribute).join(ABWASSER.metaattribute):

        # baseclass --- haltungspunkt.t_type, haltungspunkt.t_ili_tid
        # sia405_baseclass --- haltungspunkt.obj_id
        # haltungspunkt --- haltungspunkt.t_id, haltungspunkt.auslaufform, haltungspunkt.bemerkung, haltungspunkt.bezeichnung, haltungspunkt.hoehengenauigkeit, haltungspunkt.kote, haltungspunkt.lage, haltungspunkt.lage_anschluss, haltungspunkt.abwassernetzelementref
        # _bwrel_ --- haltungspunkt.haltung_alternativverlauf__BWREL_t_id, haltungspunkt.symbolpos__BWREL_t_id, haltungspunkt.metaattribute__BWREL_sia405_baseclass_metaattribute, haltungspunkt.textpos__BWREL_t_id, haltungspunkt.haltung__BWREL_nachhaltungspunktref, haltungspunkt.haltung__BWREL_vonhaltungspunktref, haltungspunkt.untersuchung__BWREL_haltungspunktref
        # _rel_ --- haltungspunkt.abwassernetzelementref__REL

        # metaattribute --- metaattribute.t_id, metaattribute.t_ili_tid, metaattribute.t_seq, metaattribute.datenherr, metaattribute.datenlieferant, metaattribute.letzte_aenderung, metaattribute.sia405_baseclass_metaattribute
        # _rel_ --- metaattribute.sia405_baseclass_metaattribute__REL

        reach_point = QGEP.reach_point(

            # --- reach_point ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)
            # elevation_accuracy=row.REPLACE_ME,  # INTEGER
            # identifier=row.REPLACE_ME,  # VARCHAR(20)
            # level=row.REPLACE_ME,  # NUMERIC(7, 3)
            # outlet_shape=row.REPLACE_ME,  # INTEGER
            # position_of_connection=row.REPLACE_ME,  # SMALLINT
            # remark=row.REPLACE_ME,  # VARCHAR(80)
            # situation_geometry=row.REPLACE_ME,  # geometry(POINTZ,2056)
            # last_modification=row.REPLACE_ME,  # TIMESTAMP WITHOUT TIME ZONE
            # fk_dataowner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_provider=row.REPLACE_ME,  # VARCHAR(16)
            # fk_wastewater_networkelement=row.REPLACE_ME,  # VARCHAR(16)
        )
        qgep_session.add(reach_point)
        print(".", end="")
    print("done")

    print("Importing ABWASSER.abwasserknoten, ABWASSER.metaattribute -> QGEP.wastewater_node")
    for row, metaattribute in abwasser_session.query(ABWASSER.abwasserknoten, ABWASSER.metaattribute).join(ABWASSER.metaattribute):

        # baseclass --- abwasserknoten.t_type, abwasserknoten.t_ili_tid
        # sia405_baseclass --- abwasserknoten.obj_id
        # abwassernetzelement --- abwasserknoten.bemerkung, abwasserknoten.bezeichnung, abwasserknoten.abwasserbauwerkref
        # abwasserknoten --- abwasserknoten.t_id, abwasserknoten.lage, abwasserknoten.rueckstaukote, abwasserknoten.sohlenkote
        # _bwrel_ --- abwasserknoten.haltung_alternativverlauf__BWREL_t_id, abwasserknoten.symbolpos__BWREL_t_id, abwasserknoten.metaattribute__BWREL_sia405_baseclass_metaattribute, abwasserknoten.textpos__BWREL_t_id, abwasserknoten.haltungspunkt__BWREL_abwassernetzelementref
        # _rel_ --- abwasserknoten.abwasserbauwerkref__REL

        # metaattribute --- metaattribute.t_id, metaattribute.t_ili_tid, metaattribute.t_seq, metaattribute.datenherr, metaattribute.datenlieferant, metaattribute.letzte_aenderung, metaattribute.sia405_baseclass_metaattribute
        # _rel_ --- metaattribute.sia405_baseclass_metaattribute__REL

        wastewater_node = QGEP.wastewater_node(

            # --- wastewater_networkelement ---
            # identifier=row.REPLACE_ME,  # VARCHAR(20)
            # remark=row.REPLACE_ME,  # VARCHAR(80)
            # last_modification=row.REPLACE_ME,  # TIMESTAMP WITHOUT TIME ZONE
            # fk_dataowner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_provider=row.REPLACE_ME,  # VARCHAR(16)
            # fk_wastewater_structure=row.REPLACE_ME,  # VARCHAR(16)

            # --- wastewater_node ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)
            # backflow_level=row.REPLACE_ME,  # NUMERIC(7, 3)
            # bottom_level=row.REPLACE_ME,  # NUMERIC(7, 3)
            # situation_geometry=row.REPLACE_ME,  # geometry(POINTZ,2056)
            # fk_hydr_geometry=row.REPLACE_ME,  # VARCHAR(16)
        )
        qgep_session.add(wastewater_node)
        print(".", end="")
    print("done")

    print("Importing ABWASSER.haltung, ABWASSER.metaattribute -> QGEP.reach")
    for row, metaattribute in abwasser_session.query(ABWASSER.haltung, ABWASSER.metaattribute).join(ABWASSER.metaattribute):

        # baseclass --- haltung.t_type, haltung.t_ili_tid
        # sia405_baseclass --- haltung.obj_id
        # abwassernetzelement --- haltung.bemerkung, haltung.bezeichnung, haltung.abwasserbauwerkref
        # haltung --- haltung.t_id, haltung.innenschutz, haltung.laengeeffektiv, haltung.lagebestimmung, haltung.lichte_hoehe, haltung.material, haltung.plangefaelle, haltung.reibungsbeiwert, haltung.reliner_art, haltung.reliner_bautechnik, haltung.reliner_material, haltung.reliner_nennweite, haltung.ringsteifigkeit, haltung.verlauf, haltung.wandrauhigkeit, haltung.rohrprofilref, haltung.nachhaltungspunktref, haltung.vonhaltungspunktref
        # _bwrel_ --- haltung.sia405_textpos__BWREL_haltungref, haltung.haltung_alternativverlauf__BWREL_haltungref, haltung.haltung_alternativverlauf__BWREL_t_id, haltung.symbolpos__BWREL_t_id, haltung.metaattribute__BWREL_sia405_baseclass_metaattribute, haltung.textpos__BWREL_t_id, haltung.haltungspunkt__BWREL_abwassernetzelementref
        # _rel_ --- haltung.rohrprofilref__REL, haltung.nachhaltungspunktref__REL, haltung.vonhaltungspunktref__REL, haltung.abwasserbauwerkref__REL

        # metaattribute --- metaattribute.t_id, metaattribute.t_ili_tid, metaattribute.t_seq, metaattribute.datenherr, metaattribute.datenlieferant, metaattribute.letzte_aenderung, metaattribute.sia405_baseclass_metaattribute
        # _rel_ --- metaattribute.sia405_baseclass_metaattribute__REL

        reach = QGEP.reach(

            # --- wastewater_networkelement ---
            # identifier=row.REPLACE_ME,  # VARCHAR(20)
            # remark=row.REPLACE_ME,  # VARCHAR(80)
            # last_modification=row.REPLACE_ME,  # TIMESTAMP WITHOUT TIME ZONE
            # fk_dataowner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_provider=row.REPLACE_ME,  # VARCHAR(16)
            # fk_wastewater_structure=row.REPLACE_ME,  # VARCHAR(16)

            # --- reach ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)
            # clear_height=row.REPLACE_ME,  # INTEGER
            # coefficient_of_friction=row.REPLACE_ME,  # SMALLINT
            # elevation_determination=row.REPLACE_ME,  # INTEGER
            # horizontal_positioning=row.REPLACE_ME,  # INTEGER
            # inside_coating=row.REPLACE_ME,  # INTEGER
            # length_effective=row.REPLACE_ME,  # NUMERIC(7, 2)
            # material=row.REPLACE_ME,  # INTEGER
            # progression_geometry=row.REPLACE_ME,  # geometry(COMPOUNDCURVEZ,2056)
            # reliner_material=row.REPLACE_ME,  # INTEGER
            # reliner_nominal_size=row.REPLACE_ME,  # INTEGER
            # relining_construction=row.REPLACE_ME,  # INTEGER
            # relining_kind=row.REPLACE_ME,  # INTEGER
            # ring_stiffness=row.REPLACE_ME,  # SMALLINT
            # slope_building_plan=row.REPLACE_ME,  # SMALLINT
            # wall_roughness=row.REPLACE_ME,  # NUMERIC(5, 2)
            # fk_reach_point_from=row.REPLACE_ME,  # VARCHAR(16)
            # fk_reach_point_to=row.REPLACE_ME,  # VARCHAR(16)
            # fk_pipe_profile=row.REPLACE_ME,  # VARCHAR(16)
        )
        qgep_session.add(reach)
        print(".", end="")
    print("done")

    print("Importing ABWASSER.trockenwetterfallrohr, ABWASSER.metaattribute -> QGEP.dryweather_downspout")
    for row, metaattribute in abwasser_session.query(ABWASSER.trockenwetterfallrohr, ABWASSER.metaattribute).join(ABWASSER.metaattribute):

        # baseclass --- trockenwetterfallrohr.t_type, trockenwetterfallrohr.t_ili_tid
        # sia405_baseclass --- trockenwetterfallrohr.obj_id
        # bauwerksteil --- trockenwetterfallrohr.bemerkung, trockenwetterfallrohr.bezeichnung, trockenwetterfallrohr.instandstellung, trockenwetterfallrohr.abwasserbauwerkref
        # trockenwetterfallrohr --- trockenwetterfallrohr.t_id, trockenwetterfallrohr.durchmesser
        # _bwrel_ --- trockenwetterfallrohr.haltung_alternativverlauf__BWREL_t_id, trockenwetterfallrohr.symbolpos__BWREL_t_id, trockenwetterfallrohr.metaattribute__BWREL_sia405_baseclass_metaattribute, trockenwetterfallrohr.textpos__BWREL_t_id
        # _rel_ --- trockenwetterfallrohr.abwasserbauwerkref__REL

        # metaattribute --- metaattribute.t_id, metaattribute.t_ili_tid, metaattribute.t_seq, metaattribute.datenherr, metaattribute.datenlieferant, metaattribute.letzte_aenderung, metaattribute.sia405_baseclass_metaattribute
        # _rel_ --- metaattribute.sia405_baseclass_metaattribute__REL

        dryweather_downspout = QGEP.dryweather_downspout(

            # --- structure_part ---
            # identifier=row.REPLACE_ME,  # VARCHAR(20)
            # remark=row.REPLACE_ME,  # VARCHAR(80)
            # renovation_demand=row.REPLACE_ME,  # INTEGER
            # last_modification=row.REPLACE_ME,  # TIMESTAMP WITHOUT TIME ZONE
            # fk_dataowner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_provider=row.REPLACE_ME,  # VARCHAR(16)
            # fk_wastewater_structure=row.REPLACE_ME,  # VARCHAR(16)

            # --- dryweather_downspout ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)
            # diameter=row.REPLACE_ME,  # SMALLINT
        )
        qgep_session.add(dryweather_downspout)
        print(".", end="")
    print("done")

    print("Importing ABWASSER.einstiegshilfe, ABWASSER.metaattribute -> QGEP.access_aid")
    for row, metaattribute in abwasser_session.query(ABWASSER.einstiegshilfe, ABWASSER.metaattribute).join(ABWASSER.metaattribute):

        # baseclass --- einstiegshilfe.t_type, einstiegshilfe.t_ili_tid
        # sia405_baseclass --- einstiegshilfe.obj_id
        # bauwerksteil --- einstiegshilfe.bemerkung, einstiegshilfe.bezeichnung, einstiegshilfe.instandstellung, einstiegshilfe.abwasserbauwerkref
        # einstiegshilfe --- einstiegshilfe.t_id, einstiegshilfe.art
        # _bwrel_ --- einstiegshilfe.haltung_alternativverlauf__BWREL_t_id, einstiegshilfe.symbolpos__BWREL_t_id, einstiegshilfe.metaattribute__BWREL_sia405_baseclass_metaattribute, einstiegshilfe.textpos__BWREL_t_id
        # _rel_ --- einstiegshilfe.abwasserbauwerkref__REL

        # metaattribute --- metaattribute.t_id, metaattribute.t_ili_tid, metaattribute.t_seq, metaattribute.datenherr, metaattribute.datenlieferant, metaattribute.letzte_aenderung, metaattribute.sia405_baseclass_metaattribute
        # _rel_ --- metaattribute.sia405_baseclass_metaattribute__REL

        access_aid = QGEP.access_aid(

            # --- structure_part ---
            # identifier=row.REPLACE_ME,  # VARCHAR(20)
            # remark=row.REPLACE_ME,  # VARCHAR(80)
            # renovation_demand=row.REPLACE_ME,  # INTEGER
            # last_modification=row.REPLACE_ME,  # TIMESTAMP WITHOUT TIME ZONE
            # fk_dataowner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_provider=row.REPLACE_ME,  # VARCHAR(16)
            # fk_wastewater_structure=row.REPLACE_ME,  # VARCHAR(16)

            # --- access_aid ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)
            # kind=row.REPLACE_ME,  # INTEGER
        )
        qgep_session.add(access_aid)
        print(".", end="")
    print("done")

    print("Importing ABWASSER.trockenwetterrinne, ABWASSER.metaattribute -> QGEP.dryweather_flume")
    for row, metaattribute in abwasser_session.query(ABWASSER.trockenwetterrinne, ABWASSER.metaattribute).join(ABWASSER.metaattribute):

        # baseclass --- trockenwetterrinne.t_type, trockenwetterrinne.t_ili_tid
        # sia405_baseclass --- trockenwetterrinne.obj_id
        # bauwerksteil --- trockenwetterrinne.bemerkung, trockenwetterrinne.bezeichnung, trockenwetterrinne.instandstellung, trockenwetterrinne.abwasserbauwerkref
        # trockenwetterrinne --- trockenwetterrinne.t_id, trockenwetterrinne.material
        # _bwrel_ --- trockenwetterrinne.haltung_alternativverlauf__BWREL_t_id, trockenwetterrinne.symbolpos__BWREL_t_id, trockenwetterrinne.metaattribute__BWREL_sia405_baseclass_metaattribute, trockenwetterrinne.textpos__BWREL_t_id
        # _rel_ --- trockenwetterrinne.abwasserbauwerkref__REL

        # metaattribute --- metaattribute.t_id, metaattribute.t_ili_tid, metaattribute.t_seq, metaattribute.datenherr, metaattribute.datenlieferant, metaattribute.letzte_aenderung, metaattribute.sia405_baseclass_metaattribute
        # _rel_ --- metaattribute.sia405_baseclass_metaattribute__REL

        dryweather_flume = QGEP.dryweather_flume(

            # --- structure_part ---
            # identifier=row.REPLACE_ME,  # VARCHAR(20)
            # remark=row.REPLACE_ME,  # VARCHAR(80)
            # renovation_demand=row.REPLACE_ME,  # INTEGER
            # last_modification=row.REPLACE_ME,  # TIMESTAMP WITHOUT TIME ZONE
            # fk_dataowner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_provider=row.REPLACE_ME,  # VARCHAR(16)
            # fk_wastewater_structure=row.REPLACE_ME,  # VARCHAR(16)

            # --- dryweather_flume ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)
            # material=row.REPLACE_ME,  # INTEGER
        )
        qgep_session.add(dryweather_flume)
        print(".", end="")
    print("done")

    print("Importing ABWASSER.deckel, ABWASSER.metaattribute -> QGEP.cover")
    for row, metaattribute in abwasser_session.query(ABWASSER.deckel, ABWASSER.metaattribute).join(ABWASSER.metaattribute):

        # baseclass --- deckel.t_type, deckel.t_ili_tid
        # sia405_baseclass --- deckel.obj_id
        # bauwerksteil --- deckel.bemerkung, deckel.bezeichnung, deckel.instandstellung, deckel.abwasserbauwerkref
        # deckel --- deckel.t_id, deckel.deckelform, deckel.durchmesser, deckel.entlueftung, deckel.fabrikat, deckel.kote, deckel.lage, deckel.lagegenauigkeit, deckel.material, deckel.schlammeimer, deckel.verschluss
        # _bwrel_ --- deckel.haltung_alternativverlauf__BWREL_t_id, deckel.symbolpos__BWREL_t_id, deckel.metaattribute__BWREL_sia405_baseclass_metaattribute, deckel.textpos__BWREL_t_id
        # _rel_ --- deckel.abwasserbauwerkref__REL

        # metaattribute --- metaattribute.t_id, metaattribute.t_ili_tid, metaattribute.t_seq, metaattribute.datenherr, metaattribute.datenlieferant, metaattribute.letzte_aenderung, metaattribute.sia405_baseclass_metaattribute
        # _rel_ --- metaattribute.sia405_baseclass_metaattribute__REL

        cover = QGEP.cover(

            # --- structure_part ---
            # identifier=row.REPLACE_ME,  # VARCHAR(20)
            # remark=row.REPLACE_ME,  # VARCHAR(80)
            # renovation_demand=row.REPLACE_ME,  # INTEGER
            # last_modification=row.REPLACE_ME,  # TIMESTAMP WITHOUT TIME ZONE
            # fk_dataowner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_provider=row.REPLACE_ME,  # VARCHAR(16)
            # fk_wastewater_structure=row.REPLACE_ME,  # VARCHAR(16)

            # --- cover ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)
            # brand=row.REPLACE_ME,  # VARCHAR(50)
            # cover_shape=row.REPLACE_ME,  # INTEGER
            # diameter=row.REPLACE_ME,  # SMALLINT
            # fastening=row.REPLACE_ME,  # INTEGER
            # level=row.REPLACE_ME,  # NUMERIC(7, 3)
            # material=row.REPLACE_ME,  # INTEGER
            # positional_accuracy=row.REPLACE_ME,  # INTEGER
            # situation_geometry=row.REPLACE_ME,  # geometry(POINTZ,2056)
            # sludge_bucket=row.REPLACE_ME,  # INTEGER
            # venting=row.REPLACE_ME,  # INTEGER
        )
        qgep_session.add(cover)
        print(".", end="")
    print("done")

    print("Importing ABWASSER.bankett, ABWASSER.metaattribute -> QGEP.benching")
    for row, metaattribute in abwasser_session.query(ABWASSER.bankett, ABWASSER.metaattribute).join(ABWASSER.metaattribute):

        # baseclass --- bankett.t_type, bankett.t_ili_tid
        # sia405_baseclass --- bankett.obj_id
        # bauwerksteil --- bankett.bemerkung, bankett.bezeichnung, bankett.instandstellung, bankett.abwasserbauwerkref
        # bankett --- bankett.t_id, bankett.art
        # _bwrel_ --- bankett.haltung_alternativverlauf__BWREL_t_id, bankett.symbolpos__BWREL_t_id, bankett.metaattribute__BWREL_sia405_baseclass_metaattribute, bankett.textpos__BWREL_t_id
        # _rel_ --- bankett.abwasserbauwerkref__REL

        # metaattribute --- metaattribute.t_id, metaattribute.t_ili_tid, metaattribute.t_seq, metaattribute.datenherr, metaattribute.datenlieferant, metaattribute.letzte_aenderung, metaattribute.sia405_baseclass_metaattribute
        # _rel_ --- metaattribute.sia405_baseclass_metaattribute__REL

        benching = QGEP.benching(

            # --- structure_part ---
            # identifier=row.REPLACE_ME,  # VARCHAR(20)
            # remark=row.REPLACE_ME,  # VARCHAR(80)
            # renovation_demand=row.REPLACE_ME,  # INTEGER
            # last_modification=row.REPLACE_ME,  # TIMESTAMP WITHOUT TIME ZONE
            # fk_dataowner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_provider=row.REPLACE_ME,  # VARCHAR(16)
            # fk_wastewater_structure=row.REPLACE_ME,  # VARCHAR(16)

            # --- benching ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)
            # kind=row.REPLACE_ME,  # INTEGER
        )
        qgep_session.add(benching)
        print(".", end="")
    print("done")

    print("Importing ABWASSER.untersuchung, ABWASSER.metaattribute -> QGEP.examination")
    for row, metaattribute in abwasser_session.query(ABWASSER.untersuchung, ABWASSER.metaattribute).join(ABWASSER.metaattribute):

        # baseclass --- untersuchung.t_type, untersuchung.t_ili_tid
        # sia405_baseclass --- untersuchung.obj_id
        # erhaltungsereignis --- untersuchung.art, untersuchung.ausfuehrender, untersuchung.bemerkung, untersuchung.bezeichnung, untersuchung.datengrundlage, untersuchung.dauer, untersuchung.detaildaten, untersuchung.ergebnis, untersuchung.grund, untersuchung.kosten, untersuchung.astatus, untersuchung.zeitpunkt, untersuchung.abwasserbauwerkref, untersuchung.ausfuehrende_firmaref
        # untersuchung --- untersuchung.t_id, untersuchung.bispunktbezeichnung, untersuchung.erfassungsart, untersuchung.fahrzeug, untersuchung.geraet, untersuchung.inspizierte_laenge, untersuchung.videonummer, untersuchung.vonpunktbezeichnung, untersuchung.witterung, untersuchung.haltungspunktref
        # _bwrel_ --- untersuchung.haltung_alternativverlauf__BWREL_t_id, untersuchung.symbolpos__BWREL_t_id, untersuchung.metaattribute__BWREL_sia405_baseclass_metaattribute, untersuchung.textpos__BWREL_t_id, untersuchung.schaden__BWREL_untersuchungref
        # _rel_ --- untersuchung.haltungspunktref__REL, untersuchung.ausfuehrende_firmaref__REL, untersuchung.abwasserbauwerkref__REL

        # metaattribute --- metaattribute.t_id, metaattribute.t_ili_tid, metaattribute.t_seq, metaattribute.datenherr, metaattribute.datenlieferant, metaattribute.letzte_aenderung, metaattribute.sia405_baseclass_metaattribute
        # _rel_ --- metaattribute.sia405_baseclass_metaattribute__REL

        examination = QGEP.examination(

            # --- maintenance_event ---
            # base_data=row.REPLACE_ME,  # TEXT
            # cost=row.REPLACE_ME,  # NUMERIC(10, 2)
            # data_details=row.REPLACE_ME,  # VARCHAR(50)
            # duration=row.REPLACE_ME,  # SMALLINT
            # identifier=row.REPLACE_ME,  # VARCHAR(20)
            # kind=row.REPLACE_ME,  # INTEGER
            # operator=row.REPLACE_ME,  # VARCHAR(50)
            # reason=row.REPLACE_ME,  # VARCHAR(50)
            # remark=row.REPLACE_ME,  # VARCHAR(80)
            # result=row.REPLACE_ME,  # VARCHAR(50)
            # status=row.REPLACE_ME,  # INTEGER
            # time_point=row.REPLACE_ME,  # TIMESTAMP WITHOUT TIME ZONE
            # last_modification=row.REPLACE_ME,  # TIMESTAMP WITHOUT TIME ZONE
            # fk_dataowner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_provider=row.REPLACE_ME,  # VARCHAR(16)
            # fk_operating_company=row.REPLACE_ME,  # VARCHAR(16)
            # active_zone=row.REPLACE_ME,  # VARCHAR(1)

            # --- examination ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)
            # equipment=row.REPLACE_ME,  # VARCHAR(50)
            # from_point_identifier=row.REPLACE_ME,  # VARCHAR(41)
            # inspected_length=row.REPLACE_ME,  # NUMERIC(7, 2)
            # recording_type=row.REPLACE_ME,  # INTEGER
            # to_point_identifier=row.REPLACE_ME,  # VARCHAR(41)
            # vehicle=row.REPLACE_ME,  # VARCHAR(50)
            # videonumber=row.REPLACE_ME,  # VARCHAR(41)
            # weather=row.REPLACE_ME,  # INTEGER
            # fk_reach_point=row.REPLACE_ME,  # VARCHAR(16)
        )
        qgep_session.add(examination)
        print(".", end="")
    print("done")

    print("Importing ABWASSER.normschachtschaden, ABWASSER.metaattribute -> QGEP.damage_manhole")
    for row, metaattribute in abwasser_session.query(ABWASSER.normschachtschaden, ABWASSER.metaattribute).join(ABWASSER.metaattribute):

        # baseclass --- normschachtschaden.t_type, normschachtschaden.t_ili_tid
        # sia405_baseclass --- normschachtschaden.obj_id
        # schaden --- normschachtschaden.anmerkung, normschachtschaden.ansichtsparameter, normschachtschaden.einzelschadenklasse, normschachtschaden.streckenschaden, normschachtschaden.verbindung, normschachtschaden.videozaehlerstand, normschachtschaden.untersuchungref
        # normschachtschaden --- normschachtschaden.t_id, normschachtschaden.distanz, normschachtschaden.quantifizierung1, normschachtschaden.quantifizierung2, normschachtschaden.schachtbereich, normschachtschaden.schachtschadencode, normschachtschaden.schadenlageanfang, normschachtschaden.schadenlageende
        # _bwrel_ --- normschachtschaden.haltung_alternativverlauf__BWREL_t_id, normschachtschaden.symbolpos__BWREL_t_id, normschachtschaden.metaattribute__BWREL_sia405_baseclass_metaattribute, normschachtschaden.textpos__BWREL_t_id
        # _rel_ --- normschachtschaden.untersuchungref__REL

        # metaattribute --- metaattribute.t_id, metaattribute.t_ili_tid, metaattribute.t_seq, metaattribute.datenherr, metaattribute.datenlieferant, metaattribute.letzte_aenderung, metaattribute.sia405_baseclass_metaattribute
        # _rel_ --- metaattribute.sia405_baseclass_metaattribute__REL

        damage_manhole = QGEP.damage_manhole(

            # --- damage ---
            # comments=row.REPLACE_ME,  # VARCHAR(100)
            # connection=row.REPLACE_ME,  # INTEGER
            # damage_begin=row.REPLACE_ME,  # SMALLINT
            # damage_end=row.REPLACE_ME,  # SMALLINT
            # damage_reach=row.REPLACE_ME,  # VARCHAR(3)
            # distance=row.REPLACE_ME,  # NUMERIC(7, 2)
            # quantification1=row.REPLACE_ME,  # INTEGER
            # quantification2=row.REPLACE_ME,  # INTEGER
            # single_damage_class=row.REPLACE_ME,  # INTEGER
            # video_counter=row.REPLACE_ME,  # VARCHAR(11)
            # view_parameters=row.REPLACE_ME,  # VARCHAR(200)
            # last_modification=row.REPLACE_ME,  # TIMESTAMP WITHOUT TIME ZONE
            # fk_dataowner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_provider=row.REPLACE_ME,  # VARCHAR(16)
            # fk_examination=row.REPLACE_ME,  # VARCHAR(16)

            # --- damage_manhole ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)
            # manhole_damage_code=row.REPLACE_ME,  # INTEGER
            # manhole_shaft_area=row.REPLACE_ME,  # INTEGER
        )
        qgep_session.add(damage_manhole)
        print(".", end="")
    print("done")

    print("Importing ABWASSER.kanalschaden, ABWASSER.metaattribute -> QGEP.damage_channel")
    for row, metaattribute in abwasser_session.query(ABWASSER.kanalschaden, ABWASSER.metaattribute).join(ABWASSER.metaattribute):

        # baseclass --- kanalschaden.t_type, kanalschaden.t_ili_tid
        # sia405_baseclass --- kanalschaden.obj_id
        # schaden --- kanalschaden.anmerkung, kanalschaden.ansichtsparameter, kanalschaden.einzelschadenklasse, kanalschaden.streckenschaden, kanalschaden.verbindung, kanalschaden.videozaehlerstand, kanalschaden.untersuchungref
        # kanalschaden --- kanalschaden.t_id, kanalschaden.distanz, kanalschaden.kanalschadencode, kanalschaden.quantifizierung1, kanalschaden.quantifizierung2, kanalschaden.schadenlageanfang, kanalschaden.schadenlageende
        # _bwrel_ --- kanalschaden.haltung_alternativverlauf__BWREL_t_id, kanalschaden.symbolpos__BWREL_t_id, kanalschaden.metaattribute__BWREL_sia405_baseclass_metaattribute, kanalschaden.textpos__BWREL_t_id
        # _rel_ --- kanalschaden.untersuchungref__REL

        # metaattribute --- metaattribute.t_id, metaattribute.t_ili_tid, metaattribute.t_seq, metaattribute.datenherr, metaattribute.datenlieferant, metaattribute.letzte_aenderung, metaattribute.sia405_baseclass_metaattribute
        # _rel_ --- metaattribute.sia405_baseclass_metaattribute__REL

        damage_channel = QGEP.damage_channel(

            # --- damage ---
            # comments=row.REPLACE_ME,  # VARCHAR(100)
            # connection=row.REPLACE_ME,  # INTEGER
            # damage_begin=row.REPLACE_ME,  # SMALLINT
            # damage_end=row.REPLACE_ME,  # SMALLINT
            # damage_reach=row.REPLACE_ME,  # VARCHAR(3)
            # distance=row.REPLACE_ME,  # NUMERIC(7, 2)
            # quantification1=row.REPLACE_ME,  # INTEGER
            # quantification2=row.REPLACE_ME,  # INTEGER
            # single_damage_class=row.REPLACE_ME,  # INTEGER
            # video_counter=row.REPLACE_ME,  # VARCHAR(11)
            # view_parameters=row.REPLACE_ME,  # VARCHAR(200)
            # last_modification=row.REPLACE_ME,  # TIMESTAMP WITHOUT TIME ZONE
            # fk_dataowner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_provider=row.REPLACE_ME,  # VARCHAR(16)
            # fk_examination=row.REPLACE_ME,  # VARCHAR(16)

            # --- damage_channel ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)
            # channel_damage_code=row.REPLACE_ME,  # INTEGER
        )
        qgep_session.add(damage_channel)
        print(".", end="")
    print("done")

    print("Importing ABWASSER.datentraeger, ABWASSER.metaattribute -> QGEP.data_media")
    for row, metaattribute in abwasser_session.query(ABWASSER.datentraeger, ABWASSER.metaattribute).join(ABWASSER.metaattribute):

        # baseclass --- datentraeger.t_type, datentraeger.t_ili_tid
        # sia405_baseclass --- datentraeger.obj_id
        # datentraeger --- datentraeger.t_id, datentraeger.art, datentraeger.bemerkung, datentraeger.bezeichnung, datentraeger.pfad, datentraeger.standort
        # _bwrel_ --- datentraeger.haltung_alternativverlauf__BWREL_t_id, datentraeger.symbolpos__BWREL_t_id, datentraeger.metaattribute__BWREL_sia405_baseclass_metaattribute, datentraeger.textpos__BWREL_t_id, datentraeger.datei__BWREL_datentraegerref

        # metaattribute --- metaattribute.t_id, metaattribute.t_ili_tid, metaattribute.t_seq, metaattribute.datenherr, metaattribute.datenlieferant, metaattribute.letzte_aenderung, metaattribute.sia405_baseclass_metaattribute
        # _rel_ --- metaattribute.sia405_baseclass_metaattribute__REL

        data_media = QGEP.data_media(

            # --- data_media ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)
            # identifier=row.REPLACE_ME,  # VARCHAR(40)
            # kind=row.REPLACE_ME,  # INTEGER
            # location=row.REPLACE_ME,  # VARCHAR(50)
            # path=row.REPLACE_ME,  # VARCHAR(100)
            # remark=row.REPLACE_ME,  # VARCHAR(80)
            # last_modification=row.REPLACE_ME,  # TIMESTAMP WITHOUT TIME ZONE
            # fk_dataowner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_provider=row.REPLACE_ME,  # VARCHAR(16)
        )
        qgep_session.add(data_media)
        print(".", end="")
    print("done")

    print("Importing ABWASSER.datei, ABWASSER.metaattribute -> QGEP.file")
    for row, metaattribute in abwasser_session.query(ABWASSER.datei, ABWASSER.metaattribute).join(ABWASSER.metaattribute):

        # baseclass --- datei.t_type, datei.t_ili_tid
        # sia405_baseclass --- datei.obj_id
        # datei --- datei.t_id, datei.art, datei.bemerkung, datei.bezeichnung, datei.klasse, datei.objekt, datei.relativpfad, datei.datentraegerref
        # _bwrel_ --- datei.haltung_alternativverlauf__BWREL_t_id, datei.symbolpos__BWREL_t_id, datei.metaattribute__BWREL_sia405_baseclass_metaattribute, datei.textpos__BWREL_t_id
        # _rel_ --- datei.datentraegerref__REL

        # metaattribute --- metaattribute.t_id, metaattribute.t_ili_tid, metaattribute.t_seq, metaattribute.datenherr, metaattribute.datenlieferant, metaattribute.letzte_aenderung, metaattribute.sia405_baseclass_metaattribute
        # _rel_ --- metaattribute.sia405_baseclass_metaattribute__REL

        file = QGEP.file(

            # --- file ---
            # obj_id=row.REPLACE_ME,  # VARCHAR(16)
            # class=row.REPLACE_ME,  # INTEGER
            # identifier=row.REPLACE_ME,  # VARCHAR(60)
            # kind=row.REPLACE_ME,  # INTEGER
            # object=row.REPLACE_ME,  # VARCHAR(41)
            # path_relative=row.REPLACE_ME,  # VARCHAR(200)
            # remark=row.REPLACE_ME,  # VARCHAR(80)
            # last_modification=row.REPLACE_ME,  # TIMESTAMP WITHOUT TIME ZONE
            # fk_dataowner=row.REPLACE_ME,  # VARCHAR(16)
            # fk_provider=row.REPLACE_ME,  # VARCHAR(16)
            # fk_data_media=row.REPLACE_ME,  # VARCHAR(16)
        )
        qgep_session.add(file)
        print(".", end="")
    print("done")

    qgep_session.commit()

    qgep_session.close()
    abwasser_session.close()
