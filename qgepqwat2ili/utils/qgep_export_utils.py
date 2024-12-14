import json

import psycopg2
from geoalchemy2.functions import ST_Force2D, ST_GeomFromGeoJSON
from sqlalchemy import or_

from .various import get_pgconf_as_psycopg2_dsn, logger


class QgepExportUtils:

    def __init__(
        self,
        tid_maker,
        current_basket,
        abwasser_session,
        abwasser_model,
        qgep_session,
        qgep_model,
        labelorientation,
        filtered,
        subset_ids,
        subset_wws_ids,
    ):
        self.tid_maker = tid_maker
        self.current_basket = current_basket
        self.abwasser_session = abwasser_session
        self.abwasser_model = abwasser_model
        self.qgep_session = qgep_session
        self.qgep_model = qgep_model
        self.labelorientation = labelorientation
        self.filtered = filtered
        self.subset_ids = subset_ids
        self.subset_wws_ids = subset_wws_ids

    def get_tid(self, relation):
        """
        Makes a tid for a relation
        """
        if relation is None:
            return None

        return self.tid_maker.tid_for_row(relation)

    def get_vl(self, relation):
        """
        Gets a literal value from a value list relation
        """
        if relation is None:
            return None
        return relation.value_de

    def null_to_emptystr(self, val):
        """
        Converts nulls to blank strings and raises a warning
        """
        if val is None:
            logger.warning(
                "A mandatory value was null. It will be cast to a blank string, and probably cause validation errors",
            )
            val = ""
        return val

    def emptystr_to_null(self, val):
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

    def truncate(self, val, max_length):
        """
        Raises a warning if values gets truncated
        """
        if val is None:
            return None
        if len(val) > max_length:
            logger.warning(f"Value '{val}' exceeds expected length ({max_length})")
        return val[0:max_length]

    def modulo_angle(self, val):
        """
        Returns an angle between 0 and 359.9 (for Orientierung in Base_d-20181005.ili)
        """
        if val is None:
            return None

        # add orientation
        val = val + float(self.labelorientation)

        val = val % 360.0
        if val > 359.9:
            val = 0

        logger.info(f"modulo_angle - added orientation: {self.labelorientation}")

        return val

    def check_fk_in_subsetid(self, subset, relation):
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

        # if no selection subset will be None
        if subset is None:
            return self.tid_maker.tid_for_row(relation)
        else:
            if fremdschluesselstr in subset:
                logger.debug(f"check_fk_in_subsetid - '{fremdschluesselstr}' is in subset ")
                logger.debug(
                    f"check_fk_in_subsetid - tid = '{self.tid_maker.tid_for_row(relation)}' "
                )
                return self.tid_maker.tid_for_row(relation)
            else:
                # take out - as it has to work also without filtered (SIA405 Abwasser)
                # if self.filtered:
                logger.warning(
                    f"check_fk_in_subsetid - '{fremdschluesselstr}' is not in subset - replaced with None instead!"
                )
                return None
                # else:
                #    return self.tid_maker.tid_for_row(relation)

    def create_metaattributes(self, row):
        metaattribute = self.abwasser_model.metaattribute(
            # FIELDS TO MAP TO ABWASSER.metaattribute
            # --- metaattribute ---
            # 31.3.2023 identifier instead of name
            # datenherr=getattr(row.fk_dataowner__REL, "name", "unknown"),  # TODO : is unknown ok ?
            # datenlieferant=getattr(row.fk_provider__REL, "name", "unknown"),  # TODO : is unknown ok ?
            # datenherr=getattr(row.fk_dataowner__REL, "identifier", "unknown"),  # TODO : is unknown ok ?
            # datenlieferant=getattr(row.fk_provider__REL, "identifier", "unknown"),  # TODO : is unknown ok ?
            # 31.3.2023 obj_id instead of name
            datenherr=getattr(
                row.fk_dataowner__REL, "obj_id", "unknown"
            ),  # TODO : is unknown ok ?
            datenlieferant=getattr(
                row.fk_provider__REL, "obj_id", "unknown"
            ),  # TODO : is unknown ok ?
            letzte_aenderung=row.last_modification,
            sia405_baseclass_metaattribute=self.get_tid(row),
            # OD : is this OK ? Don't we need a different t_id from what inserted above in organisation ? if so, consider adding a "for_class" arg to tid_for_row
            t_id=self.get_tid(row),
            t_seq=0,
        )

        if self.current_basket is not None:
            metaattribute["t_basket"] = self.current_basket.t_id

        self.abwasser_session.add(metaattribute)

    def base_common(self, row, type_name):
        """
        Returns common attributes for base
        """

        base = {
            "t_ili_tid": row.obj_id,
            "t_type": type_name,
            "obj_id": row.obj_id,
            "t_id": self.get_tid(row),
        }

        if self.current_basket is not None:
            base["t_basket"] = self.current_basket.t_id

        return base

    def wastewater_structure_common(self, row):
        """
        Returns common attributes for wastewater_structure
        """
        # logger.warning(
        # "Mapping of wastewater_structure->abwasserbauwerk is not yet implemented for 3D extensions of SIA405 Abwasser, VSA-KEK and VSA-DSS 2015."
        # )
        return {
            # --- abwasserbauwerk ---
            "akten": row.records,
            "astatus": self.get_vl(row.status__REL),
            "baujahr": row.year_of_construction,
            "baulicherzustand": self.get_vl(row.structure_condition__REL),
            "baulos": row.contract_section,
            "bemerkung": self.truncate(self.emptystr_to_null(row.remark), 80),
            "betreiberref": self.get_tid(row.fk_operator__REL),
            "bezeichnung": self.null_to_emptystr(row.identifier),
            "bruttokosten": row.gross_costs,
            "detailgeometrie": ST_Force2D(row.detail_geometry_geometry),
            "eigentuemerref": self.get_tid(row.fk_owner__REL),
            "ersatzjahr": row.year_of_replacement,
            "finanzierung": self.get_vl(row.financing__REL),
            "inspektionsintervall": row.inspection_interval,
            "sanierungsbedarf": self.get_vl(row.renovation_necessity__REL),
            "standortname": row.location_name,
            "subventionen": row.subsidies,
            "wbw_basisjahr": row.rv_base_year,
            "wbw_bauart": self.get_vl(row.rv_construction_type__REL),
            "wiederbeschaffungswert": row.replacement_value,
            "zugaenglichkeit": self.get_vl(row.accessibility__REL),
        }

    def wastewater_networkelement_common(self, row):
        """
        Returns common attributes for wastewater_networkelement
        """
        return {
            # added check_fk_in_subsetid with subset_wws_ids (only needed for SIA405 Abwasser export wwtp_structure - but as now in qgep_export_utils done for all export - might slow donw export
            # "abwasserbauwerkref": self.get_tid(row.fk_wastewater_structure__REL),
            "abwasserbauwerkref": self.check_fk_in_subsetid(
                self.subset_wws_ids, row.fk_wastewater_structure__REL
            ),
            "bemerkung": self.truncate(self.emptystr_to_null(row.remark), 80),
            "bezeichnung": self.null_to_emptystr(row.identifier),
        }

    def structure_part_common(self, row):
        """
        Returns common attributes for structure_part
        """
        return {
            # "abwasserbauwerkref": self.get_tid(row.fk_wastewater_structure__REL),
            "abwasserbauwerkref": self.check_fk_in_subsetid(
                self.subset_wws_ids, row.fk_wastewater_structure__REL
            ),
            "bemerkung": self.truncate(self.emptystr_to_null(row.remark), 80),
            "bezeichnung": self.null_to_emptystr(row.identifier),
            "instandstellung": self.get_vl(row.renovation_demand__REL),
        }

    def textpos_common(self, row, t_type, geojson_crs_def):
        """
        Returns common attributes for textpos
        """
        t_id = self.tid_maker.next_tid()
        textpos = {
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
            "textori": self.modulo_angle(row["properties"]["LabelRotation"]),
            "texthali": "Left",  # can be Left/Center/Right
            "textvali": "Bottom",  # can be Top,Cap,Half,Base,Bottom
            # --- SIA405_TextPos ---
            "plantyp": row["properties"]["scale"],
            "textinhalt": row["properties"]["LabelText"],
            "bemerkung": None,
        }

        if self.current_basket is not None:
            textpos["t_basket"] = self.current_basket.t_id

        return textpos

    def export_organisation(self):
        query = self.qgep_session.query(self.qgep_model.organisation)
        for row in query:

            # AVAILABLE FIELDS IN QGEP.organisation

            # --- organisation ---
            # fk_dataowner, fk_provider, identifier, last_modification, obj_id, remark, uid

            # --- _bwrel_ ---
            # accident__BWREL_fk_dataowner, accident__BWREL_fk_provider, administrative_office__BWREL_obj_id, aquifier__BWREL_fk_dataowner, aquifier__BWREL_fk_provider, bathing_area__BWREL_fk_dataowner, bathing_area__BWREL_fk_provider, canton__BWREL_obj_id, catchment_area__BWREL_fk_dataowner, catchment_area__BWREL_fk_provider, connection_object__BWREL_fk_dataowner, connection_object__BWREL_fk_operator, connection_object__BWREL_fk_owner, connection_object__BWREL_fk_provider, control_center__BWREL_fk_dataowner, control_center__BWREL_fk_provider, cooperative__BWREL_obj_id, damage__BWREL_fk_dataowner, damage__BWREL_fk_provider, data_media__BWREL_fk_dataowner, data_media__BWREL_fk_provider, file__BWREL_fk_dataowner, file__BWREL_fk_provider, fish_pass__BWREL_fk_dataowner, fish_pass__BWREL_fk_provider, hazard_source__BWREL_fk_dataowner, hazard_source__BWREL_fk_owner, hazard_source__BWREL_fk_provider, hq_relation__BWREL_fk_dataowner, hq_relation__BWREL_fk_provider, hydr_geom_relation__BWREL_fk_dataowner, hydr_geom_relation__BWREL_fk_provider, hydr_geometry__BWREL_fk_dataowner, hydr_geometry__BWREL_fk_provider, hydraulic_char_data__BWREL_fk_dataowner, hydraulic_char_data__BWREL_fk_provider, maintenance_event__BWREL_fk_dataowner, maintenance_event__BWREL_fk_operating_company, maintenance_event__BWREL_fk_provider, measurement_result__BWREL_fk_dataowner, measurement_result__BWREL_fk_provider, measurement_series__BWREL_fk_dataowner, measurement_series__BWREL_fk_provider, measuring_device__BWREL_fk_dataowner, measuring_device__BWREL_fk_provider, measuring_point__BWREL_fk_dataowner, measuring_point__BWREL_fk_operator, measuring_point__BWREL_fk_provider, mechanical_pretreatment__BWREL_fk_dataowner, mechanical_pretreatment__BWREL_fk_provider, municipality__BWREL_obj_id, mutation__BWREL_fk_dataowner, mutation__BWREL_fk_provider, organisation__BWREL_fk_dataowner, organisation__BWREL_fk_provider, overflow__BWREL_fk_dataowner, overflow__BWREL_fk_provider, overflow_char__BWREL_fk_dataowner, overflow_char__BWREL_fk_provider, pipe_profile__BWREL_fk_dataowner, pipe_profile__BWREL_fk_provider, private__BWREL_obj_id, profile_geometry__BWREL_fk_dataowner, profile_geometry__BWREL_fk_provider, reach_point__BWREL_fk_dataowner, reach_point__BWREL_fk_provider, retention_body__BWREL_fk_dataowner, retention_body__BWREL_fk_provider, river_bank__BWREL_fk_dataowner, river_bank__BWREL_fk_provider, river_bed__BWREL_fk_dataowner, river_bed__BWREL_fk_provider, sector_water_body__BWREL_fk_dataowner, sector_water_body__BWREL_fk_provider, sludge_treatment__BWREL_fk_dataowner, sludge_treatment__BWREL_fk_provider, structure_part__BWREL_fk_dataowner, structure_part__BWREL_fk_provider, substance__BWREL_fk_dataowner, substance__BWREL_fk_provider, surface_runoff_parameters__BWREL_fk_dataowner, surface_runoff_parameters__BWREL_fk_provider, surface_water_bodies__BWREL_fk_dataowner, surface_water_bodies__BWREL_fk_provider, throttle_shut_off_unit__BWREL_fk_dataowner, throttle_shut_off_unit__BWREL_fk_provider, txt_symbol__BWREL_fk_dataowner, txt_symbol__BWREL_fk_provider, waste_water_association__BWREL_obj_id, waste_water_treatment__BWREL_fk_dataowner, waste_water_treatment__BWREL_fk_provider, waste_water_treatment_plant__BWREL_obj_id, wastewater_networkelement__BWREL_fk_dataowner, wastewater_networkelement__BWREL_fk_provider, wastewater_structure__BWREL_fk_dataowner, wastewater_structure__BWREL_fk_operator, wastewater_structure__BWREL_fk_owner, wastewater_structure__BWREL_fk_provider, wastewater_structure_symbol__BWREL_fk_dataowner, wastewater_structure_symbol__BWREL_fk_provider, water_catchment__BWREL_fk_dataowner, water_catchment__BWREL_fk_provider, water_control_structure__BWREL_fk_dataowner, water_control_structure__BWREL_fk_provider, water_course_segment__BWREL_fk_dataowner, water_course_segment__BWREL_fk_provider, wwtp_energy_use__BWREL_fk_dataowner, wwtp_energy_use__BWREL_fk_provider, zone__BWREL_fk_dataowner, zone__BWREL_fk_provider

            # --- _rel_ ---
            # fk_dataowner__REL, fk_provider__REL

            organisation = self.abwasser_model.organisation(
                # FIELDS TO MAP TO ABWASSER.organisation
                # --- baseclass ---
                # --- sia405_baseclass ---
                **self.base_common(row, "organisation"),
                # --- organisation ---
                auid=row.uid,
                bemerkung=self.truncate(self.emptystr_to_null(row.remark), 80),
                bezeichnung=self.null_to_emptystr(row.identifier),
            )
            self.abwasser_session.add(organisation)
            self.create_metaattributes(row)
            print(".", end="")
        logger.info("done")
        self.abwasser_session.flush()

    def export_channel(self):
        query = self.qgep_session.query(self.qgep_model.channel)
        if self.filtered:
            query = query.join(self.qgep_model.wastewater_networkelement).filter(
                self.qgep_model.wastewater_networkelement.obj_id.in_(self.subset_ids)
            )
        for row in query:
            # AVAILABLE FIELDS IN QGEP.channel

            # --- wastewater_structure ---
            # _bottom_label, _cover_label, _depth, _function_hierarchic, _input_label, _label, _output_label, _usage_current, accessibility, contract_section, detail_geometry_geometry, financing, fk_dataowner, fk_main_cover, fk_main_wastewater_node, fk_operator, fk_owner, fk_provider, gross_costs, identifier, inspection_interval, last_modification, location_name, records, remark, renovation_necessity, replacement_value, rv_base_year, rv_construction_type, status, structure_condition, subsidies, year_of_construction, year_of_replacement

            # --- _bwrel_ ---
            # measuring_point__BWREL_fk_wastewater_structure, mechanical_pretreatment__BWREL_fk_wastewater_structure, re_maintenance_event_wastewater_structure__BWREL_fk_wastewater_structure, structure_part__BWREL_fk_wastewater_structure, txt_symbol__BWREL_fk_wastewater_structure, txt_text__BWREL_fk_wastewater_structure, wastewater_networkelement__BWREL_fk_wastewater_structure, wastewater_structure_symbol__BWREL_fk_wastewater_structure, wastewater_structure_text__BWREL_fk_wastewater_structure, wwtp_structure_kind__BWREL_obj_id

            # --- _rel_ ---
            # accessibility__REL, bedding_encasement__REL, connection_type__REL, financing__REL, fk_dataowner__REL, fk_main_cover__REL, fk_main_wastewater_node__REL, fk_operator__REL, fk_owner__REL, fk_provider__REL, function_hierarchic__REL, function_hydraulic__REL, renovation_necessity__REL, rv_construction_type__REL, status__REL, structure_condition__REL, usage_current__REL, usage_planned__REL

            kanal = self.abwasser_model.kanal(
                # FIELDS TO MAP TO ABWASSER.kanal
                # --- baseclass ---
                # --- sia405_baseclass ---
                **self.base_common(row, "kanal"),
                # --- abwasserbauwerk ---
                **self.wastewater_structure_common(row),
                # --- kanal ---
                bettung_umhuellung=self.get_vl(row.bedding_encasement__REL),
                funktionhierarchisch=self.get_vl(row.function_hierarchic__REL),
                funktionhydraulisch=self.get_vl(row.function_hydraulic__REL),
                nutzungsart_geplant=self.get_vl(row.usage_planned__REL),
                nutzungsart_ist=self.get_vl(row.usage_current__REL),
                rohrlaenge=row.pipe_length,
                spuelintervall=row.jetting_interval,
                verbindungsart=self.get_vl(row.connection_type__REL),
            )
            self.abwasser_session.add(kanal)
            self.create_metaattributes(row)
            print(".", end="")
        logger.info("done")
        self.abwasser_session.flush()

    def export_manhole(self):
        query = self.qgep_session.query(self.qgep_model.manhole)
        if self.filtered:
            query = query.join(self.qgep_model.wastewater_networkelement).filter(
                self.qgep_model.wastewater_networkelement.obj_id.in_(self.subset_ids)
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

            normschacht = self.abwasser_model.normschacht(
                # FIELDS TO MAP TO ABWASSER.normschacht
                # --- baseclass ---
                # --- sia405_baseclass ---
                **self.base_common(row, "normschacht"),
                # --- abwasserbauwerk ---
                **self.wastewater_structure_common(row),
                # --- normschacht ---
                dimension1=row.dimension1,
                dimension2=row.dimension2,
                funktion=self.get_vl(row.function__REL),
                # -- attribute 3D ---
                # maechtigkeit=row.depth,
                material=self.get_vl(row.material__REL),
                oberflaechenzulauf=self.get_vl(row.surface_inflow__REL),
            )
            self.abwasser_session.add(normschacht)
            self.create_metaattributes(row)
            print(".", end="")
        logger.info("done")
        self.abwasser_session.flush()

    def export_special_structure(self):
        query = self.qgep_session.query(self.qgep_model.special_structure)
        if self.filtered:
            query = query.join(self.qgep_model.wastewater_networkelement).filter(
                self.qgep_model.wastewater_networkelement.obj_id.in_(self.subset_ids)
            )
        for row in query:
            # AVAILABLE FIELDS IN QGEP.special_structure

            # --- wastewater_structure ---
            # _bottom_label, _cover_label, _depth, _function_hierarchic, _input_label, _label, _output_label, _usage_current, accessibility, contract_section, detail_geometry_geometry, financing, fk_dataowner, fk_main_cover, fk_main_wastewater_node, fk_operator, fk_owner, fk_provider, gross_costs, identifier, inspection_interval, last_modification, location_name, records, remark, renovation_necessity, replacement_value, rv_base_year, rv_construction_type, status, structure_condition, subsidies, year_of_construction, year_of_replacement

            # --- special_structure ---
            # bypass, emergency_spillway, function, obj_id, stormwater_tank_arrangement, upper_elevation

            # --- _bwrel_ ---
            # measuring_point__BWREL_fk_wastewater_structure, mechanical_pretreatment__BWREL_fk_wastewater_structure, re_maintenance_event_wastewater_structure__BWREL_fk_wastewater_structure, structure_part__BWREL_fk_wastewater_structure, txt_symbol__BWREL_fk_wastewater_structure, txt_text__BWREL_fk_wastewater_structure, wastewater_networkelement__BWREL_fk_wastewater_structure, wastewater_structure_symbol__BWREL_fk_wastewater_structure, wastewater_structure_text__BWREL_fk_wastewater_structure, wwtp_structure_kind__BWREL_obj_id

            # --- _rel_ ---
            # accessibility__REL, bypass__REL, emergency_spillway__REL, financing__REL, fk_dataowner__REL, fk_main_cover__REL, fk_main_wastewater_node__REL, fk_operator__REL, fk_owner__REL, fk_provider__REL, function__REL, renovation_necessity__REL, rv_construction_type__REL, status__REL, stormwater_tank_arrangement__REL, structure_condition__REL

            # QGEP field special_structure.upper_elevation is a 3D attribute and has no equivalent in the INTERLIS 2D model release used. It will be ignored for now and not supported with QGEP.

            spezialbauwerk = self.abwasser_model.spezialbauwerk(
                # FIELDS TO MAP TO ABWASSER.spezialbauwerk
                # --- baseclass ---
                # --- sia405_baseclass ---
                **self.base_common(row, "spezialbauwerk"),
                # --- abwasserbauwerk ---
                **self.wastewater_structure_common(row),
                # --- spezialbauwerk ---
                # TODO : WARNING : upper_elevation is not mapped
                bypass=self.get_vl(row.bypass__REL),
                funktion=self.get_vl(row.function__REL),
                notueberlauf=self.get_vl(row.emergency_spillway__REL),
                regenbecken_anordnung=self.get_vl(row.stormwater_tank_arrangement__REL),
            )
            self.abwasser_session.add(spezialbauwerk)
            self.create_metaattributes(row)
            print(".", end="")
        logger.info("done")
        self.abwasser_session.flush()

    def export_pipe_profile(self):
        query = self.qgep_session.query(self.qgep_model.pipe_profile)
        # always export all pipe_profile
        # add sql statement to logger
        statement = query.statement
        logger.info(f" always export all pipe_profile datasets query = {statement}")
        for row in query:

            # AVAILABLE FIELDS IN QGEP.pipe_profile

            # --- pipe_profile ---
            # fk_dataowner, fk_provider, height_width_ratio, identifier, last_modification, obj_id, profile_type, remark

            # --- _bwrel_ ---
            # profile_geometry__BWREL_fk_pipe_profile, reach__BWREL_fk_pipe_profile

            # --- _rel_ ---
            # fk_dataowner__REL, fk_provider__REL, profile_type__REL

            rohrprofil = self.abwasser_model.rohrprofil(
                # FIELDS TO MAP TO ABWASSER.rohrprofil
                # --- baseclass ---
                # --- sia405_baseclass ---
                **self.base_common(row, "rohrprofil"),
                # --- rohrprofil ---
                bemerkung=self.truncate(self.emptystr_to_null(row.remark), 80),
                bezeichnung=self.null_to_emptystr(row.identifier),
                hoehenbreitenverhaeltnis=row.height_width_ratio,
                profiltyp=self.get_vl(row.profile_type__REL),
            )
            self.abwasser_session.add(rohrprofil)
            self.create_metaattributes(row)
            print(".", end="")
        logger.info("done")
        self.abwasser_session.flush()

    def export_reach_point(self):
        query = self.qgep_session.query(self.qgep_model.reach_point)
        if self.filtered:
            query = query.join(
                self.qgep_model.reach,
                or_(
                    self.qgep_model.reach_point.obj_id
                    == self.qgep_model.reach.fk_reach_point_from,
                    self.qgep_model.reach_point.obj_id == self.qgep_model.reach.fk_reach_point_to,
                ),
            ).filter(self.qgep_model.wastewater_networkelement.obj_id.in_(self.subset_ids))
        for row in query:

            # AVAILABLE FIELDS IN QGEP.reach_point

            # --- reach_point ---
            # elevation_accuracy, fk_dataowner, fk_provider, fk_wastewater_networkelement, identifier, last_modification, level, obj_id, outlet_shape, position_of_connection, remark, situation_geometry

            # --- _bwrel_ ---
            # examination__BWREL_fk_reach_point, reach__BWREL_fk_reach_point_from, reach__BWREL_fk_reach_point_to

            # --- _rel_ ---
            # elevation_accuracy__REL, fk_dataowner__REL, fk_provider__REL, fk_wastewater_networkelement__REL, outlet_shape__REL

            haltungspunkt = self.abwasser_model.haltungspunkt(
                # FIELDS TO MAP TO ABWASSER.haltungspunkt
                # --- baseclass ---
                # --- sia405_baseclass ---
                **self.base_common(row, "haltungspunkt"),
                # --- haltungspunkt ---
                # changed call from self.get_tid to self.check_fk_in_subsetid so it does not wirte foreignkeys on elements that do not exist
                # abwassernetzelementref=self.get_tid(row.fk_wastewater_networkelement__REL),
                abwassernetzelementref=self.check_fk_in_subsetid(
                    self.subset_ids, row.fk_wastewater_networkelement__REL
                ),
                auslaufform=self.get_vl(row.outlet_shape__REL),
                bemerkung=self.truncate(self.emptystr_to_null(row.remark), 80),
                bezeichnung=self.null_to_emptystr(row.identifier),
                hoehengenauigkeit=self.get_vl(row.elevation_accuracy__REL),
                kote=row.level,
                lage=ST_Force2D(row.situation_geometry),
                lage_anschluss=row.position_of_connection,
            )
            self.abwasser_session.add(haltungspunkt)
            self.create_metaattributes(row)
            print(".", end="")
        logger.info("done")
        self.abwasser_session.flush()

    def export_reach(self):
        query = self.qgep_session.query(self.qgep_model.reach)
        if self.filtered:
            query = query.filter(
                self.qgep_model.wastewater_networkelement.obj_id.in_(self.subset_ids)
            )
        for row in query:
            # AVAILABLE FIELDS IN QGEP.reach

            # --- wastewater_networkelement ---
            # fk_dataowner, fk_provider, fk_wastewater_structure, identifier, last_modification, remark

            # --- reach ---
            # clear_height, coefficient_of_friction, elevation_determination, fk_pipe_profile, fk_reach_point_from, fk_reach_point_to, horizontal_positioning, inside_coating, length_effective, material, obj_id, progression_geometry, reliner_material, reliner_nominal_size, relining_construction, relining_kind, ring_stiffness, slope_building_plan, wall_roughness

            # --- _bwrel_ ---
            # catchment_area__BWREL_fk_wastewater_networkelement_rw_current, catchment_area__BWREL_fk_wastewater_networkelement_rw_planned, catchment_area__BWREL_fk_wastewater_networkelement_ww_current, catchment_area__BWREL_fk_wastewater_networkelement_ww_planned, connection_object__BWREL_fk_wastewater_networkelement, reach_point__BWREL_fk_wastewater_networkelement, reach_text__BWREL_fk_reach, txt_text__BWREL_fk_reach

            # --- _rel_ ---
            # elevation_determination__REL, fk_dataowner__REL, fk_pipe_profile__REL, fk_provider__REL, fk_reach_point_from__REL, fk_reach_point_to__REL, fk_wastewater_structure__REL, horizontal_positioning__REL, inside_coating__REL, material__REL, reliner_material__REL, relining_construction__REL, relining_kind__REL

            # QGEP field reach.elevation_determination has no equivalent in the interlis model. It will be ignored.

            haltung = self.abwasser_model.haltung(
                # FIELDS TO MAP TO ABWASSER.haltung
                # --- baseclass ---
                # --- sia405_baseclass ---
                **self.base_common(row, "haltung"),
                # --- abwassernetzelement ---
                **self.wastewater_networkelement_common(row),
                # --- haltung ---
                # NOT MAPPED : elevation_determination
                innenschutz=self.get_vl(row.inside_coating__REL),
                laengeeffektiv=row.length_effective,
                lagebestimmung=self.get_vl(row.horizontal_positioning__REL),
                lichte_hoehe=row.clear_height,
                material=self.get_vl(row.material__REL),
                nachhaltungspunktref=self.get_tid(row.fk_reach_point_to__REL),
                plangefaelle=row.slope_building_plan,  # TODO : check, does this need conversion ?
                reibungsbeiwert=row.coefficient_of_friction,
                reliner_art=self.get_vl(row.relining_kind__REL),
                reliner_bautechnik=self.get_vl(row.relining_construction__REL),
                reliner_material=self.get_vl(row.reliner_material__REL),
                reliner_nennweite=row.reliner_nominal_size,
                ringsteifigkeit=row.ring_stiffness,
                rohrprofilref=self.get_tid(row.fk_pipe_profile__REL),
                verlauf=ST_Force2D(row.progression_geometry),
                # -- attribute 3D ---
                # verlauf3d=row.progression3d,
                vonhaltungspunktref=self.get_tid(row.fk_reach_point_from__REL),
                wandrauhigkeit=row.wall_roughness,
            )
            self.abwasser_session.add(haltung)
            self.create_metaattributes(row)
            print(".", end="")
        logger.info("done")
        self.abwasser_session.flush()

    def export_dryweather_downspout(self):
        query = self.qgep_session.query(self.qgep_model.dryweather_downspout)
        if self.filtered:
            query = (
                query.join(self.qgep_model.wastewater_structure)
                .join(self.qgep_model.wastewater_networkelement)
                .filter(self.qgep_model.wastewater_networkelement.obj_id.in_(self.subset_ids))
            )
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

            trockenwetterfallrohr = self.abwasser_model.trockenwetterfallrohr(
                # FIELDS TO MAP TO ABWASSER.trockenwetterfallrohr
                # --- baseclass ---
                # --- sia405_baseclass ---
                **self.base_common(row, "trockenwetterfallrohr"),
                # --- bauwerksteil ---
                **self.structure_part_common(row),
                # --- trockenwetterfallrohr ---
                durchmesser=row.diameter,
            )
            self.abwasser_session.add(trockenwetterfallrohr)
            self.create_metaattributes(row)
            print(".", end="")
        logger.info("done")
        self.abwasser_session.flush()

    def export_access_aid(self):
        query = self.qgep_session.query(self.qgep_model.access_aid)
        if self.filtered:
            query = (
                query.join(self.qgep_model.wastewater_structure)
                .join(self.qgep_model.wastewater_networkelement)
                .filter(self.qgep_model.wastewater_networkelement.obj_id.in_(self.subset_ids))
            )
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

            einstiegshilfe = self.abwasser_model.einstiegshilfe(
                # FIELDS TO MAP TO ABWASSER.einstiegshilfe
                # --- baseclass ---
                # --- sia405_baseclass ---
                **self.base_common(row, "einstiegshilfe"),
                # --- bauwerksteil ---
                **self.structure_part_common(row),
                # --- einstiegshilfe ---
                art=self.get_vl(row.kind__REL),
            )
            self.abwasser_session.add(einstiegshilfe)
            self.create_metaattributes(row)
            print(".", end="")
        logger.info("done")
        self.abwasser_session.flush()

    def export_dryweather_flume(self):
        query = self.qgep_session.query(self.qgep_model.dryweather_flume)
        if self.filtered:
            query = (
                query.join(self.qgep_model.wastewater_structure)
                .join(self.qgep_model.wastewater_networkelement)
                .filter(self.qgep_model.wastewater_networkelement.obj_id.in_(self.subset_ids))
            )
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

            trockenwetterrinne = self.abwasser_model.trockenwetterrinne(
                # FIELDS TO MAP TO ABWASSER.trockenwetterrinne
                # --- baseclass ---
                # --- sia405_baseclass ---
                **self.base_common(row, "trockenwetterrinne"),
                # --- bauwerksteil ---
                **self.structure_part_common(row),
                # --- trockenwetterrinne ---
                material=self.get_vl(row.material__REL),
            )
            self.abwasser_session.add(trockenwetterrinne)
            self.create_metaattributes(row)
            print(".", end="")
        logger.info("done")
        self.abwasser_session.flush()

    def export_cover(self):
        query = self.qgep_session.query(self.qgep_model.cover)
        if self.filtered:
            query = (
                query.join(self.qgep_model.wastewater_structure)
                .join(self.qgep_model.wastewater_networkelement)
                .filter(self.qgep_model.wastewater_networkelement.obj_id.in_(self.subset_ids))
            )
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

            deckel = self.abwasser_model.deckel(
                # FIELDS TO MAP TO ABWASSER.deckel
                # --- baseclass ---
                # --- sia405_baseclass ---
                **self.base_common(row, "deckel"),
                # --- bauwerksteil ---
                **self.structure_part_common(row),
                # --- deckel ---
                deckelform=self.get_vl(row.cover_shape__REL),
                durchmesser=row.diameter,
                entlueftung=self.get_vl(row.venting__REL),
                fabrikat=row.brand,
                kote=row.level,
                lage=ST_Force2D(row.situation_geometry),
                lagegenauigkeit=self.get_vl(row.positional_accuracy__REL),
                material=self.get_vl(row.material__REL),
                schlammeimer=self.get_vl(row.sludge_bucket__REL),
                verschluss=self.get_vl(row.fastening__REL),
            )
            self.abwasser_session.add(deckel)
            self.create_metaattributes(row)
            print(".", end="")
        logger.info("done")
        self.abwasser_session.flush()

    def export_benching(self):
        query = self.qgep_session.query(self.qgep_model.benching)
        if self.filtered:
            query = (
                query.join(self.qgep_model.wastewater_structure)
                .join(self.qgep_model.wastewater_networkelement)
                .filter(self.qgep_model.wastewater_networkelement.obj_id.in_(self.subset_ids))
            )
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

            bankett = self.abwasser_model.bankett(
                # FIELDS TO MAP TO ABWASSER.bankett
                # --- baseclass ---
                # --- sia405_baseclass ---
                **self.base_common(row, "bankett"),
                # --- bauwerksteil ---
                **self.structure_part_common(row),
                # --- bankett ---
                art=self.get_vl(row.kind__REL),
            )
            self.abwasser_session.add(bankett)
            self.create_metaattributes(row)
            print(".", end="")
        logger.info("done")
        self.abwasser_session.flush()


# end class QgepExportUtils


# 10.12.2024
def get_selection_text_for_in_statement(selection_list):
    """
    convert selection_list to selection_text to fit SQL IN statement
    """
    if selection_list is None:
        selection_text = None
    else:
        if not selection_list:
            # list is empty - no need for adaption
            selection_text = None
        else:
            selection_text = ""

            for list_item in selection_list:
                selection_text += "'"
                selection_text += list_item
                selection_text += "',"

            # remove last komma to make it a correct IN statement
            selection_text = selection_text[:-1]

    logger.debug(f"selection_text = {selection_text} ...")
    return selection_text


# 10.12.2024
def get_connected_we_from_re(subset_reaches):
    """
    Get connected wastewater_networkelements (wastewater_nodes and reaches) from subset of reaches
    """
    if subset_reaches is None:
        connected_wn_from_re_ids = None
    else:
        # check if list is empty https://stackoverflow.com/questions/53513/how-do-i-check-if-a-list-is-empty
        if not subset_reaches:
            connected_wn_from_re_ids = None
        else:
            logger.debug(
                f"get list of id's of connected wastewater_nodes of provides subset of reaches {subset_reaches} ..."
            )
            connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
            connection.set_session(autocommit=True)
            cursor = connection.cursor()

            connected_wn_from_re_ids = []

            subset_reaches_text = get_selection_text_for_in_statement(subset_reaches)

            # select all connected from wastewater_nodes from provided subset of reaches
            cursor.execute(
                f"SELECT  wef.obj_id as wef_obj_id FROM qgep_od.reach re LEFT JOIN qgep_od.reach_point rpf ON rpf.obj_id = re.fk_reach_point_from LEFT JOIN qgep_od.wastewater_networkelement wef ON wef.obj_id = rpf.fk_wastewater_networkelement WHERE re.obj_id IN ({subset_reaches_text}) AND NOT wef.obj_id isNull;"
            )

            # cursor.fetchall() - see https://pynative.com/python-cursor-fetchall-fetchmany-fetchone-to-read-rows-from-table/
            # ws_wn_ids_count = int(cursor.fetchone()[0])
            # if ws_wn_ids_count == 0:
            if cursor.fetchone() is None:
                connected_wn_from_re_ids = None
            else:
                # added cursor.execute again to see if with this all records will be available
                # 15.11.2024 added - see https://stackoverflow.com/questions/58101874/cursor-fetchall-or-other-method-fetchone-is-not-working
                cursor.execute(
                    f"SELECT  wef.obj_id as wef_obj_id FROM qgep_od.reach re LEFT JOIN qgep_od.reach_point rpf ON rpf.obj_id = re.fk_reach_point_from LEFT JOIN qgep_od.wastewater_networkelement wef ON wef.obj_id = rpf.fk_wastewater_networkelement WHERE re.obj_id IN ({subset_reaches_text}) AND NOT wef.obj_id isNull;"
                )
                records = cursor.fetchall()

                # 15.11.2024 - does not get all records, but only n-1
                for row in records:
                    # logger.debug(f" row[0] = {row[0]}")
                    # https://www.pythontutorial.net/python-string-methods/python-string-concatenation/
                    strrow = str(row[0])
                    if strrow is not None:
                        connected_wn_from_re_ids.append(strrow)
                        # logger.debug(f" building up '{connected_wn_from_re_ids}' ...")
            logger.debug(f" connected_wn_from_re_ids: '{connected_wn_from_re_ids}'")
    return connected_wn_from_re_ids


# 10.12.2024
def get_connected_we_to_re(subset_reaches):
    """
    Get connected wastewater_networkelements (wastewater_nodes and reaches) to subset of reaches
    """
    if subset_reaches is None:
        return None
    if not subset_reaches:
        return None
    else:
        logger.debug(
            f"get list of id's of connected wastewater_nodes of provides subset of reaches {subset_reaches} ..."
        )
        connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
        connection.set_session(autocommit=True)
        cursor = connection.cursor()

        connected_wn_to_re_ids = []

        subset_reaches_text = get_selection_text_for_in_statement(subset_reaches)

        # select all connected to wastewater_nodes from provided subset of reaches
        cursor.execute(
            f"SELECT  wet.obj_id as wet_obj_id FROM qgep_od.reach re LEFT JOIN qgep_od.reach_point rpt ON rpt.obj_id = re.fk_reach_point_to LEFT JOIN qgep_od.wastewater_networkelement wet ON wet.obj_id = rpt.fk_wastewater_networkelement WHERE re.obj_id IN ({subset_reaches_text}) AND NOT wet.obj_id isNull;"
        )

        # cursor.fetchall() - see https://pynative.com/python-cursor-fetchall-fetchmany-fetchone-to-read-rows-from-table/
        # ws_wn_ids_count = int(cursor.fetchone()[0])
        # if ws_wn_ids_count == 0:
        if cursor.fetchone() is None:
            connected_wn_to_re_ids = None
        else:
            # added cursor.execute again to see if with this all records will be available
            # 15.11.2024 added - see https://stackoverflow.com/questions/58101874/cursor-fetchall-or-other-method-fetchone-is-not-working
            cursor.execute(
                f"SELECT  wet.obj_id as wet_obj_id FROM qgep_od.reach re LEFT JOIN qgep_od.reach_point rpt ON rpt.obj_id = re.fk_reach_point_to LEFT JOIN qgep_od.wastewater_networkelement wet ON wet.obj_id = rpt.fk_wastewater_networkelement WHERE re.obj_id IN ({subset_reaches_text}) AND NOT wet.obj_id isNull;"
            )
            records = cursor.fetchall()

            # 15.11.2024 - does not get all records, but only n-1
            for row in records:
                # logger.debug(f" row[0] = {row[0]}")
                # https://www.pythontutorial.net/python-string-methods/python-string-concatenation/
                strrow = str(row[0])
                if strrow is not None:
                    connected_wn_to_re_ids.append(strrow)
                    # logger.debug(f" building up '{connected_wn_to_re_ids}' ...")
    logger.debug(f" connected_wn_to_re_ids: '{connected_wn_to_re_ids}'")
    return connected_wn_to_re_ids


# 10.12.2024
def get_connected_overflow_to_wn_ids(selected_ids):
    """
    Get all connected wastewater_nodes from overflows.fk_overflow_to
    """
    if selected_ids is None:
        return None
    if not selected_ids:
        return None
    else:
        logger.debug(
            f"Get all connected wastewater_nodes from overflows.fk_overflow_to {selected_ids} ..."
        )
        connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
        connection.set_session(autocommit=True)
        cursor = connection.cursor()

        connected_overflow_to_wn_ids = []

        subset_text = get_selection_text_for_in_statement(selected_ids)

        # select all connected to wastewater_nodes from provided subset of reaches
        cursor.execute(
            f"SELECT ov.fk_overflow_to FROM qgep_od.wastewater_node wn LEFT JOIN qgep_od.overflow ov ON wn.obj_id = ov.fk_wastewater_node WHERE wn.obj_id IN ({subset_text}) AND NOT ov.fk_overflow_to isNULL;"
        )

        # cursor.fetchall() - see https://pynative.com/python-cursor-fetchall-fetchmany-fetchone-to-read-rows-from-table/
        # ws_wn_ids_count = int(cursor.fetchone()[0])
        # if ws_wn_ids_count == 0:
        if cursor.fetchone() is None:
            connected_overflow_to_wn_ids = None
        else:
            # added cursor.execute again to see if with this all records will be available
            # 15.11.2024 added - see https://stackoverflow.com/questions/58101874/cursor-fetchall-or-other-method-fetchone-is-not-working
            cursor.execute(
                f"SELECT ov.fk_overflow_to FROM qgep_od.wastewater_node wn LEFT JOIN qgep_od.overflow ov ON wn.obj_id = ov.fk_wastewater_node WHERE wn.obj_id IN ({subset_text}) AND NOT ov.fk_overflow_to isNULL;"
            )
            records = cursor.fetchall()

            # 15.11.2024 - does not get all records, but only n-1
            for row in records:
                # logger.debug(f" row[0] = {row[0]}")
                # https://www.pythontutorial.net/python-string-methods/python-string-concatenation/
                strrow = str(row[0])
                if strrow is not None:
                    connected_overflow_to_wn_ids.append(strrow)
                    # logger.debug(f" building up '{connected_overflow_to_wn_ids}' ...")
        logger.debug(f" connected_overflow_to_wn_ids: '{connected_overflow_to_wn_ids}'")
    return connected_overflow_to_wn_ids


def get_ws_wn_ids(classname):
    """
    Get list of id's of wastewater_nodes of the wastewater_structure (sub)class provided, eg. wwtp_structure (ARABauwerk, does also work for channel (give reaches then)
    """
    if classname is None:
        return None
    else:
        logger.debug(f"get list of id's of wastewater_nodes of {classname} ...")
        connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
        connection.set_session(autocommit=True)
        cursor = connection.cursor()

        ws_wn_ids = []

        # select all obj_id of the wastewater_nodes of wwtp_structure
        cursor.execute(
            f"SELECT wn.obj_id FROM qgep_od.{classname} LEFT JOIN qgep_od.wastewater_networkelement wn ON wn.fk_wastewater_structure = {classname}.obj_id WHERE wn.obj_id is not NULL;"
        )

        # cursor.fetchall() - see https://pynative.com/python-cursor-fetchall-fetchmany-fetchone-to-read-rows-from-table/
        # ws_wn_ids_count = int(cursor.fetchone()[0])
        # if ws_wn_ids_count == 0:
        if cursor.fetchone() is None:
            ws_wn_ids = None
        else:
            # added cursor.execute again to see if with this all records will be available
            # 15.11.2024 added - see https://stackoverflow.com/questions/58101874/cursor-fetchall-or-other-method-fetchone-is-not-working
            cursor.execute(
                f"SELECT wn.obj_id FROM qgep_od.{classname} LEFT JOIN qgep_od.wastewater_networkelement wn ON wn.fk_wastewater_structure = {classname}.obj_id WHERE wn.obj_id is not NULL;"
            )
            records = cursor.fetchall()

            # 15.11.2024 - does not get all records, but only n-1
            for row in records:
                # logger.debug(f" row[0] = {row[0]}")
                # https://www.pythontutorial.net/python-string-methods/python-string-concatenation/
                strrow = str(row[0])
                if strrow is not None:
                    ws_wn_ids.append(strrow)
                    # logger.debug(f" building up '{ws_wn_ids}' ...")

    return ws_wn_ids


# 12.12.2024
def get_ws_ids(classname):
    """
    Get list of id's of the wastewater_structure (sub)class provided, eg. wwtp_structure (ARABauwerk, does also work for channel (give reaches then)
    """

    if classname is None:
        return None
    else:
        logger.debug(f"get list of id's of subclass {classname} ...")
        connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
        connection.set_session(autocommit=True)
        cursor = connection.cursor()

        ws_ids = []

        # select all obj_id of the wastewater_nodes of wwtp_structure
        cursor.execute(f"SELECT obj_id FROM qgep_od.{classname};")

        # cursor.fetchall() - see https://pynative.com/python-cursor-fetchall-fetchmany-fetchone-to-read-rows-from-table/
        # ws_wn_ids_count = int(cursor.fetchone()[0])
        # if ws_wn_ids_count == 0:
        if cursor.fetchone() is None:
            pass
        else:
            # added cursor.execute again to see if with this all records will be available
            # 15.11.2024 added - see https://stackoverflow.com/questions/58101874/cursor-fetchall-or-other-method-fetchone-is-not-working
            cursor.execute(f"SELECT obj_id FROM qgep_od.{classname};")
            records = cursor.fetchall()

            # 15.11.2024 - does not get all records, but only n-1
            for row in records:
                # logger.debug(f" row[0] = {row[0]}")
                # https://www.pythontutorial.net/python-string-methods/python-string-concatenation/
                strrow = str(row[0])
                if strrow is not None:
                    ws_ids.append(strrow)
                    # logger.debug(f" building up '{ws_wn_ids}' ...")
        logger.debug(f" ws_ids: '{ws_ids}' ...")
    return ws_ids


def get_ws_selected_ww_networkelements(selected_wwn):
    """
    Get list of id's of wastewater_structure from selected wastewater_network_elements
    """

    if selected_wwn is None:
        return None
    else:

        logger.debug(
            f"get list of id's of wastewater_structure of selected wastewater_network_elements {selected_wwn} ..."
        )
        connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
        connection.set_session(autocommit=True)
        cursor = connection.cursor()

        selection_text = ""

        for list_item in selected_wwn:
            selection_text += "'"
            selection_text += list_item
            selection_text += "',"

        # remove last komma to make it a correct IN statement
        selection_text = selection_text[:-1]

        logger.debug(f"selection_text = {selection_text} ...")

        ws_selected_ww_networkelements_ids = []

        # select all obj_id of the wastewater_nodes of wwtp_structure
        cursor.execute(
            f"SELECT ws.obj_id FROM qgep_od.wastewater_structure ws LEFT JOIN qgep_od.wastewater_networkelement wn ON wn.fk_wastewater_structure = ws.obj_id WHERE wn.obj_id IN ({selection_text});"
        )

        # cursor.fetchall() - see https://pynative.com/python-cursor-fetchall-fetchmany-fetchone-to-read-rows-from-table/
        # ws_wn_ids_count = int(cursor.fetchone()[0])
        # if ws_wn_ids_count == 0:
        if cursor.fetchone() is None:
            ws_selected_ww_networkelements_ids = None
        else:
            records = cursor.fetchall()
            for row in records:
                logger.debug(f" row[0] = {row[0]}")
                # https://www.pythontutorial.net/python-string-methods/python-string-concatenation/
                strrow = str(row[0])
                if strrow is not None:
                    ws_selected_ww_networkelements_ids.append(strrow)
                    # logger.debug(f" building up '{ws_selected_ww_networkelements_ids}' ...")
        logger.debug(
            f" ws_selected_ww_networkelements_ids: '{ws_selected_ww_networkelements_ids}' ..."
        )
    return ws_selected_ww_networkelements_ids


# 10.1.2024
def filter_reaches(selected_ids):
    """
    Filter out reaches from selected_ids
    """

    if selected_ids is None:
        return None
    else:
        logger.debug(f"Filter out reaches from selected_ids {selected_ids} ...")
        connection = psycopg2.connect(get_pgconf_as_psycopg2_dsn())
        connection.set_session(autocommit=True)
        cursor = connection.cursor()

        subset_reaches_ids = []
        all_reaches_ids = []

        get_selection_text_for_in_statement(selected_ids)

        # select all reaches
        cursor.execute("SELECT obj_id FROM qgep_od.reach;")

        # cursor.fetchall() - see https://pynative.com/python-cursor-fetchall-fetchmany-fetchone-to-read-rows-from-table/
        # ws_wn_ids_count = int(cursor.fetchone()[0])
        # if ws_wn_ids_count == 0:
        if cursor.fetchone() is None:
            all_reaches_ids = None
        else:
            # added cursor.execute again to see if with this all records will be available
            # 15.11.2024 added - see https://stackoverflow.com/questions/58101874/cursor-fetchall-or-other-method-fetchone-is-not-working
            cursor.execute("SELECT obj_id FROM qgep_od.reach;")
            records = cursor.fetchall()

            # 15.11.2024 - does not get all records, but only n-1
            for row in records:
                # logger.debug(f" row[0] = {row[0]}")
                # https://www.pythontutorial.net/python-string-methods/python-string-concatenation/
                strrow = str(row[0])
                if strrow is not None:
                    all_reaches_ids.append(strrow)
                    # logger.debug(f" building up '{all_reaches_ids}' ...")

            for list_item in selected_ids:
                if list_item in all_reaches_ids:
                    subset_reaches_ids.append(list_item)
                    logger.debug(
                        f"'filter_reaches: {list_item}' is a reach id - added to subset_reaches_ids"
                    )
                else:
                    logger.debug(f"'filter_reaches: {list_item}' is not a reach id")
        logger.debug(f"'subset_reaches_ids: {subset_reaches_ids}'")
    return subset_reaches_ids


def remove_from_selection(selected_ids, remove_ids):
    """
    Remove ids from selected_ids if they are in selected_ids
    """

    if remove_ids is None:
        return None
    else:
        for list_item in remove_ids:
            # selected_ids = selected_ids.remove(list_item)
            try:
                selected_ids.remove(list_item)
            except Exception:
                logger.debug(
                    f" remove_from_selection: '{list_item}' not in selected_ids - could not be removed!"
                )

    return selected_ids


def add_to_selection(selected_ids, add_ids):
    """
    Append ids to selected_ids
    """
    if add_ids is None:
        return None
    else:
        if selected_ids is None:
            selected_ids = []

        for list_item in add_ids:
            # selected_ids = selected_ids.append(list_item)
            selected_ids.append(list_item)

    return selected_ids
