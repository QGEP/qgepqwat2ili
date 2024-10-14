import json

from geoalchemy2.functions import ST_Force2D, ST_GeomFromGeoJSON

from .various import logger


class QgepExportUtils:

    def __init__(
        self, tid_maker, current_basket, abwasser_session, abwasser_model, labelorientation
    ):
        self.tid_maker = tid_maker
        self.current_basket = current_basket
        self.abwasser_session = abwasser_session
        self.abwasser_model = abwasser_model
        self.labelorientation = labelorientation

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

        logger.info(f"check_fk_in_subsetid -  Subset ID's '{subset}'")
        # get the value of the fk_ attribute as str out of the relation to be able to check whether it is in the subset
        fremdschluesselstr = getattr(relation, "obj_id")
        logger.info(f"check_fk_in_subsetid -  fremdschluesselstr '{fremdschluesselstr}'")

        if fremdschluesselstr in subset:
            logger.info(f"check_fk_in_subsetid - '{fremdschluesselstr}' is in subset ")
            logger.info(f"check_fk_in_subsetid - tid = '{self.tid_maker.tid_for_row(relation)}' ")
            return self.tid_maker.tid_for_row(relation)
        else:
            logger.info(
                f"check_fk_in_subsetid - '{fremdschluesselstr}' is not in subset - replaced with None instead!"
            )
            return None

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
            t_basket=self.current_basket.t_id,
        )

        if not self.current_basket.t_id:
            raise Exception("Basket id can't be null")

        self.abwasser_session.add(metaattribute)

    def base_common(self, row, type_name):
        """
        Returns common attributes for base
        """
        if not self.current_basket.t_id:
            raise Exception("Basket id can't be null")

        return {
            "t_ili_tid": row.obj_id,
            "t_type": type_name,
            "obj_id": row.obj_id,
            "t_id": self.get_tid(row),
            "t_basket": self.current_basket.t_id,
        }

    def wastewater_structure_common(self, row):
        """
        Returns common attributes for wastewater_structure
        """
        logger.warning(
            "Mapping of wastewater_structure->abwasserbauwerk is not fully implemented."
        )
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
            "abwasserbauwerkref": self.get_tid(row.fk_wastewater_structure__REL),
            "bemerkung": self.truncate(self.emptystr_to_null(row.remark), 80),
            "bezeichnung": self.null_to_emptystr(row.identifier),
        }

    def structure_part_common(self, row):
        """
        Returns common attributes for structure_part
        """
        return {
            "abwasserbauwerkref": self.get_tid(row.fk_wastewater_structure__REL),
            "bemerkung": self.truncate(self.emptystr_to_null(row.remark), 80),
            "bezeichnung": self.null_to_emptystr(row.identifier),
            "instandstellung": self.get_vl(row.renovation_demand__REL),
        }

    def textpos_common(self, row, t_type, geojson_crs_def):
        """
        Returns common attributes for textpos
        """
        t_id = self.tid_maker.next_tid()
        return {
            "t_id": t_id,
            "t_type": t_type,
            "t_ili_tid": t_id,
            "t_basket": self.current_basket.t_id,
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
