/*
This script is a temporary delta for QWAT, must be commited to QWAT itself
*/

-- Add columns

ALTER TABLE qwat_vl.bedding ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.cistern ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.cover_type ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.hydrant_material ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.hydrant_model_inf ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.hydrant_model_sup ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.hydrant_output ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.hydrant_provider ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.leak_cause ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.locationtype ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.nominal_diameter ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.object_reference ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.overflow ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.part_type ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.pipe_function ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.pipe_installmethod ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.pipe_material ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.pipe_protection ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.precision ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.precisionalti ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.pressurecontrol_type ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.protectionzone_type ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.pump_operating ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.pump_type ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.remote_type ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.source_quality ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.source_type ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.status ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.subscriber_type ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.survey_type ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.tank_firestorage ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.value_list_base ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.valve_actuation ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.valve_function ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.valve_maintenance ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.valve_type ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.visible ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.watertype ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';
ALTER TABLE qwat_vl.worker_type ADD COLUMN sia405_de TEXT NOT NULL DEFAULT 'unbekannt';


-- Populate columns

-- TODO : complete mapping
UPDATE qwat_vl.bedding SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.cistern SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.cover_type SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.hydrant_material SET sia405_de = CASE
    WHEN short_fr LIKE 'F %' THEN 'Metall'
    ELSE 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.hydrant_model_inf SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.hydrant_model_sup SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.hydrant_output SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.hydrant_provider SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.leak_cause SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.locationtype SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.nominal_diameter SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.object_reference SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.overflow SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.part_type SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.pipe_function SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.pipe_installmethod SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.pipe_material SET sia405_de = CASE
    WHEN short_fr = 'AC' THEN 'Stahl.unbekannt'
    WHEN short_fr = 'PE' THEN 'Kunststoff.Polyethylen.unbekannt'
    ELSE 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.pipe_protection SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.precision SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.precisionalti SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.pressurecontrol_type SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.protectionzone_type SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.pump_operating SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.pump_type SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.remote_type SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.source_quality SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.source_type SET sia405_de = CASE
    WHEN value_fr = 'captage eau lac' THEN 'Fassungsanlage'
    WHEN value_fr = 'captage eau nappe' THEN 'Fassungsanlage.Grundwasserfassung'
    WHEN value_fr = 'captage eau source' THEN 'Fassungsanlage.Fluss_Seewasserfassung'
    WHEN value_fr = 'captage eau rivière' THEN 'Fassungsanlage.Fluss_Seewasserfassung'
    ELSE 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.status SET sia405_de = CASE
    WHEN value_en = 'in use' THEN 'in_Betrieb'
    WHEN value_en = 'out of service' THEN 'ausser_Betrieb'
    ELSE 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.subscriber_type SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.survey_type SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.tank_firestorage SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.value_list_base SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.valve_actuation SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.valve_function SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.valve_maintenance SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.valve_type SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.visible SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.watertype SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;

-- TODO : complete mapping
UPDATE qwat_vl.worker_type SET sia405_de = CASE
    WHEN TRUE THEN 'unbekannt'
END;
