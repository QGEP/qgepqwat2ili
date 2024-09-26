/*
This fixes some invalid (according to SIA405) values in the QGEP demo data, so that
we ilivalidator validates the exports.

TODO this should probably be applied to the actual demo data if these are actual data errors.
*/

-- Error: line 17814: SIA405_ABWASSER_2015_LV95.SIA405_Abwasser.Normschacht: tid ch13p7mzMA009659: value 4500 is out of range in attribute Dimension1
UPDATE qgep_od.manhole SET dimension1=4000 WHERE dimension1>4000;
UPDATE qgep_od.manhole SET dimension2=4000 WHERE dimension2>4000;

-- Error: line 79976: SIA405_ABWASSER_2015_LV95.SIA405_Abwasser.Haltung: tid ch13p7mzRE005187: duplicate coord at (2748419.603, 1265870.794, NaN)
UPDATE qgep_od.reach SET progression_geometry=ST_RemoveRepeatedPoints(progression_geometry, 0.002) WHERE NOT ST_EQUALS(progression_geometry, ST_RemoveRepeatedPoints(progression_geometry, 0.001));

-- Error: line 84452: SIA405_ABWASSER_2015_LV95.SIA405_Abwasser.Haltung: tid ch13p7mzRE009663: duplicate coord at (2750088.164, 1263080.203, NaN)
-- degenerated geometry
DELETE FROM qgep_od.reach WHERE obj_id='ch13p7mzRE009663';

-- add organisations subclass entries to fit requirements for VSA-DSS export
-- "ch13p7mzOG000002"	"Berg"
-- "ch13p7mzOG000003"	"Mörschwil"
-- "ch13p7mzOG000004"	"Tübach"
-- "ch13p7mzOG000007"	"Arbon"
-- "ch13p7mzOG000008"	"Frasnacht"
-- "ch13p7mzOG000009"	"Egnach"
-- "ch13p7mzOG000010"	"Horn"
-- "ch13p7mzOG000011"	"Roggwil"
-- "ch13p7mzOG000012"	"Steinach"
-- "ch13p7mzOG000013"	"Gemeinde"
INSERT INTO qgep_od.municipality (obj_id) VALUES ('ch13p7mzOG000002');
INSERT INTO qgep_od.municipality (obj_id) VALUES ('ch13p7mzOG000003');
INSERT INTO qgep_od.municipality (obj_id) VALUES ('ch13p7mzOG000004');
INSERT INTO qgep_od.municipality (obj_id) VALUES ('ch13p7mzOG000007');
INSERT INTO qgep_od.municipality (obj_id) VALUES ('ch13p7mzOG000008');
INSERT INTO qgep_od.municipality (obj_id) VALUES ('ch13p7mzOG000009');
INSERT INTO qgep_od.municipality (obj_id) VALUES ('ch13p7mzOG000010');
INSERT INTO qgep_od.municipality (obj_id) VALUES ('ch13p7mzOG000011');
INSERT INTO qgep_od.municipality (obj_id) VALUES ('ch13p7mzOG000012');
INSERT INTO qgep_od.municipality (obj_id) VALUES ('ch13p7mzOG000013');

-- "ch13p7mzOG000014"	"Private"
-- "ch13p7mzOG000006"	"Privat"
-- "ch13p7mzOG000016"	"unbekannt"
INSERT INTO qgep_od.private (obj_id) VALUES ('ch13p7mzOG000014');
INSERT INTO qgep_od.private (obj_id) VALUES ('ch13p7mzOG000006');
INSERT INTO qgep_od.private (obj_id) VALUES ('ch13p7mzOG000016');

-- "ch13p7mzOG000005"	"AV Morgental"
-- "ch13p7mzOG000015"	"Verband"
INSERT INTO qgep_od.waste_water_association (obj_id) VALUES ('ch13p7mzOG000005');
INSERT INTO qgep_od.waste_water_association (obj_id) VALUES ('ch13p7mzOG000015');

-- also add missing fk_dataowner and fk_provider

UPDATE qgep_od.organisation
SET fk_dataowner = 'ch13p7mzOG000016',
    fk_provider = 'ch13p7mzOG000016'
;

-- set fk_owner to ch13p7mzOG000013 Gemeinde - to do distinguish PAA, SAA more in detail and differentiate the municipalities
UPDATE qgep_od.wastewater_structure
SET fk_owner = 'ch13p7mzOG000013'
WHERE fk_owner is NULL;

-- set fk_operator to ch13p7mzOG000015 Verband
UPDATE qgep_od.wastewater_structure
SET fk_operator = 'ch13p7mzOG000015'
WHERE fk_operator is NULL;

-- set fk_dataowner to ch13p7mzOG000013 Gemeinde - to do distinguish PAA, SAA more in detail and differentiate the municipalities
UPDATE qgep_od.wastewater_structure
SET fk_dataowner = 'ch13p7mzOG000013'
WHERE fk_dataowner is NULL;

-- set fk_provider to ch13p7mzOG000013 Gemeinde - to do distinguish PAA, SAA more in detail and differentiate the municipalities
UPDATE qgep_od.wastewater_structure
SET fk_provider = 'ch13p7mzOG000013'
WHERE fk_provider is NULL;
