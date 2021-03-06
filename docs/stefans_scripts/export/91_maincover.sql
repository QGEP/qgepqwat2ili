UPDATE qgep_od.wastewater_structure WS
SET fk_main_cover = structure_cover.co_obj_id
FROM (
  SELECT WS.obj_id AS obj_id, min(CO.obj_id) AS co_obj_id
  FROM qgep_od.wastewater_structure WS
  LEFT JOIN qgep_od.structure_part SP ON SP.fk_wastewater_structure = WS.obj_id
  LEFT JOIN qgep_od.cover CO ON SP.obj_id = CO.obj_id
  GROUP BY WS.obj_id
) AS structure_cover
WHERE structure_cover.obj_id = WS.obj_id;
