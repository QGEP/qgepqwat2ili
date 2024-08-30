# Needs delta_1.6.3_functions_update_fk_main_cover_main_wastewater_node.sql to work properly

from functools import lru_cache

from geoalchemy2.functions import ST_Force3D
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_dirty
from sqlalchemy.sql import text

#31.5.2024 pfade anpassen
#from .. import utils
#from ..utils.various import logger
from . import utils
from .utils.various import logger
#from .model_abwasser import get_abwasser_model
#from .model_qgep import get_qgep_model


def qgep_postimport():
    """
    Additional queries run after qgep_import
    """


    # move in extra file and function postimport
    # TODO : put this in an "finally" block (or context handler) to make sure it's executed
    # even if there's an exception


    post_session = Session(utils.sqlalchemy.create_engine(), autocommit=False, autoflush=False)
    logger.info("re-enabling symbology triggers (postimport.py)")
    post_session.execute(text("SELECT qgep_sys.create_symbology_triggers();"))
    logger.info("symbology triggers successfully created! (postimport.py)")

    # add queries for main_cover and main_node as in TEKSI, add to symbology functions
    # see teksi ww https://github.com/teksi/wastewater/blob/3acfba249866d299f8a22e249d9f1e475fe7b88d/datamodel/app/symbology_functions.sql#L290
    # needs delta_1.6.3_functions_update_fk_main_cover_main_wastewater_node.sql
    logger.info("Update wastewater structure fk_main_cover")
    post_session.execute(text("SELECT qgep_od.wastewater_structure_update_fk_main_cover('', True);"))

    logger.info("Update wastewater structure fk_main_wastewater_node")
    post_session.execute(text("SELECT qgep_od.wastewater_structure_update_fk_main_wastewater_node('', True));"
    )

    # add symbology update queries - backporting from tww https://github.com/teksi/wastewater/pull/263
    logger.info("update_wastewater_node_symbology for all datasets - please be patient")
    post_session.execute(text("SELECT qgep_od.update_wastewater_node_symbology(NULL, True);"))

    logger.info("update_wastewater_node_symbology for all datasets - please be patient")
    logger.info("update_wastewater_structure_label for all datasets - please be patient")
    post_session.execute(text("SELECT qgep_od.update_wastewater_structure_label(NULL, True);"))

    logger.info("update_wastewater_node_symbology for all datasets - please be patient")
    # update_wastewater_structure_symbology instead of update_wn_symbology_by_overflow (tww)
    logger.info("update_wastewater_structure_symbology for all datasets - please be patient")
    post_session.execute(text("SELECT qgep_od.update_wastewater_structure_symbology(NULL, True);"))

    logger.info("Refresh materialized views")
    post_session.execute(text("SELECT qgep_network.refresh_network_simple();"))

    post_session.commit()
    post_session.close()