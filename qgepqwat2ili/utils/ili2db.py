import collections

import psycopg2
from sqlalchemy.ext.automap import AutomapBase

from .. import config
from .various import exec_, get_pgconf, logger


def create_ili_schema(schema, model, log_path, recreate_schema=False):
    logger.info("CONNECTING TO DATABASE...")

    pgconf = get_pgconf()

    connection = psycopg2.connect(
        f"host={pgconf['host']} port={pgconf['port']} dbname={pgconf['dbname']} user={pgconf['user']} password={pgconf['password']}"
    )
    connection.set_session(autocommit=True)
    cursor = connection.cursor()

    if not recreate_schema:
        # If the schema already exists, we just truncate all tables
        cursor.execute(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema}';")
        if cursor.rowcount > 0:
            logger.info(f"Schema {schema} already exists, we truncate instead")
            cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}';")
            for row in cursor.fetchall():
                cursor.execute(f"TRUNCATE TABLE {schema}.{row[0]} CASCADE;")
            return

    logger.info(f"DROPPING THE SCHEMA {schema}...")
    cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE ;')
    logger.info(f"CREATING THE SCHEMA {schema}...")
    cursor.execute(f'CREATE SCHEMA "{schema}";')
    connection.commit()
    connection.close()

    logger.info(f"ILIDB SCHEMAIMPORT INTO {schema}...")
    pgconf = get_pgconf()
    #exec_(
    #    f'"{config.JAVA}" -jar {config.ILI2PG} --schemaimport --dbhost {pgconf["host"]} --dbport {pgconf["port"]} --dbusr {pgconf["user"]} --dbpwd {pgconf["password"]} --dbdatabase {pgconf["dbname"]} --dbschema {schema} --setupPgExt --createGeomIdx --createFk --createFkIdx --createTidCol --importTid --noSmartMapping --defaultSrsCode 2056 --log {log_path} --nameLang de {model}'
    #)
    # 13.7.2022 mit --trace / ohne --createFkIdx und --createGeomIdx
    exec_(
        f'"{config.JAVA}" -jar {config.ILI2PG} --schemaimport --dbhost {pgconf["host"]} --dbport {pgconf["port"]} --dbusr {pgconf["user"]} --dbpwd {pgconf["password"]} --dbdatabase {pgconf["dbname"]} --dbschema {schema} --setupPgExt --createFk  --createTidCol --importTid --noSmartMapping --defaultSrsCode 2056 --trace --log {log_path} --nameLang de {model}'
    )


def validate_xtf_data(xtf_file, log_path):
    logger.info("VALIDATING XTF DATA...")
    exec_(f'"{config.JAVA}" -jar {config.ILIVALIDATOR} --modeldir {config.ILI_FOLDER} --log {log_path} {xtf_file}')


# 22.7.2022 sb [WIP]
def get_xtf_model(xtf_file):
    logger.info("GET XTF MODEL... ")
    # logger.info("vorher" + imodel)
# funktioniert nicht
    # global imodel # define imodel as global variable for import model name
    # impmodel = "" 

    # open and read xtf / xml file line by line until <DATASECTION>
    #<DATASECTION>
    #<VSA_KEK_2019_LV95.KEK BID="VSA_KEK_2019_LV95.KEK">
    # read string between < and . -> eg. VSA_KEK_2019_LV95
    # impmodel 
    from io import open
    import re
    
    checkdatasection = -1
    impmodel = "not found"
    
    with open(xtf_file, mode="r", encoding="utf-8") as f:
        while True:
            if checkdatasection == -1:
                line = f.readline()
                if not line:
                    break
                else:
                    checkdatasection = line.find('<DATASECTION>')
                    # logger.info("checkdatasection: " + str(checkdatasection))
                    logger.info(str(checkdatasection))
                    
            else:
                line2 = f.readline()
                if not line:
                    break
                else:
                    print(line)
                    logger.info(str(checkdatasection))
                    strmodel = str(line2)
                    logger.info("MODEL definition found in xtf: " + strmodel)
                    #<VSA_KEK_2019_LV95.KEK BID="VSA_KEK_2019_LV95.KEK">
                    # read string between < and . -> eg. VSA_KEK_2019_LV95
                    
                    result = re.search('<(.*).',strmodel)
                    result = str(result.group(1))
                    result2 = result.split('.',1)
                    result3 = str(result2[0])
                    result4 = result3.strip('<')
                    impmodel = str(result4)
                    # 23.7.2022 neu als str
                    #impmodel = str(result)
                    # 23.7.2022 + impmodel geht nicht, check solution [WIP] TypeError: can only concatenate str (not "re.Match") to str
                    logger.info("MODEL found: " + str(impmodel))
                    break
    
    if impmodel == "not found":
        # write that MODEL was not found
        logger.info("MODEL was " + impmodel)
        
    # im Moment fix gesetzt
    # 23.7.2022 import_dialog nicht bekannt so
    # import_dialog.label_importmodelname.setText("VSA_KEK_2019_LV95")
    #logger.info("import_dialog.label: " + import_dialog.label_importmodelname.currentText())
    # impmodel = "VSA_KEK_2019_LV95"

    # close xtf file to avoid conflicts
    f.close()

    # neu 23.7.2022 return imodel from get_xtf_model so it can be called in _init_.py
    return impmodel

# 23.7.2022 added model_name
def import_xtf_data(schema, xtf_file, log_path):
#def import_xtf_data(schema, xtf_file, log_path, model_name):
    logger.info("IMPORTING XTF DATA...")
    #logger.info("IMPORTING XTF DATA..." + model_name)
    pgconf = get_pgconf()
    exec_(
        f'"{config.JAVA}" -jar {config.ILI2PG} --import --deleteData --dbhost {pgconf["host"]} --dbport {pgconf["port"]} --dbusr {pgconf["user"]} --dbpwd {pgconf["password"]} --dbdatabase {pgconf["dbname"]} --dbschema {schema} --modeldir {config.ILI_FOLDER} --disableValidation --skipReferenceErrors --createTidCol --noSmartMapping --defaultSrsCode 2056 --log {log_path} {xtf_file}'
    )


def export_xtf_data(schema, model_name, xtf_file, log_path):
    # logger.info("EXPORT ILIDB...")
    logger.info("EXPORT ILIDB..." + model_name)
    pgconf = get_pgconf()
    exec_(
        f'"{config.JAVA}" -jar {config.ILI2PG} --export --models {model_name} --dbhost {pgconf["host"]} --dbport {pgconf["port"]} --dbusr {pgconf["user"]} --dbpwd {pgconf["password"]} --dbdatabase {pgconf["dbname"]} --dbschema {schema} --modeldir {config.ILI_FOLDER} --disableValidation --skipReferenceErrors --createTidCol --noSmartMapping --defaultSrsCode 2056 --log {log_path} --trace {xtf_file}'
    )


class TidMaker:
    """
    Helper class that creates globally unique integer primary key forili2pg class (t_id)
    from a a QGEP/QWAT id (obj_id or id).
    """

    def __init__(self, id_attribute="id"):
        self._id_attr = id_attribute
        self._autoincrementer = collections.defaultdict(lambda: len(self._autoincrementer))

    def tid_for_row(self, row, for_class=None):
        # tid are globally unique, while ids are only guaranteed unique per table,
        # so include the base table in the key
        # this finds the base class (the first parent class before sqlalchemy.ext.automap.Base)
        class_for_id = row.__class__.__mro__[row.__class__.__mro__.index(AutomapBase) - 2]
        key = (class_for_id, getattr(row, self._id_attr), for_class)
        # was_created = key not in self._autoincrementer  # just for debugging
        tid = self._autoincrementer[key]
        # if was_created:
        #     # just for debugging
        #     logger.info(f"created tid {tid} for {key}")
        return tid
