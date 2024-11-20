import os

BASE = os.path.dirname(__file__)

PGSERVICE = None  # overriden by PG* settings below
PGHOST = os.getenv("PGHOST", None)
PGPORT = os.getenv("PGPORT", None)
PGDATABASE = os.getenv("PGDATABASE", None)
PGUSER = os.getenv("PGUSER", None)
PGPASS = os.getenv("PGPASS", None)
JAVA = r"java"

# 12.7.2022 Anpassung auf 4.61 damit auch VSA-DSS kompatibel siehe https://github.com/claeis/ili2db/issues/374
ILI2PG = os.path.join(BASE, "bin", "ili2pg-5.2.0", "ili2pg-5.2.0.jar")
ILIVALIDATOR = os.path.join(BASE, "bin", "ilivalidator-1.14.3", "ilivalidator-1.14.3.jar")
ILI_FOLDER = os.path.join(BASE, "ili")
DATA_FOLDER = os.path.join(BASE, "data")

QGEP_DEFAULT_PGSERVICE = "pg_qgep"
QGEP_SCHEMA = "qgep_od"
# to do rename to ABWASSER_KEK_SCHEMA = "pg2ili_kek"
ABWASSER_SCHEMA = "pg2ili_abwasser"

ABWASSER_ILI_MODEL = os.path.join(ILI_FOLDER, "VSA_KEK_2019_2_d_LV95-20210120.ili")
ABWASSER_ILI_MODEL_NAME = "VSA_KEK_2019_LV95"
ABWASSER_ILI_MODEL_NAME_SIA405 = "SIA405_ABWASSER_2015_LV95"
ABWASSER_ILI_EXPORT_MODEL = os.path.join(ILI_FOLDER, "SIA405_Abwasser_2015_2_d-20180417.ili")


# neu 12.7.2022 weitere Modelle ergänzt
# to add for only SIA405 Abwasser
ABWASSER_SIA405_SCHEMA = "pg2ili_sia405abwasser"
# copy from ABWASSER_ILI_MODEL_NAME_SIA405 as has to be the same
ABWASSER_SIA405_ILI_MODEL_NAME = ABWASSER_ILI_MODEL_NAME_SIA405
# copy from ABWASSER_ILI_EXPORT_MODEL as has to be the same
ABWASSER_SIA405_ILI_MODEL = ABWASSER_ILI_EXPORT_MODEL

ABWASSER_DSS_SCHEMA = "pg2ili_dss"
ABWASSER_DSS_ILI_MODEL = os.path.join(ILI_FOLDER, "VSA_DSS_2015_2_d-20200330.ili")
ABWASSER_DSS_ILI_MODEL_NAME = "DSS_2015_LV95"


QWAT_DEFAULT_PGSERVICE = "qwat"
QWAT_SCHEMA = "qwat_od"
WASSER_SCHEMA = "pg2ili_wasser"
# Currently, ili2pg fails to export SIA405_Wasser_2015_2_d due to views
# TODO Once https://github.com/claeis/ili2db/issues/397#issuecomment-821342568 is fixed
# use the normal file instad
WASSER_ILI_MODEL = os.path.join(ILI_FOLDER, "SIA405_Wasser_2015_2_d-20181005.ili")
#WASSER_ILI_MODEL = os.path.join(ILI_FOLDER, "SIA405_Wasser_2015_2_d-20181005-WITHOUT_VIEWS.ili")
WASSER_ILI_MODEL_NAME = "SIA405_WASSER_2015_LV95"

TOPIC_NAME_SIA405_ADMINISTRATION = "SIA405_Base_Abwasser_LV95.Administration"
TOPIC_NAME_SIA405_ABWASSER = "SIA405_ABWASSER_2015.SIA405_Abwasser"
TOPIC_NAME_DSS = "DSS_2015.Siedlungsentwaesserung"
TOPIC_NAME_KEK = "VSA_KEK_2019_LV95.KEK"
