import collections
import configparser
import logging
import os
import subprocess
import time
import warnings

from .. import config

logging.captureWarnings(True)
logger = logging.getLogger(__package__)


class CmdException(BaseException):
    pass


def exec_(command, check=True):
    logger.info(f"EXECUTING: {command}")
    try:
        proc = subprocess.run(
            command,
            check=True,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
    except subprocess.CalledProcessError as e:
        if check:
            logger.exception(e.output.decode("windows-1252" if os.name == "nt" else "utf-8"))
            raise CmdException(f"Command errored ! See logs for more info.")
        return e.returncode
    return proc.returncode


def setup_test_db(template="full"):
    """
    As initializing demo data can be a bit slow,
    we do these steps in a Docker container then commit it to an image, so we can
    start a clean database relatively quickly.

    We prepare two template databases : full, empty
    And then copy those to config.PGDATABASE when needed to initialize an fresh db
    for testing.
    """

    pgconf = get_pgconf()

    def dexec_(cmd, check=True):
        return exec_(f"docker exec qgepqwat {cmd}", check)

    logger.info("SETTING UP QGEP/QWAT DATABASE...")
    r = exec_("docker inspect -f '{{.State.Running}}' qgepqwat", check=False)
    if r != 0:
        logger.info("Test container not running, we create it")

        exec_(
            f"docker run -d --rm -p 5432:5432 --name qgepqwat -e POSTGRES_PASSWORD={pgconf['password'] or 'postgres'} -e POSTGRES_DB={pgconf['dbname'] or 'qgep_prod'} postgis/postgis"
        )

    # Wait for PG
    while exec_("docker exec qgepqwat pg_isready", check=False) != 0:
        logger.info("Postgres not ready... we wait...")
        time.sleep(1)

    db_didnt_exist = (
        dexec_("createdb -U postgres tpl_empty", check=False) == 0
        or dexec_("createdb -U postgres tpl_full", check=False) == 0
    )
    if db_didnt_exist:
        logger.info("Test templates don't exist, we create them")

        dexec_("dropdb -U postgres tpl_empty --if-exists")
        dexec_("dropdb -U postgres tpl_full --if-exists")

        dexec_("apt-get update")
        dexec_("apt-get install -y wget")

        # Getting data
        dexec_(
            "wget https://github.com/QGEP/datamodel/releases/download/1.5.4/qgep_1.5.4_structure_and_demo_data.backup"
        )
        dexec_(
            "wget https://github.com/QGEP/datamodel/releases/download/1.5.4/qgep_1.5.4_structure_with_value_lists.sql"
        )
        dexec_(
            "wget https://github.com/qwat/qwat-data-model/releases/download/1.3.5/qwat_v1.3.5_data_and_structure_sample.backup"
        )
        dexec_("wget https://github.com/qwat/qwat-data-model/releases/download/1.3.5/qwat_v1.3.5_structure_only.sql")
        dexec_(
            "wget https://github.com/qwat/qwat-data-model/releases/download/1.3.5/qwat_v1.3.5_value_list_data_only.sql"
        )

        # Creating the template DB with empty structure
        dexec_("psql -f qgep_1.5.4_structure_with_value_lists.sql qgep_prod postgres")
        dexec_("psql -f qwat_v1.3.5_structure_only.sql qgep_prod postgres")
        dexec_("psql -f qwat_v1.3.5_value_list_data_only.sql qgep_prod postgres")
        dexec_("createdb -U postgres --template=qgep_prod tpl_empty")

        # Creating the template DB with full data
        dexec_(
            'psql -U postgres -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid<>pg_backend_pid();"'
        )
        dexec_("dropdb -U postgres qgep_prod --if-exists")
        dexec_("createdb -U postgres qgep_prod")
        dexec_(
            "pg_restore -U postgres --dbname qgep_prod --verbose --no-privileges --exit-on-error qgep_1.5.4_structure_and_demo_data.backup"
        )
        dexec_(
            "pg_restore -U postgres --dbname qgep_prod --verbose --no-privileges --exit-on-error qwat_v1.3.5_data_and_structure_sample.backup"
        )
        dexec_("createdb -U postgres --template=qgep_prod tpl_full")

        # Hotfix qgep invalid demo data
        delta_path = os.path.join(os.path.dirname(__file__), "..", "data", "test_data", "qgep_demodata_hotfix.sql")
        exec_(f"docker cp {delta_path} qgepqwat:/qgep_demodata_hotfix.sql")
        dexec_("psql -U postgres -d tpl_full -v ON_ERROR_STOP=1 -f /qgep_demodata_hotfix.sql")

        # Hotfix qwat invalid demo data
        delta_path = os.path.join(os.path.dirname(__file__), "..", "data", "test_data", "qwat_demodata_hotfix.sql")
        exec_(f"docker cp {delta_path} qgepqwat:/qwat_demodata_hotfix.sql")
        dexec_("psql -U postgres -d tpl_full -v ON_ERROR_STOP=1 -f /qwat_demodata_hotfix.sql")

        # Hotfix qwat datamodel migration
        # TODO : publish a QWAT datamodel upgrade and remove this
        logger.warning("A HOTFIX DELTA WILL BE APPIED TO QWAT, THIS MUST NOT BE APPLIED TO PRODUCTION")
        delta_path = os.path.join(os.path.dirname(__file__), "..", "data", "test_data", "qwat_temp_migration.sql")
        exec_(f"docker cp {delta_path} qgepqwat:/qwat_temp_migration.sql")
        dexec_("psql -U postgres -d tpl_full -v ON_ERROR_STOP=1 -f /qwat_temp_migration.sql")
        dexec_("psql -U postgres -d tpl_empty -v ON_ERROR_STOP=1 -f /qwat_temp_migration.sql")

    dexec_(
        f'psql -U postgres -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid<>pg_backend_pid();"'
    )
    dexec_(f"dropdb -U postgres qgep_prod --if-exists")
    dexec_(f"createdb -U postgres --template=tpl_{template} qgep_prod")


def capfirst(s):
    return s[0].upper() + s[1:]


def invert_dict(d):
    return {v: k for k, v in d.items()}


def read_pgservice(service_name):
    """
    Returns a config object from a pg_service name (parsed from PGSERVICEFILE).
    """

    # Path for pg_service.conf
    if os.environ.get("PGSERVICEFILE"):
        PG_CONFIG_PATH = os.environ.get("PGSERVICEFILE")
    elif os.environ.get("PGSYSCONFDIR"):
        PG_CONFIG_PATH = os.path.join(os.environ.get("PGSYSCONFDIR"), "pg_service.conf")
    else:
        PG_CONFIG_PATH = " ~/.pg_service.conf"

    config = configparser.ConfigParser()
    if os.path.exists(PG_CONFIG_PATH):
        config.read(PG_CONFIG_PATH)

    if service_name not in config:
        warnings.warn(f"Service `{service_name}` not found in {PG_CONFIG_PATH}.")
        return {}

    return config[service_name]


def get_pgconf():
    """
    Returns the postgres configuration (parsed from the config.PGSERVICE service and overriden by config.PG* settings)
    """

    if config.PGSERVICE:
        pgconf = read_pgservice(config.PGSERVICE)
    else:
        pgconf = {}

    if config.PGHOST:
        pgconf["host"] = config.PGHOST
    if config.PGPORT:
        pgconf["port"] = config.PGPORT
    if config.PGDATABASE:
        pgconf["dbname"] = config.PGDATABASE
    if config.PGUSER:
        pgconf["user"] = config.PGUSER
    if config.PGPASS:
        pgconf["password"] = config.PGPASS

    return collections.defaultdict(str, pgconf)
