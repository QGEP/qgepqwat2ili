import argparse
import sys
from logging import INFO, FileHandler, Formatter

from . import config, utils
from .qgep.export import qgep_export
# 4.4.2023
from .qgepsia405.export import qgep_export as qgepsia405_export
from .qgepdss.export import qgep_export as qgepdss_export
from .qgep.import_ import qgep_import
# 5.4.2023
from .qgepsia405.import_ import qgep_import as qgepsia405_import
from .qgepdss.import_ import qgep_import as qgepdss_import

from .qgep.mapping import get_qgep_mapping
from .qgep.model_abwasser import Base as BaseAbwasser
from .qgep.model_qgep import Base as BaseQgep
from .qwat.export import qwat_export
from .qwat.import_ import qwat_import
from .qwat.mapping import get_qwat_mapping
from .qwat.model_qwat import Base as BaseQwat
from .qwat.model_wasser import Base as BaseWasser
from .utils.various import make_log_path


def main(args):

    parser = argparse.ArgumentParser(
        description="ili2QWAT / ili2QGEP entrypoint", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    subparsers = parser.add_subparsers(title="subcommands", dest="parser")
    # subparsers.required = True

    parser_qgep = subparsers.add_parser(
        "qgep",
        help="import/export QGEP datamodel",
        description="ili2QGEP entrypoint",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    # group = parser_qgep.add_mutually_exclusive_group(required=True)
    parser_qgep.add_argument("direction", choices=["import", "export"])
    parser_qgep.add_argument(
        "--selection",
        help="if provided, limits the export to networkelements that are provided in the selection (comma separated list of ids)",
    )
   
    # TODO: this only makes sense for export
    parser_qgep.add_argument(
        "--labels_file",
        help="if provided, includes the label positions in the export (the file should be the results of the provided `qgep:extractlabels_interlis` QGIS algorithm as geojson)",
    )

    #4.4.2023 / 5.4.2023 replaced with choices
    #parser_qgep.add_argument(
    #    "--labels_orientation",
    #    help="parameter to adjust orientation of label text to fit other default values for north direction. If provided, turns the orientation by the given value [90, -90, 0] degree)",
    #)
    #parser_tpl.add_argument("model", choices=["qgep", "qwat"])
    # back to argument to make it optional
    #parser_qgep.add_argument("labels_orientation", choices=["90.0", "0.0","-90.0"])
    parser_qgep.add_argument(
        "--labels_orientation",
        #choices=["90.0", "0.0","-90.0"],
        #help="parameter to adjust orientation of label text to fit other default values for north direction. If provided, turns the orientation by the given value [90, -90, 0] degree)",
    )

    parser_qgep.add_argument(
        "--recreate_schema", action="store_true", help="drops schema and reruns ili2pg importschema"
    )
    parser_qgep.add_argument(
        "--skip_validation",
        action="store_true",
        help="skips running ilivalidator on input/output xtf (required to import invalid files, invalid outputs are still generated)",
    )
    parser_qgep.add_argument("path", help="path to the input/output .xtf file")
    parser_qgep.add_argument(
        "--pgservice",
        help="name of the pgservice to use to connect to the database",
        default=config.QGEP_DEFAULT_PGSERVICE,
    )
    parser_qgep.add_argument(
        "--log",
        action="store_true",
        help="saves the log files next to the input/output file",
    )
    parser_qgep.add_argument(
        "--export_sia405",
        action="store_true",
        help="export the model SIA405_ABWASSER_2015_LV95 (instead of default VSA_KEK_2019_LV95)",
    )
    # 24.3.2023
    parser_qgep.add_argument(
        "--export_dss",
        action="store_true",
        help="export the model DSS_2015_LV95 (instead of default VSA_KEK_2019_LV95)",
    )

    parser_qwat = subparsers.add_parser(
        "qwat",
        help="import/export QWAT datamodel",
        description="ili2QWAT entrypoint",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_qwat.add_argument("direction", choices=["import", "export"])
    parser_qwat.add_argument(
        "--include_hydraulics",
        action="store_true",
        help="if provided, exports will include hydraulischer_strang and hydraulischer_node classes (these are currently likely to make the export invalid due to issues with the current ili model)",
    )
    parser_qwat.add_argument(
        "--recreate_schema", action="store_true", help="drops schema and reruns ili2pg importschema"
    )
    parser_qwat.add_argument(
        "--skip_validation",
        action="store_true",
        help="skips running ilivalidator on input/output xtf (required to import invalid files, invalid outputs are still generated)",
    )
    parser_qwat.add_argument("path", help="path to the input/output .xtf file")
    parser_qwat.add_argument(
        "--pgservice",
        help="name of the pgservice to use to connect to the database",
        default=config.QWAT_DEFAULT_PGSERVICE,
    )
    parser_qwat.add_argument(
        "--log",
        action="store_true",
        help="saves a log file next to the input/output file",
    )

    parser_tpl = subparsers.add_parser(
        "tpl", help="generate code templates [dev]", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser_tpl.add_argument("model", choices=["qgep", "qwat"])
    parser_tpl.add_argument(
        "--pgservice",
        help=f"name of the pgservice to use to connect to the database (defaults to {config.QGEP_DEFAULT_PGSERVICE} or {config.QWAT_DEFAULT_PGSERVICE})",
    )

    parser_setupdb = subparsers.add_parser(
        "setupdb", help="setup test db", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser_setupdb.set_defaults(parser="setupdb")
    parser_setupdb.add_argument("type", choices=["empty", "full"], help="type")

    args = parser.parse_args(args)

    if not args.parser:
        parser.print_help(sys.stderr)
        exit(1)

    # Set log path
    log_path = args.path if args.parser in ["qgep", "qwat"] and args.log else None

    # Write root logger to file
    filename = make_log_path(log_path, "qgepqwat2ili")
    file_handler = FileHandler(filename, mode="w", encoding="utf-8")
    file_handler.setLevel(INFO)
    file_handler.setFormatter(Formatter("%(levelname)-8s %(message)s"))
    utils.various.logger.addHandler(file_handler)
    
    # to do maybe move orientation_list to CONFIG ?
    orientation_list = [0.0, 90.0, -90.0]
    #if args.labels_orientation is None:
    #    args.labels_orientation = "0.0"
    # if args.labels_orientation in orientation_list or args.labels_orientation is None :
    # check if args.selection exists, if not set to "0.0"
# if args.labels_orientation:
        # print ("args.labels_orientation {args.labels_orientation}")
    # else:
        # #set args.selection to "0.0"
        # args.labels_orientation = "0.0"
    
    #if args.labels_orientation in orientation_list :
    if args.parser == "qgep":
        config.PGSERVICE = args.pgservice
        #SCHEMA = config.ABWASSER_SCHEMA
        #ILI_MODEL = config.ABWASSER_ILI_MODEL
        if args.export_sia405:
            SCHEMA = config.ABWASSER_SIA405_SCHEMA
            ILI_MODEL = config.ABWASSER_SIA405_ILI_MODEL
            ILI_MODEL_NAME = config.ABWASSER_ILI_MODEL_NAME_SIA405
            ILI_EXPORT_MODEL_NAME = config.ABWASSER_ILI_MODEL_NAME_SIA405
            #ABWASSER_SIA405_SCHEMA = config.ABWASSER_SIA405_SCHEMA
            #ABWASSER_SIA405_ILI_MODEL = config.ABWASSER_SIA405_ILI_MODEL
        # 24.3.2023 added dss export
        elif args.export_dss:
            SCHEMA = config.ABWASSER_DSS_SCHEMA
            ILI_MODEL = config.ABWASSER_SIA405_ILI_MODEL
            ILI_MODEL_NAME = config.ABWASSER_DSS_ILI_MODEL
            ILI_EXPORT_MODEL_NAME = None
            #ABWASSER_DSS_SCHEMA = config.ABWASSER_DSS_SCHEMA
            #ABWASSER_DSS_ILI_MODEL = config.ABWASSER_DSS_ILI_MODEL
        else:
            SCHEMA = config.ABWASSER_SCHEMA
            ILI_MODEL = config.ABWASSER_ILI_MODEL
            ILI_MODEL_NAME = config.ABWASSER_ILI_MODEL_NAME
            ILI_EXPORT_MODEL_NAME = None

        if args.direction == "export":
            utils.ili2db.create_ili_schema(
                SCHEMA, ILI_MODEL, make_log_path(log_path, "ilicreate"), recreate_schema=args.recreate_schema
            )
            #add model dependency
            if args.export_sia405:
                # SIA405_ABWASSER_2015_LV95
                qgepsia405_export(selection=args.selection.split(",") if args.selection else None, labels_file=args.labels_file, orientation=args.labels_orientation)
            elif args.export_dss:
                # DSS_2015_LV95 expor5t
                qgepdss_export(selection=args.selection.split(",") if args.selection else None, labels_file=args.labels_file, orientation=args.labels_orientation)
            else:
                # VSA_KEK_2019_LV95 export
 
                #qgep_export(selection=args.selection.split(",") if args.selection else None, labels_file=args.labels_file)
                # 4.4.2023 with labels_orientation
                #qgep_export(selection=args.selection.split(",") if args.selection else None, labels_file=args.labels_file, orientation=args.labels_orientation if args.labels_orientation else 0)
                qgep_export(selection=args.selection.split(",") if args.selection else None, labels_file=args.labels_file, orientation=args.labels_orientation)
            
            utils.ili2db.export_xtf_data(
                SCHEMA, ILI_MODEL_NAME, ILI_EXPORT_MODEL_NAME, args.path, make_log_path(log_path, "iliexport")
            )

            if not args.skip_validation:
                try:
                    utils.ili2db.validate_xtf_data(args.path, make_log_path(log_path, "ilivalidate"))
                except utils.various.CmdException:
                    print("Ilivalidator doesn't recognize output as valid ! Run with --skip_validation to ignore")
                    exit(1)

        elif args.direction == "import":
            if args.selection:
                print("Selection is only supported on export")
                exit(1)
            if args.labels_orientation:
                print("Labels_orientation is only supported on export")
                exit(1)
            if not args.skip_validation:
                try:
                    utils.ili2db.validate_xtf_data(args.path, make_log_path(log_path, "ilivalidate"))
                except utils.various.CmdException:
                    print("Ilivalidator doesn't recognize input as valid ! Run with --skip_validation to ignore")
                    exit(1)
            
            #add model dependency, as in __init_.py
            impmodel = "nothing"
            #impmodel = utils.ili2db.get_xtf_model(args.path)
            impmodel = utils.ili2db.get_xtf_model2(args.path)
            
            if impmodel == "VSA_KEK_2019_LV95":
                SCHEMA = config.ABWASSER_SCHEMA
                ILI_MODEL = config.ABWASSER_ILI_MODEL
                utils.ili2db.create_ili_schema(
                    SCHEMA, ILI_MODEL, make_log_path(log_path, "ilicreate"), recreate_schema=args.recreate_schema
                )
                utils.ili2db.import_xtf_data(SCHEMA, args.path, make_log_path(log_path, "iliimport"))
                print ("qgep_import: " + SCHEMA + "/" + ILI_MODEL)
                qgep_import()
            elif impmodel == "SIA405_ABWASSER_2015_LV95":
                ABWASSER_SIA405_SCHEMA = config.ABWASSER_SIA405_SCHEMA
                ABWASSER_SIA405_ILI_MODEL = config.ABWASSER_SIA405_ILI_MODEL
                utils.ili2db.create_ili_schema(
                    ABWASSER_SIA405_SCHEMA, ABWASSER_SIA405_ILI_MODEL, make_log_path(log_path, "ilicreate"), recreate_schema=args.recreate_schema
                )
                utils.ili2db.import_xtf_data(
                    ABWASSER_SIA405_SCHEMA, args.path, make_log_path(log_path, "iliimport")
                )
                print ("qgepsia405_import: " + ABWASSER_SIA405_SCHEMA + "/" + ABWASSER_SIA405_ILI_MODEL)
                qgepsia405_import()

            elif impmodel == "DSS_2015_LV95":
                ABWASSER_DSS_SCHEMA = config.ABWASSER_DSS_SCHEMA
                ABWASSER_DSS_ILI_MODEL = config.ABWASSER_DSS_ILI_MODEL
                utils.ili2db.create_ili_schema(
                    ABWASSER_DSS_SCHEMA, ABWASSER_DSS_ILI_MODEL, make_log_path(log_path, "ilicreate"), recreate_schema=args.recreate_schema
                )
                utils.ili2db.import_xtf_data(
                    ABWASSER_DSS_SCHEMA, args.path, make_log_path(log_path, "iliimport"))
                print ("qgepdss_import: " + ABWASSER_DSS_SCHEMA + "/" + ABWASSER_DSS_ILI_MODEL)
                qgepdss_import()

            else:
                print("MODEL " + impmodel + " schema creation failed: Not yet supported for INTERLIS import - no configuration available in config.py / _init_.py")



    elif args.parser == "qwat":
        config.PGSERVICE = args.pgservice
        SCHEMA = config.WASSER_SCHEMA
        ILI_MODEL = config.WASSER_ILI_MODEL
        ILI_MODEL_NAME = config.WASSER_ILI_MODEL_NAME
        ILI_EXPORT_MODEL_NAME = None

        if args.direction == "export":
            utils.ili2db.create_ili_schema(
                SCHEMA, ILI_MODEL, make_log_path(log_path, "ilicreate"), recreate_schema=args.recreate_schema
            )
            qwat_export(include_hydraulics=args.include_hydraulics)
            utils.ili2db.export_xtf_data(
                SCHEMA, ILI_MODEL_NAME, ILI_EXPORT_MODEL_NAME, args.path, make_log_path(log_path, "iliexport")
            )
            if not args.skip_validation:
                try:
                    utils.ili2db.validate_xtf_data(args.path, make_log_path(log_path, "ilivalidate"))
                except utils.various.CmdException:
                    print("Ilivalidator doesn't recognize output as valid ! Run with --skip_validation to ignore")
                    exit(1)

        elif args.direction == "import":
            if args.include_hydraulics:
                print("--include_hydraulics is only supported on export")
                exit(1)
            if not args.skip_validation:
                try:
                    utils.ili2db.validate_xtf_data(args.path, make_log_path(log_path, "ilivalidate"))
                except utils.various.CmdException:
                    print("Ilivalidator doesn't recognize input as valid ! Run with --skip_validation to ignore")
                    exit(1)
            utils.ili2db.create_ili_schema(
                SCHEMA, ILI_MODEL, make_log_path(log_path, "ilicreate"), recreate_schema=args.recreate_schema
            )
            utils.ili2db.import_xtf_data(SCHEMA, args.path, make_log_path(log_path, "iliimport"))
            qwat_import()

    elif args.parser == "tpl":
        config.PGSERVICE = args.pgservice

        if args.model == "qgep":
            if config.PGSERVICE is None:
                config.PGSERVICE = config.QGEP_DEFAULT_PGSERVICE
            #to do add model dependency
            utils.ili2db.create_ili_schema(config.ABWASSER_SCHEMA, config.ABWASSER_ILI_MODEL, recreate_schema=True)
            QGEPMAPPING = get_qgep_mapping()
            utils.templates.generate_template("qgep", "abwasser", BaseQgep, BaseAbwasser, QGEPMAPPING)

        elif args.model == "qwat":
            if config.PGSERVICE is None:
                config.PGSERVICE = config.QWAT_DEFAULT_PGSERVICE
            utils.ili2db.create_ili_schema(config.WASSER_SCHEMA, config.WASSER_ILI_MODEL, recreate_schema=True)
            QWATMAPPING = get_qwat_mapping()
            utils.templates.generate_template("qwat", "wasser", BaseQwat, BaseWasser, QWATMAPPING)

        else:
            print("Unknown model")
            exit(1)

    elif args.parser == "setupdb":
        utils.various.setup_test_db(args.type)

    else:
        print("Unknown operation")
        exit(1)
    # else:
            # # to do maybe read message from orientation_list
            # print("No valid value for labels_orientation: [0.0, 90.0, -90.0]")
            # exit(1)
    print("Operation completed sucessfully !")
