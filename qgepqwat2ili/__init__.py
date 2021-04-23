import argparse
import sys

from . import config, utils
from .qgep.export import qgep_export
from .qgep.import_ import qgep_import
from .qgep.mapping import get_qgep_mapping
from .qwat.export import qwat_export
from .qwat.import_ import qwat_import
from .qwat.mapping import get_qwat_mapping
from .qwat.model_qwat import Base as BaseQwat
from .qwat.model_wasser import Base as BaseWasser


def main(args):

    parser = argparse.ArgumentParser(description="ili2QWAT / ili2QGEP prototype entrypoint")
    subparsers = parser.add_subparsers(title="subcommands", dest="parser")
    # subparsers.required = True

    parser_qgep = subparsers.add_parser("qgep", help="import/export QGEP datamodel")
    # group = parser_qgep.add_mutually_exclusive_group(required=True)
    parser_qgep.add_argument("direction", choices=["import", "export"])
    parser_qgep.add_argument("--upstream_of", help="limit to network upstream of network element (id)")
    parser_qgep.add_argument("--downstream_of", help="limit to network downstream of network element (id)")
    parser_qgep.add_argument(
        "--recreate_schema", action="store_true", help="drops schema and reruns ili2pg importschema"
    )
    parser_qgep.add_argument(
        "--skip_validation",
        action="store_true",
        help="skips running ilivalidator on input/output xtf (required to import invalid files, invalid outputs are still generated)",
    )
    parser_qgep.add_argument("path", help="path to the input/output .xtf file")

    parser_qwat = subparsers.add_parser("qwat", help="import/export QWAT datamodel")
    parser_qwat.add_argument("direction", choices=["import", "export"])
    parser_qwat.add_argument(
        "--recreate_schema", action="store_true", help="drops schema and reruns ili2pg importschema"
    )
    parser_qwat.add_argument(
        "--skip_validation",
        action="store_true",
        help="skips running ilivalidator on input/output xtf (required to import invalid files, invalid outputs are still generated)",
    )
    parser_qwat.add_argument("path", help="path to the input/output .xtf file")

    parser_tpl = subparsers.add_parser("tpl", help="generate code templates [dev]")

    parser_setupdb = subparsers.add_parser("setupdb", help="setup test db")
    parser_setupdb.set_defaults(parser="setupdb")
    parser_setupdb.add_argument("type", choices=["empty", "full"], help="type")

    args = parser.parse_args(args)

    if not args.parser:
        parser.print_help(sys.stderr)
        exit(1)

    elif args.parser == "qgep":
        config.ABWASSER_SCHEMA
        config.ABWASSER_ILI_MODEL
        config.ABWASSER_ILI_MODEL_NAME
        if args.direction == "export":
            qgep_export(args.path, upstream_of=args.upstream_of, downstream_of=args.downstream_of)
            if not args.skip_validation:
                try:
                    utils.ili2db.validate_xtf_data(args.path)
                except utils.various.CmdException:
                    print("Ilivalidator doesn't recognize output as valid ! Run with --skip_validation to ignore")
                    exit(1)

        elif args.direction == "import":
            if args.upstream_of or args.downstream_of:
                print("Subnetwork is only supported on export")
                exit(1)
            if not args.skip_validation:
                try:
                    utils.ili2db.validate_xtf_data(args.path)
                except utils.various.CmdException:
                    print("Ilivalidator doesn't recognize input as valid ! Run with --skip_validation to ignore")
                    exit(1)
            qgep_import(args.path)

    elif args.parser == "qwat":
        config.WASSER_SCHEMA
        config.WASSER_ILI_MODEL
        config.WASSER_ILI_MODEL_NAME
        if args.direction == "export":
            qwat_export()
            if not args.skip_validation:
                try:
                    utils.ili2db.validate_xtf_data(args.path)
                except utils.various.CmdException:
                    print("Ilivalidator doesn't recognize output as valid ! Run with --skip_validation to ignore")
                    exit(1)

        elif args.direction == "import":
            if not args.skip_validation:
                try:
                    utils.ili2db.validate_xtf_data(args.path)
                except utils.various.CmdException:
                    print("Ilivalidator doesn't recognize input as valid ! Run with --skip_validation to ignore")
                    exit(1)
            qwat_import()

    elif args.parser == "tpl":
        utils.ili2db.create_ili_schema(config.WASSER_SCHEMA, config.WASSER_ILI_MODEL, recreate_schema=True)
        utils.ili2db.create_ili_schema(config.ABWASSER_SCHEMA, config.ABWASSER_ILI_MODEL, recreate_schema=True)

        QWATMAPPING = get_qwat_mapping()
        get_qgep_mapping()

        # utils.templates.generate_template("qgep", "abwasser", BaseQgep, BaseAbwasser, QGEPMAPPING)
        utils.templates.generate_template("qwat", "wasser", BaseQwat, BaseWasser, QWATMAPPING)

    elif args.parser == "setupdb":
        utils.various.setup_test_db(args.type)

    else:
        print("Unknown operation")
        exit(1)

    print("Operation completed sucessfully !")
