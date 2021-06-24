# QGEP2ili / QWAT2ili

This is the toolkit to import/export between QGEP/QWAT and their counterpart SIA405 interlis model exchange files.

It can be used as a command line tool or integrated in QGIS plugins.

## Command line tool

### Installation

The tool requires **python** (3.6 or newer) and **java** to be available. Then, it can be installed like any other python library:
```
pip install --upgrade qgepqwat2ili
```

### Import/export QGEP

Import example
```
python -m qgepqwat2ili qgep import data/test_without_abwasserbauwerkref.xtf
```

Export example
```
python -m qgepqwat2ili qgep export desktop/my_export.xtf --selection "ch13p7mzRE001221,ch13p7mzWN003445,ch13p7mzWN008122"
```

Full usage
```
usage: python -m qgepqwat2ili qgep [-h] [--selection SELECTION] [--recreate_schema] [--skip_validation] [--pgservice PGSERVICE] {import,export} path

ili2QGEP entrypoint

positional arguments:
  {import,export}
  path                  path to the input/output .xtf file

optional arguments:
  -h, --help            show this help message and exit
  --selection SELECTION
                        if provided, limits the export to networkelements that are provided in the selection (comma separated list of ids)
                        (default: None)
  --recreate_schema     drops schema and reruns ili2pg importschema (default: False)
  --skip_validation     skips running ilivalidator on input/output xtf (required to import invalid files, invalid outputs are still generated)
                        (default: False)
  --pgservice PGSERVICE
                        name of the pgservice to use to connect to the database (default: pg_qgep)
```

### Import/export QWAT

Import example
```
# QWAT import not implemented yet
```

Export example
```
python -m qgepqwat2ili qwat export desktop/my_export.xtf
```

Full usage
```
usage: python -m qgepqwat2ili qwat [-h] [--recreate_schema] [--skip_validation] [--pgservice PGSERVICE] {import,export} path

ili2QWAT entrypoint

positional arguments:
  {import,export}
  path                  path to the input/output .xtf file

optional arguments:
  -h, --help            show this help message and exit
  --recreate_schema     drops schema and reruns ili2pg importschema (default: False)
  --skip_validation     skips running ilivalidator on input/output xtf (required to import invalid files, invalid outputs are still generated)
                        (default: False)
  --pgservice PGSERVICE
                        name of the pgservice to use to connect to the database (default: qwat)
```

### Logging

Logging output to a file can be achieved by redirecting stdout to a file :
```
python -m qgepqwat2ili qgep import data/test_without_abwasserbauwerkref.xtf > output.log
```

Please include this when reporting issues.

Additionally, ili2db and ilivalidator related commands will create a log file named `ili2qgepqwat-yyyy-mm-dd-hh-mm-ss-STEP.log` in your temporary directory (`/tmp` on linux or `C:\Users\You\AppData\Local\Temp\` on Windows).


## QGIS integration

This will be integrated into the official QGEP and QWAT plugin installable through the QGIS plugin manager.


## Dev

Import/export scripts templates can be generated using `python -m qgepqwat2ili tpl`. This uses the mapping defined in `datamodels/mapping.py` to auto-generate import script templates, that can then be manually merged into the existing scripts.

Tests are run with
```
python -m unittest qgepqwat2ili.tests
```

Style is done with pre-commit
```
pip install pre-commit
pre-commit install
```

## Releases

Releases to PyPi are made automatically via github workflows whenever a new tag matching `v*` is pushed.
