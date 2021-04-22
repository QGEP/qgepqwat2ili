# QGEP2ili / QWAT2ili

This is the toolkit to import/export between QGEP/QWAT and their counterpart SIA405 interlis model exchange files.

It can be used as a command line tool or integrated in QGIS plugins.

## Command line tool

### Import/export QGEP

Import example
```
python -m qgepqwat2ili qgep import data/test_without_abwasserbauwerkref.xtf
```

Export example
```
python -m qgepqwat2ili qgep export desktop/my_export.xtf --upstream_of "WN00000"
```

Full usage
```
usage: python -m qgepqwat2ili qgep [-h] [--upstream_of UPSTREAM_OF] [--downstream_of DOWNSTREAM_OF] [--recreate_schema] [--skip_validation] {import,export} path

positional arguments:
  {import,export}
  path                  path to the input/output .xtf file

optional arguments:
  -h, --help            show this help message and exit
  --upstream_of UPSTREAM_OF
                        limit to network upstream of network element (id)
  --downstream_of DOWNSTREAM_OF
                        limit to network downstream of network element (id)
  --recreate_schema     drops schema and reruns ili2pg importschema
  --skip_validation     skips running ilivalidator on input/output xtf (required to import invalid files, invalid outputs are still generated)
```

### Import/export QWAT

Import example
```
python -m qgepqwat2ili qgep import data/test_without_abwasserbauwerkref.xtf
```

Export example
```
python -m qgepqwat2ili qgep export desktop/my_export.xtf --upstream_of "WN00000"
```

Full usage
```
usage: python -m qgepqwat2ili qwat [-h] [--recreate_schema] [--skip_validation] {import,export} path

positional arguments:
  {import,export}
  path               path to the input/output .xtf file

optional arguments:
  -h, --help         show this help message and exit
  --recreate_schema  drops schema and reruns ili2pg importschema
  --skip_validation  skips running ilivalidator on input/output xtf (required to import invalid files, invalid outputs are still generated)
```


## QGIS integration

This will be integrated into the official QGEP and QWAT plugin installable through the QGIS plugin manager.


## Dev

Import/export scripts templates can be generated using `python -m qgepqwat2ili tpl`. This uses the mapping defined in `datamodels/mapping.py` to auto-generate import script templates, that can then be manually merged into the existing scripts.

Tests are run with 
```
python -m unittest qgepqwat2ili.tests
```
