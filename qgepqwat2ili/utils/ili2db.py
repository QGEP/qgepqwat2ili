from .. import config
from .various import exec_, logger


def validate_xtf_data(xtf_file):
    logger.info("VALIDATING XTF DATA...")
    exec_(f'"{config.JAVA}" -jar {config.ILIVALIDATOR} --modeldir {config.ILI_FOLDER} {xtf_file}')
