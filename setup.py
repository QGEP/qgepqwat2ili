from distutils.core import setup

setup(
    name="qgepqwat2ili",
    version="__dev__",  # DON'T CHANGE, WILL BE OVERRIDEN BY CI
    description="Toolkit to import/export between QGEP/QWAT and their counterpart SIA405 interlis model exchange files",
    author="Olivier Dalang",
    author_email="olivier@opengis.ch",
    url="https://github.com/QGEP/qgepqwat2ili",
    packages=["qgepqwat2ili"],
    install_requires=open("requirements.txt", encoding="utf-8").read().splitlines(),
)
