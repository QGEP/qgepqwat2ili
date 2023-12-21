from sqlalchemy.ext.automap import automap_base

from .. import config, utils

SCHEMA = config.ABWASSER_DSS_SCHEMA

Base = automap_base()


class baseclass(Base):
    __tablename__ = "baseclass"
    __table_args__ = {"schema": SCHEMA}


class sia405_baseclass(baseclass):
    __tablename__ = "sia405_baseclass"
    __table_args__ = {"schema": SCHEMA}

# 21.7.2022 / 22.7.2022 von hand () angepasst, kein sia405_baseclass -> baseclass
# 26.7.2022 baseclass geht auch nicht -> base
# class erhaltungsereignis_abwasserbauwerk(baseclass):
# leer lassen ()
class erhaltungsereignis_abwasserbauwerk():
    __tablename__ = "erhaltungsereignis_abwasserbauwerkassoc"
    __table_args__ = {"schema": SCHEMA}

class organisation(sia405_baseclass):
    __tablename__ = "organisation"
    __table_args__ = {"schema": SCHEMA}


class mutation(sia405_baseclass):
    __tablename__ = "mutation"
    __table_args__ = {"schema": SCHEMA}


class grundwasserleiter(sia405_baseclass):
    __tablename__ = "grundwasserleiter"
    __table_args__ = {"schema": SCHEMA}


class oberflaechengewaesser(sia405_baseclass):
    __tablename__ = "oberflaechengewaesser"
    __table_args__ = {"schema": SCHEMA}


class fliessgewaesser(oberflaechengewaesser):
    __tablename__ = "fliessgewaesser"
    __table_args__ = {"schema": SCHEMA}


class see(oberflaechengewaesser):
    __tablename__ = "see"
    __table_args__ = {"schema": SCHEMA}


class gewaesserabschnitt(sia405_baseclass):
    __tablename__ = "gewaesserabschnitt"
    __table_args__ = {"schema": SCHEMA}


class wasserfassung(sia405_baseclass):
    __tablename__ = "wasserfassung"
    __table_args__ = {"schema": SCHEMA}


class ufer(sia405_baseclass):
    __tablename__ = "ufer"
    __table_args__ = {"schema": SCHEMA}


class gewaessersohle(sia405_baseclass):
    __tablename__ = "gewaessersohle"
    __table_args__ = {"schema": SCHEMA}


class gewaessersektor(sia405_baseclass):
    __tablename__ = "gewaessersektor"
    __table_args__ = {"schema": SCHEMA}


class amt(organisation):
    __tablename__ = "amt"
    __table_args__ = {"schema": SCHEMA}


class genossenschaft_korporation(organisation):
    __tablename__ = "genossenschaft_korporation"
    __table_args__ = {"schema": SCHEMA}


class kanton(organisation):
    __tablename__ = "kanton"
    __table_args__ = {"schema": SCHEMA}


class abwasserverband(organisation):
    __tablename__ = "abwasserverband"
    __table_args__ = {"schema": SCHEMA}


class gemeinde(organisation):
    __tablename__ = "gemeinde"
    __table_args__ = {"schema": SCHEMA}


class abwasserreinigungsanlage(organisation):
    __tablename__ = "abwasserreinigungsanlage"
    __table_args__ = {"schema": SCHEMA}


class privat(organisation):
    __tablename__ = "privat"
    __table_args__ = {"schema": SCHEMA}


class abwasserbauwerk(sia405_baseclass):
    __tablename__ = "abwasserbauwerk"
    __table_args__ = {"schema": SCHEMA}


class kanal(abwasserbauwerk):
    __tablename__ = "kanal"
    __table_args__ = {"schema": SCHEMA}


class normschacht(abwasserbauwerk):
    __tablename__ = "normschacht"
    __table_args__ = {"schema": SCHEMA}


class einleitstelle(abwasserbauwerk):
    __tablename__ = "einleitstelle"
    __table_args__ = {"schema": SCHEMA}


class spezialbauwerk(abwasserbauwerk):
    __tablename__ = "spezialbauwerk"
    __table_args__ = {"schema": SCHEMA}


class versickerungsanlage(abwasserbauwerk):
    __tablename__ = "versickerungsanlage"
    __table_args__ = {"schema": SCHEMA}


class arabauwerk(abwasserbauwerk):
    __tablename__ = "arabauwerk"
    __table_args__ = {"schema": SCHEMA}


class erhaltungsereignis(sia405_baseclass):
    __tablename__ = "erhaltungsereignis"
    __table_args__ = {"schema": SCHEMA}

#18.7.2022 class zone(sia405_baseclass):
class azone(sia405_baseclass):
#    __tablename__ = "azone"
    __tablename__ = "azone"
    __table_args__ = {"schema": SCHEMA}


class planungszone(azone):
    __tablename__ = "planungszone"
    __table_args__ = {"schema": SCHEMA}


class versickerungsbereich(azone):
    __tablename__ = "versickerungsbereich"
    __table_args__ = {"schema": SCHEMA}


class entwaesserungssystem(azone):
    __tablename__ = "entwaesserungssystem"
    __table_args__ = {"schema": SCHEMA}


class gewaesserschutzbereich(azone):
    __tablename__ = "gewaesserschutzbereich"
    __table_args__ = {"schema": SCHEMA}


class grundwasserschutzareal(azone):
    __tablename__ = "grundwasserschutzareal"
    __table_args__ = {"schema": SCHEMA}


class grundwasserschutzzone(azone):
    __tablename__ = "grundwasserschutzzone"
    __table_args__ = {"schema": SCHEMA}


class rohrprofil(sia405_baseclass):
    __tablename__ = "rohrprofil"
    __table_args__ = {"schema": SCHEMA}


class araenergienutzung(sia405_baseclass):
    __tablename__ = "araenergienutzung"
    __table_args__ = {"schema": SCHEMA}


class abwasserbehandlung(sia405_baseclass):
    __tablename__ = "abwasserbehandlung"
    __table_args__ = {"schema": SCHEMA}


class schlammbehandlung(sia405_baseclass):
    __tablename__ = "schlammbehandlung"
    __table_args__ = {"schema": SCHEMA}


class steuerungszentrale(sia405_baseclass):
    __tablename__ = "steuerungszentrale"
    __table_args__ = {"schema": SCHEMA}


class gewaesserverbauung(sia405_baseclass):
    __tablename__ = "gewaesserverbauung"
    __table_args__ = {"schema": SCHEMA}


class furt(gewaesserverbauung):
    __tablename__ = "furt"
    __table_args__ = {"schema": SCHEMA}


class gewaesserabsturz(gewaesserverbauung):
    __tablename__ = "gewaesserabsturz"
    __table_args__ = {"schema": SCHEMA}


class schleuse(gewaesserverbauung):
    __tablename__ = "schleuse"
    __table_args__ = {"schema": SCHEMA}


class durchlass(gewaesserverbauung):
    __tablename__ = "durchlass"
    __table_args__ = {"schema": SCHEMA}


class geschiebesperre(gewaesserverbauung):
    __tablename__ = "geschiebesperre"
    __table_args__ = {"schema": SCHEMA}


class gewaesserwehr(gewaesserverbauung):
    __tablename__ = "gewaesserwehr"
    __table_args__ = {"schema": SCHEMA}


class sohlrampe(gewaesserverbauung):
    __tablename__ = "sohlrampe"
    __table_args__ = {"schema": SCHEMA}


class fischpass(sia405_baseclass):
    __tablename__ = "fischpass"
    __table_args__ = {"schema": SCHEMA}


class badestelle(sia405_baseclass):
    __tablename__ = "badestelle"
    __table_args__ = {"schema": SCHEMA}


class hydr_geometrie(sia405_baseclass):
    __tablename__ = "hydr_geometrie"
    __table_args__ = {"schema": SCHEMA}


class abwassernetzelement(sia405_baseclass):
    __tablename__ = "abwassernetzelement"
    __table_args__ = {"schema": SCHEMA}


class haltungspunkt(sia405_baseclass):
    __tablename__ = "haltungspunkt"
    __table_args__ = {"schema": SCHEMA}


class abwasserknoten(abwassernetzelement):
    __tablename__ = "abwasserknoten"
    __table_args__ = {"schema": SCHEMA}


class haltung(abwassernetzelement):
    __tablename__ = "haltung"
    __table_args__ = {"schema": SCHEMA}


class rohrprofil_geometrie(sia405_baseclass):
    __tablename__ = "rohrprofil_geometrie"
    __table_args__ = {"schema": SCHEMA}


class hydr_geomrelation(sia405_baseclass):
    __tablename__ = "hydr_geomrelation"
    __table_args__ = {"schema": SCHEMA}


class mechanischevorreinigung(sia405_baseclass):
    __tablename__ = "mechanischevorreinigung"
    __table_args__ = {"schema": SCHEMA}


class retentionskoerper(sia405_baseclass):
    __tablename__ = "retentionskoerper"
    __table_args__ = {"schema": SCHEMA}


class ueberlaufcharakteristik(sia405_baseclass):
    __tablename__ = "ueberlaufcharakteristik"
    __table_args__ = {"schema": SCHEMA}


class hq_relation(sia405_baseclass):
    __tablename__ = "hq_relation"
    __table_args__ = {"schema": SCHEMA}


class bauwerksteil(sia405_baseclass):
    __tablename__ = "bauwerksteil"
    __table_args__ = {"schema": SCHEMA}


class trockenwetterfallrohr(bauwerksteil):
    __tablename__ = "trockenwetterfallrohr"
    __table_args__ = {"schema": SCHEMA}


class einstiegshilfe(bauwerksteil):
    __tablename__ = "einstiegshilfe"
    __table_args__ = {"schema": SCHEMA}


class trockenwetterrinne(bauwerksteil):
    __tablename__ = "trockenwetterrinne"
    __table_args__ = {"schema": SCHEMA}


class deckel(bauwerksteil):
    __tablename__ = "deckel"
    __table_args__ = {"schema": SCHEMA}


class elektrischeeinrichtung(bauwerksteil):
    __tablename__ = "elektrischeeinrichtung"
    __table_args__ = {"schema": SCHEMA}


class elektromechanischeausruestung(bauwerksteil):
    __tablename__ = "elektromechanischeausruestung"
    __table_args__ = {"schema": SCHEMA}


class bankett(bauwerksteil):
    __tablename__ = "bankett"
    __table_args__ = {"schema": SCHEMA}


class anschlussobjekt(sia405_baseclass):
    __tablename__ = "anschlussobjekt"
    __table_args__ = {"schema": SCHEMA}


class gebaeude(anschlussobjekt):
    __tablename__ = "gebaeude"
    __table_args__ = {"schema": SCHEMA}


class reservoir(anschlussobjekt):
    __tablename__ = "reservoir"
    __table_args__ = {"schema": SCHEMA}


class einzelflaeche(anschlussobjekt):
    __tablename__ = "einzelflaeche"
    __table_args__ = {"schema": SCHEMA}


class brunnen(anschlussobjekt):
    __tablename__ = "brunnen"
    __table_args__ = {"schema": SCHEMA}


class gefahrenquelle(sia405_baseclass):
    __tablename__ = "gefahrenquelle"
    __table_args__ = {"schema": SCHEMA}


class unfall(sia405_baseclass):
    __tablename__ = "unfall"
    __table_args__ = {"schema": SCHEMA}


class stoff(sia405_baseclass):
    __tablename__ = "stoff"
    __table_args__ = {"schema": SCHEMA}


class einzugsgebiet(sia405_baseclass):
    __tablename__ = "einzugsgebiet"
    __table_args__ = {"schema": SCHEMA}


class oberflaechenabflussparameter(sia405_baseclass):
    __tablename__ = "oberflaechenabflussparameter"
    __table_args__ = {"schema": SCHEMA}


class messstelle(sia405_baseclass):
    __tablename__ = "messstelle"
    __table_args__ = {"schema": SCHEMA}


class messgeraet(sia405_baseclass):
    __tablename__ = "messgeraet"
    __table_args__ = {"schema": SCHEMA}


class messreihe(sia405_baseclass):
    __tablename__ = "messreihe"
    __table_args__ = {"schema": SCHEMA}


class messresultat(sia405_baseclass):
    __tablename__ = "messresultat"
    __table_args__ = {"schema": SCHEMA}


class ueberlauf(sia405_baseclass):
    __tablename__ = "ueberlauf"
    __table_args__ = {"schema": SCHEMA}


class absperr_drosselorgan(sia405_baseclass):
    __tablename__ = "absperr_drosselorgan"
    __table_args__ = {"schema": SCHEMA}


class streichwehr(ueberlauf):
    __tablename__ = "streichwehr"
    __table_args__ = {"schema": SCHEMA}


class foerderaggregat(ueberlauf):
    __tablename__ = "foerderaggregat"
    __table_args__ = {"schema": SCHEMA}


class leapingwehr(ueberlauf):
    __tablename__ = "leapingwehr"
    __table_args__ = {"schema": SCHEMA}


class hydr_kennwerte(sia405_baseclass):
    __tablename__ = "hydr_kennwerte"
    __table_args__ = {"schema": SCHEMA}


class rueckstausicherung(bauwerksteil):
    __tablename__ = "rueckstausicherung"
    __table_args__ = {"schema": SCHEMA}


class feststoffrueckhalt(bauwerksteil):
    __tablename__ = "feststoffrueckhalt"
    __table_args__ = {"schema": SCHEMA}


class beckenreinigung(bauwerksteil):
    __tablename__ = "beckenreinigung"
    __table_args__ = {"schema": SCHEMA}


class beckenentleerung(bauwerksteil):
    __tablename__ = "beckenentleerung"
    __table_args__ = {"schema": SCHEMA}


class ezg_parameter_allg(oberflaechenabflussparameter):
    __tablename__ = "ezg_parameter_allg"
    __table_args__ = {"schema": SCHEMA}


class ezg_parameter_mouse1(oberflaechenabflussparameter):
    __tablename__ = "ezg_parameter_mouse1"
    __table_args__ = {"schema": SCHEMA}


# STRUCTS


class metaattribute(Base):
    __tablename__ = "metaattribute"
    __table_args__ = {"schema": SCHEMA}


# TEXTS


class textpos(baseclass):
    __tablename__ = "textpos"
    __table_args__ = {"schema": SCHEMA}


class sia405_textpos(textpos):
    __tablename__ = "sia405_textpos"
    __table_args__ = {"schema": SCHEMA}


class haltung_text(sia405_textpos):
    __tablename__ = "haltung_text"
    __table_args__ = {"schema": SCHEMA}


class abwasserbauwerk_text(sia405_textpos):
    __tablename__ = "abwasserbauwerk_text"
    __table_args__ = {"schema": SCHEMA}


_prepared = False


def get_abwasser_model():
    global _prepared
    if not _prepared:
        utils.sqlalchemy.prepare_automap_base(Base, SCHEMA)
        _prepared = True
    return Base.classes
