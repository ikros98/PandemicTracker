import sparql
import urllib.request
import urllib.parse
import pandas
from rdflib import Graph, Literal, Namespace, URIRef, BNode
from rdflib.namespace import RDF, RDFS, FOAF, OWL, XSD, DC, DCTERMS

# https://stackoverflow.com/questions/35569042/ssl-certificate-verify-failed-with-python3
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# per le uri che saranno create
BASE_URI = "http://localhost:8000/"
OUTPUT_NAME = "province"
OUTPUT_FORMAT = "nt"

# scarichiamo i dati completi in csv
dati_province = urllib.request.urlopen(
    'https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-province/dpc-covid19-ita-province-latest.csv')
dati_province = pandas.read_csv(dati_province)


def urify(string):
    return urllib.parse.quote_plus(string)


# creazione grafo rdf
g = Graph()
covid = Namespace(BASE_URI + "covid.rdf#")
italia = Namespace(BASE_URI + "italia.rdf#")
geo = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
g.bind("covid", covid)
g.bind("italia", italia)
g.bind("geo", geo)

for _, row in dati_province.iterrows():
    if pandas.isnull(row.sigla_provincia):
        continue

    # creazione istanze
    uri_provincia = URIRef(BASE_URI + "provincia/" +
                           urify(row.denominazione_provincia))
    uri_regione = URIRef(BASE_URI + "regione/" +
                         urify(row.denominazione_regione))
    uri_rilevazione = URIRef(BASE_URI + "rilevazione/" +
                             urify(row.data))

    g.add([uri_provincia, RDF.type, italia.Provincia])
    g.add([uri_regione, RDF.type, italia.Regione])
    g.add([uri_rilevazione, RDF.type, covid.Rilevazione])

    # dati per provincia
    g.add([uri_provincia, geo.lat, Literal(row.lat)])
    g.add([uri_provincia, geo.long, Literal(row.long)])
    g.add([uri_provincia, italia.nome, Literal(row.denominazione_provincia)])
    g.add([uri_provincia, italia.sigla, Literal(row.sigla_provincia)])
    g.add([uri_provincia, italia.codice, Literal(
        "{:03d}".format(row.codice_provincia))])

    # dati per regione
    g.add([uri_regione, italia.nome, Literal(row.denominazione_regione)])
    g.add([uri_regione, italia.haProvincia, uri_provincia])
    g.add([uri_regione, italia.codice, Literal(
        "{:02d}".format(row.codice_regione))])

    # dati per rilevazione
    blank = BNode()
    g.add([uri_rilevazione, covid.data, Literal(row.data)])
    g.add([uri_rilevazione, covid.su, blank])
    g.add([blank, covid.luogo, uri_provincia])
    g.add([blank, covid.totaleCasi, Literal(row.totale_casi)])

g.serialize(destination='./' + OUTPUT_NAME + '.' +
            OUTPUT_FORMAT, format=OUTPUT_FORMAT)
