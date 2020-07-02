import urllib.request
import urllib.parse
import pandas
from datetime import datetime
from rdflib import Graph, Literal, Namespace, URIRef, BNode
from rdflib.namespace import RDF, RDFS, FOAF, OWL, XSD, DC, DCTERMS
import progressbar

# https://stackoverflow.com/questions/35569042/ssl-certificate-verify-failed-with-python3
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# produced URIs will start with
BASE_URI = "http://localhost:8000/"
OUTPUT_NAME = "dpc"
OUTPUT_FORMAT = "nt"

# download csv data from the italian dpc
province_data = urllib.request.urlopen(
    'https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-province/dpc-covid19-ita-province-latest.csv')
province_data = pandas.read_csv(province_data)


def urify(string):
    return urllib.parse.quote_plus(string)


# create an empty rdf graph and set the proper ontologies
g = Graph()
dpc = Namespace(BASE_URI + "dpc.rdf#")
italy = Namespace(BASE_URI + "italy.rdf#")
observation = Namespace(BASE_URI + "observation.rdf#")
geo = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
g.bind("dpc", dpc)
g.bind("obs", observation)
g.bind("italy", italy)
g.bind("geo", geo)

for _, row in progressbar.progressbar(province_data.iterrows(), max_value=len(province_data.index)):
    if pandas.isnull(row.sigla_provincia):
        continue

    # create new instances
    date = str(pandas.to_datetime(row.data).date())
    uri_province = URIRef(BASE_URI + "province/" +
                          urify(row.denominazione_provincia))
    uri_region = URIRef(BASE_URI + "region/" +
                        urify(row.denominazione_regione))
    uri_observation = URIRef(BASE_URI + "observation/" +
                             urify(date))

    g.add([uri_province, RDF.type, italy.Province])
    g.add([uri_region, RDF.type, italy.Region])
    g.add([uri_observation, RDF.type, observation.Observation])

    # set data for the given province
    g.add([uri_province, geo.lat, Literal(row.lat)])
    g.add([uri_province, geo.long, Literal(row.long)])
    g.add([uri_province, italy.name, Literal(row.denominazione_provincia)])
    g.add([uri_province, italy.short_name, Literal(row.sigla_provincia)])
    g.add([uri_province, italy.code, Literal(
        "{:03d}".format(row.codice_provincia))])

    # set data for the given region
    g.add([uri_region, italy.name, Literal(row.denominazione_regione)])
    g.add([uri_region, italy.hasProvince, uri_province])
    g.add([uri_region, italy.code, Literal(
        "{:02d}".format(row.codice_regione))])

    # set data for the given observation
    blank = BNode()
    g.add([uri_observation, observation.date, Literal(date)])
    g.add([uri_observation, observation.about, blank])
    g.add([blank, observation.place, uri_province])
    g.add([blank, dpc.total_cases, Literal(row.totale_casi)])

g.serialize(destination='./' + OUTPUT_NAME + '.' +
            OUTPUT_FORMAT, format=OUTPUT_FORMAT)
