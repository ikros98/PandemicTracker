import sparql
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


def urify(string):
    return urllib.parse.quote_plus(string)


# produced URIs will start with
BASE_URI = "http://localhost:8000/"
OUTPUT_NAME = "pollution"
OUTPUT_FORMAT = "nt"

# create an empty rdf graph and set the proper ontologies
g = Graph()
observation = Namespace(BASE_URI + "observation.rdf#")
pol = Namespace(BASE_URI + "pollution.rdf#")
uri_airbase = 'http://reference.eionet.europa.eu/airbase/'
airbase = Namespace(uri_airbase + 'schema/')
geo = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
g.bind("obs", observation)
g.bind("airbase", airbase)
g.bind("pol", pol)
g.bind("geo", geo)


def download_pollutants_for_station(station, eoi_code, latitude, longitude, sources):

    # store blank nodes for each date as { date: (station, [pollutant, ...]) }
    dates = {}

    for p in range(len(sources)):

        # download the csv containing this station's pollutant p
        s = sources[p]
        data = urllib.request.urlopen(s)
        data = pandas.read_csv(data)
        data = data.dropna().sort_values('DatetimeBegin')

        # will change when there is the need for new blank nodes
        last_date = None
        pollutant_blank = None
        for _, row in data.iterrows():

            date = str(pandas.to_datetime(row.DatetimeBegin).date())
            uri_observation = URIRef(BASE_URI + "observation/" +
                                     urify(date))
            if date != last_date:
                last_date = date

                # if this is the first time (across all pollutants) we reach this date,
                # we need to prepare new blank nodes
                if date not in dates:
                    dates[date] = (BNode(), [BNode()
                                             for p in range(len(sources))])

                # select the appropriate blank nodes
                station_blank = dates[date][0]
                pollutant_blank = dates[date][1][p]

                # create a new observation instance (if needed)
                g.add([uri_observation, RDF.type, observation.Observation])
                g.add([uri_observation, observation.date, Literal(date)])

                # set data for this station
                g.add([uri_observation, observation.of, station_blank])
                g.add([station_blank, airbase.Station, URIRef(station)])
                g.add([station_blank, geo.lat, Literal(latitude)])
                g.add([station_blank, geo.long, Literal(longitude)])
                g.add([station_blank, pol.namespace, Literal(row.Namespace)])
                g.add([station_blank, pol.air_quality_network,
                       Literal(row.AirQualityNetwork)])
                g.add([station_blank, pol.air_quality_station,
                       Literal(row.AirQualityStation)])
                g.add([station_blank, pol.air_quality_station_eoi_code,
                       Literal(row.AirQualityStationEoICode)])

                # set data for this pollutant
                g.add([station_blank, pol.pollutant, pollutant_blank])
                g.add([pollutant_blank, pol.sampling_point,
                       Literal(row.SamplingPoint)])
                g.add([pollutant_blank, pol.sampling_process,
                       Literal(row.SamplingProcess)])
                g.add([pollutant_blank, pol.sample, Literal(row.Sample)])
                g.add([pollutant_blank, pol.air_pollutant,
                       Literal(row.AirPollutant)])
                g.add([pollutant_blank, pol.air_pollutant_code,
                       URIRef(row.AirPollutantCode)])
                g.add([pollutant_blank, airbase.Components,
                       URIRef(uri_airbase + 'components/' + row.AirPollutantCode.rsplit('/', 1)[-1])])
                g.add([pollutant_blank, pol.unit_of_measurement,
                       Literal(row.UnitOfMeasurement)])

            # add a measurement to the current pollutant
            measurement_blank = BNode()
            begin_time = str(pandas.to_datetime(row.DatetimeBegin).time())
            end_time = str(pandas.to_datetime(row.DatetimeEnd).time())
            g.add([pollutant_blank, pol.measurement, measurement_blank])
            g.add([measurement_blank, pol.datetime_begin, Literal(begin_time)])
            g.add([measurement_blank, pol.datetime_end, Literal(end_time)])
            g.add([measurement_blank, pol.concentration,
                   Literal(row.Concentration)])


# api to download actual measurements, will be used with &Station parameter
API_URL = 'https://fme.discomap.eea.europa.eu/fmedatastreaming/AirQualityDownload/AQData_Extract.fmw?CountryCode=IT&Year_from=2020&Year_to=2020&Source=All&Output=TEXT&TimeCoverage=Year'

# sparql query to the european environment agency to obtain all stations in italy
results = sparql.query('https://semantic.eea.europa.eu/sparql', """
    PREFIX airbase: <http://reference.eionet.europa.eu/airbase/schema/>
    PREFIX sk: <http://www.w3.org/2004/02/skos/core#>
    PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>

    SELECT ?station ?lat ?long
    WHERE {
    ?station airbase:country ?nation ;
            geo:lat ?lat ;
            geo:long ?long .
    ?nation sk:notation ?nation_code .
    filter (?nation_code='IT') .
    } 
""").fetchall()

for row in progressbar.progressbar(results):
    station, latitude, longitude = sparql.unpack_row(row)
    eoi_code = station.rsplit('/', 1)[-1]

    # query the api for a particular station
    sources = urllib.request.urlopen(
        '{}&Station=STA.{}'.format(API_URL, eoi_code))

    # the api will return a list of csv files, one for each pollutant available
    sources = [line.decode('utf-8-sig').strip()
               for line in sources.readlines()]

    # analyze each source file and create the graph
    download_pollutants_for_station(
        station, eoi_code, latitude, longitude, sources)

g.serialize(destination='./' + OUTPUT_NAME + '.' +
            OUTPUT_FORMAT, format=OUTPUT_FORMAT)
