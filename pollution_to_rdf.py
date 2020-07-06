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
OUTPUT_NAME = "pollution_data"
OUTPUT_FORMAT = "ttl"

# create an empty rdf graph and set the proper ontologies
g = Graph()
obs = Namespace(BASE_URI + "observation.ttl#")
pol = Namespace(BASE_URI + "pollution.ttl#")
uri_airbase = 'http://reference.eionet.europa.eu/airbase/'
airbase = Namespace(uri_airbase + 'schema/')
geo = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
g.bind("obs", obs)
g.bind("airbase", airbase)
g.bind("pol", pol)
g.bind("geo", geo)
g.bind("owl", OWL)


def download_pollutants_for_station(station, eoi_code, sources):

    # store blank nodes for each date as { date: (station, [pollutant, ...]) }
    dates = {}

    # create station instance
    uri_station = URIRef(BASE_URI + "station/" +
                         urify(eoi_code))
    g.add([uri_station, RDF.type, pol.Station])
    g.add([uri_station, OWL.sameAs, URIRef(station)])
    station_data_added = False

    for p in range(len(sources)):

        # download the csv containing this station's pollutant p
        s = sources[p]
        data = urllib.request.urlopen(s)
        data = pandas.read_csv(data)
        data = data.dropna().sort_values('DatetimeBegin')

        # station and pollutant data can be read from the first row of the file (every row contains the same data)

        # create pollutant instance
        uri_pollutant = URIRef(BASE_URI + "pollutant/" +
                               urify(data["AirPollutant"].iloc[0]))
        g.add([uri_pollutant, RDF.type, pol.Pollutant])
        g.add([uri_pollutant, OWL.sameAs, URIRef(uri_airbase + 'components/' +
                                                 data["AirPollutantCode"].iloc[0].rsplit('/', 1)[-1])])

        # add pollutant data
        g.add([uri_pollutant, pol.air_pollutant,
               Literal(data["AirPollutant"].iloc[0])])
        g.add([uri_pollutant, pol.air_pollutant_code,
               URIRef(data["AirPollutantCode"].iloc[0])])
        g.add([uri_pollutant, pol.unit_of_measurement,
               Literal(data["UnitOfMeasurement"].iloc[0])])

        # add station-specific pollutant data (no measurements yet)
        b_station_pollutant = BNode()
        g.add([uri_station, pol.measures, b_station_pollutant])
        g.add([b_station_pollutant, RDF.type, pol.StationPollutant])
        g.add([b_station_pollutant, pol.sampling_point,
               Literal(data["SamplingPoint"].iloc[0])])
        g.add([b_station_pollutant, pol.sampling_process,
               Literal(data["SamplingProcess"].iloc[0])])
        g.add([b_station_pollutant, pol.sample, Literal(data["Sample"].iloc[0])])
        g.add([b_station_pollutant, pol.pollutant, uri_pollutant])

        # add station data (once per station)
        if not station_data_added:
            station_data_added = True
            g.add([uri_station, pol.namespace,
                   Literal(data["Namespace"].iloc[0])])
            g.add([uri_station, pol.air_quality_network,
                   Literal(data["AirQualityNetwork"].iloc[0])])
            g.add([uri_station, pol.air_quality_station,
                   Literal(data["AirQualityStation"].iloc[0])])
            g.add([uri_station, pol.air_quality_station_eoi_code,
                   Literal(data["AirQualityStationEoICode"].iloc[0])])

        # will change when there is the need for new blank nodes
        last_date = None
        b_pollutant_observation = None
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
                b_pollution_observation = dates[date][0]
                b_pollutant_observation = dates[date][1][p]

                # create a new observation instance
                g.add([uri_observation, RDF.type, obs.Observation])
                g.add([uri_observation, obs.date, Literal(date)])

                # set relation to this station
                g.add([uri_observation, obs.of, b_pollution_observation])
                g.add([b_pollution_observation, RDF.type, pol.PollutionObservation])
                g.add([b_pollution_observation, pol.station, uri_station])

                # set relation to this pollutant
                g.add([b_pollution_observation,
                       pol.observing, b_pollutant_observation])
                g.add([b_pollutant_observation, RDF.type, pol.PollutantObservation])
                g.add([b_pollutant_observation, pol.pollutant, uri_pollutant])

            # add a measurement
            b_measurement = BNode()
            g.add([b_pollutant_observation, pol.pollutant_measurement, b_measurement])
            g.add([b_measurement, RDF.type, pol.PollutantMeasurement])
            g.add([b_measurement, pol.datetime_begin, Literal(row.DatetimeBegin)])
            g.add([b_measurement, pol.datetime_end, Literal(row.DatetimeEnd)])
            g.add([b_measurement, pol.concentration, Literal(row.Concentration)])


# api to download actual measurements, will be used with &Station parameter
API_URL = 'https://fme.discomap.eea.europa.eu/fmedatastreaming/AirQualityDownload/AQData_Extract.fmw?CountryCode=IT&Year_from=2020&Year_to=2020&Source=All&Output=TEXT&TimeCoverage=Year'

# sparql query to the european environment agency to obtain all stations in italy
results = sparql.query('https://semantic.eea.europa.eu/sparql', """
    PREFIX airbase: <http://reference.eionet.europa.eu/airbase/schema/>
    PREFIX sk: <http://www.w3.org/2004/02/skos/core#>
    PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>

    SELECT ?station ?eoi_code
    WHERE {
       ?station airbase:country ?nation ;
                airbase:station_european_code ?eoi_code .
       ?nation sk:notation ?nation_code .
       filter (?nation_code='IT') .
    }
""").fetchall()

for row in progressbar.progressbar(results):
    station, eoi_code = sparql.unpack_row(row)

    # query the api for a particular station
    sources = urllib.request.urlopen(
        '{}&Pollutant=5&Station=STA.{}'.format(API_URL, eoi_code))

    # the api will return a list of csv files, one for each pollutant available
    sources = [line.decode('utf-8-sig').strip()
               for line in sources.readlines()]

    # analyze each source file and create the graph
    download_pollutants_for_station(station, eoi_code, sources)

g.serialize(destination='./' + OUTPUT_NAME + '.' +
            OUTPUT_FORMAT, format=OUTPUT_FORMAT)
