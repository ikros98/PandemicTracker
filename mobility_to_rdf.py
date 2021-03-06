import urllib.parse
from urllib.request import urlretrieve, urlopen
from selenium import webdriver
from bs4 import BeautifulSoup
import pandas
from rdflib import Graph, Literal, Namespace, URIRef, BNode
from rdflib.namespace import RDF, RDFS, FOAF, OWL, XSD, DC, DCTERMS
import progressbar

# https://stackoverflow.com/questions/35569042/ssl-certificate-verify-failed-with-python3
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


def urify(string):
    return urllib.parse.quote_plus(string)


def get_apple_csv():
    # scraper to get the link to apple's csv

    op = webdriver.ChromeOptions()
    op.add_argument('headless')
    driver = webdriver.Chrome(
        '/usr/lib/chromium-browser/chromedriver', options=op)

    driver.get('https://www.apple.com/covid19/mobility')

    driver.implicitly_wait(5)
    link = driver.find_element_by_xpath('//*[@id="download-card"]/div[2]/a')
    href = link.get_attribute('href')

    driver.quit()

    return href


def get_google_csv():
    # scraper to get the link to google's csv

    url = 'https://www.google.com/covid19/mobility/'
    u = urlopen(url)

    try:
        html = u.read().decode('utf-8')
    finally:
        u.close()

    soup = BeautifulSoup(html, features="html.parser")

    link = soup.select_one('.icon-link')
    href = link.get('href')

    return href


# produced URIs will start with
BASE_URI = "http://localhost:8000/"
OUTPUT_NAME = "mobility_data"
OUTPUT_FORMAT = "ttl"

# download csv data from apple and google
# google_mobility_trends = pandas.read_csv(get_google_csv())
# apple_mobility_trends = pandas.read_csv(get_apple_csv())
google_mobility_trends = pandas.read_csv('google.csv')
apple_mobility_trends = pandas.read_csv('apple.csv')

g_it_regions = ['Friuli Venezia Giulia', 'Lombardia', 'Sicilia', 'Sardegna', 'Piemonte',
                'Valle d\'Aosta', 'Puglia', 'Toscana', 'Trentino Alto Adige']
g_en_regions = ['Friuli-Venezia Giulia', 'Lombardy', 'Sicily', 'Sardinia', 'Piedmont', 'Aosta',
                'Apulia', 'Tuscany', 'Trentino-South Tyrol']

a_it_regions = ['Friuli Venezia Giulia', 'Emilia-Romagna']
a_en_regions = ['Friuli venezia Giulia', 'Emilia Romagna']

# keep only italian data
google_mobility_trends = google_mobility_trends[google_mobility_trends['country_region_code'] == 'IT']
apple_mobility_trends = apple_mobility_trends[apple_mobility_trends['country'] == 'Italy']

# create an empty rdf graph and set the proper ontologies
g = Graph()
italy = Namespace(BASE_URI + "italy.ttl#")
obs = Namespace(BASE_URI + "observation.ttl#")
mob = Namespace(BASE_URI + "mobility.ttl#")
g.bind("mob", mob)
g.bind("obs", obs)
g.bind("italy", italy)

for _, google_row in progressbar.progressbar(google_mobility_trends.iterrows(), max_value=len(google_mobility_trends.index)):
    if pandas.isnull(google_row.sub_region_1):
        continue

    # translate google region names to italian
    region_name = google_row.sub_region_1
    if region_name in g_en_regions:
        region_name = g_it_regions[g_en_regions.index(google_row.sub_region_1)]

    # and then to apple's english version, so that we can put the two together
    apple_region_name = region_name
    if region_name in a_it_regions:
        apple_region_name = a_en_regions[a_it_regions.index(region_name)]

    # keep the apple row(s) with the same google region and date
    apple_rows = apple_mobility_trends.loc[(apple_mobility_trends['alternative_name'] == apple_region_name) |
                                           (apple_mobility_trends['region'] == apple_region_name + ' Region')]
    apple_rows = apple_rows[['transportation_type', google_row.date]]

    # create new instances
    uri_region = URIRef(BASE_URI + "region/" +
                        urify(region_name))

    uri_observation = URIRef(BASE_URI + "observation/" +
                             urify(google_row.date))

    g.add([uri_region, RDF.type, italy.Region])
    g.add([uri_observation, RDF.type, obs.Observation])

    # set data for the given observation
    blank = BNode()
    g.add([uri_observation, obs.date, Literal(google_row.date)])
    g.add([uri_observation, obs.of, blank])
    g.add([blank, RDF.type, mob.MobilityObservation])
    g.add([blank, mob.place, uri_region])

    g.add([blank, mob.retail_recreation, Literal(
        google_row.retail_and_recreation_percent_change_from_baseline)])
    g.add([blank, mob.grocery_pharmacy, Literal(
        google_row.grocery_and_pharmacy_percent_change_from_baseline)])
    g.add([blank, mob.parks, Literal(
        google_row.parks_percent_change_from_baseline)])
    g.add([blank, mob.transit_stations, Literal(
        google_row.transit_stations_percent_change_from_baseline)])
    g.add([blank, mob.workplaces, Literal(
        google_row.workplaces_percent_change_from_baseline)])
    g.add([blank, mob.residential, Literal(
        google_row.residential_percent_change_from_baseline)])

    apple_walking = apple_rows.loc[apple_rows['transportation_type'] == 'walking']
    apple_driving = apple_rows.loc[apple_rows['transportation_type'] == 'driving']
    apple_transit = apple_rows.loc[apple_rows['transportation_type'] == 'transit']

    if apple_walking.empty == False:
        g.add([blank, mob.walking, Literal(
            apple_walking[google_row.date].values[0])])
    if apple_driving.empty == False:
        g.add([blank, mob.driving, Literal(
            apple_driving[google_row.date].values[0])])
    if apple_transit.empty == False:
        g.add([blank, mob.transit, Literal(
            apple_transit[google_row.date].values[0])])

g.serialize(destination='./' + OUTPUT_NAME + '.' +
            OUTPUT_FORMAT, format=OUTPUT_FORMAT)
