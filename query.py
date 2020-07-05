import sparql

# https://stackoverflow.com/questions/35569042/ssl-certificate-verify-failed-with-python3
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


def get_province_for(latitude, longitude):

    q = """
    PREFIX italy: <http://localhost:8000/italy.rdf#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>

    select ?closest_prov ?closest_region bif:st_distance(bif:st_point(""" + str(latitude) + ", " + str(longitude) + """), bif:st_point(?latitude, ?longitude)) as ?dist
    where {
        ?closest_prov rdf:type italy:Province ;
                      geo:lat ?latitude ;
                      geo:long ?longitude .
        ?closest_region italy:hasProvince ?closest_prov .
    } 
    order by ?dist
    limit 1
    """

    return sparql.unpack_row(sparql.query('http://localhost:8890/sparql', q).fetchall()[0])


def get_station_for(latitude, longitude):

    q = """
    PREFIX italy: <http://localhost:8000/italy.rdf#>
    PREFIX airbase: <http://reference.eionet.europa.eu/airbase/schema/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX pol: <http://localhost:8000/pollution.rdf#>
    PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>
    PREFIX sk: <http://www.w3.org/2004/02/skos/core#>

    select distinct(?stat) ?dist
    where {
        service <https://semantic.eea.europa.eu/sparql> {
            select distinct(?stat) bif:st_distance(bif:st_point(""" + str(latitude) + ", " + str(longitude) + """), bif:st_point(?s_lat, ?s_long)) as ?dist
            where {
                ?stat rdf:type airbase:Station ;
                      geo:lat ?s_lat ;
                      geo:long ?s_long ;
                      airbase:country ?nation .
                ?nation sk:notation ?nation_code .
                filter(?nation_code='IT') .
            }
        }

        ?blank airbase:Station ?stat ;
               pol:pollutant ?pollutant .
        ?pollutant pol:air_pollutant "PM10" . 
    }
    order by asc(?dist)
    limit 1
    """

    return sparql.unpack_row(sparql.query('http://localhost:8890/sparql', q).fetchall()[0])


def get_observations_for(province, station):
    q = """
    PREFIX italy: <http://localhost:8000/italy.rdf#>
    PREFIX obs: <http://localhost:8000/observation.rdf#>
    PREFIX dpc: <http://localhost:8000/dpc.rdf#>
    PREFIX pol: <http://localhost:8000/pollution.rdf#>
    PREFIX gm: <http://localhost:8000/google_mobility.rdf#>
    PREFIX am: <http://localhost:8000/apple_mobility.rdf#>
    PREFIX airbase: <http://reference.eionet.europa.eu/airbase/schema/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>
    PREFIX sk: <http://www.w3.org/2004/02/skos/core#>

    select ?date ?PM10 ?total_cases ?driving ?retail_recreation ?grocery_pharmacy ?parks ?transit_stations ?workplaces ?residential
    where {
        ?observation obs:date ?date ; 
                     obs:of ?p , 
                            ?r , 
                            ?s .

        ?p obs:place ?prov ;
           dpc:total_cases ?total_cases .
        ?prov italy:name ?province .
        filter (?prov = <""" + province + """>) .

        ?r obs:place ?reg .
        ?reg italy:name ?region ;
             italy:hasProvince ?prov .
        optional { 
            ?r am:driving ?driving ;
               gm:retail_recreation ?retail_recreation ;
               gm:grocery_pharmacy ?grocery_pharmacy ;
               gm:parks ?parks;
               gm:transit_stations ?transit_stations;
               gm:workplaces ?workplaces ;
               gm:residential ?residential .
        } 

        ?s airbase:Station <""" + station + """> ;
           pol:air_quality_station_eoi_code ?eoi_code .
        { 
            select ?m_observation ?m_s avg(?concentration) as ?PM10
            where {
                ?m_observation obs:of ?m_s .
                
                ?m_s pol:pollutant ?pollutant .
                ?pollutant pol:air_pollutant "PM10" ;
                           pol:measurement ?measurement .
                ?measurement pol:concentration ?concentration .

            } group by ?m_observation ?m_s
        }
        filter(?observation = ?m_observation) .
        filter(?s = ?m_s) .

    } order by asc(?date)
    """

    return sparql.query('http://localhost:8890/sparql', q)
