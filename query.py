import sparql

# https://stackoverflow.com/questions/35569042/ssl-certificate-verify-failed-with-python3
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


def get_province_for_location(latitude, longitude):

    q = """
    PREFIX italy: <http://localhost:8000/italy.rdf#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>

    select ?closest_prov bif:st_distance(bif:st_point(""" + str(latitude) + ", " + str(longitude) + """), bif:st_point(?latitude, ?longitude)) as ?dist
    where {
        ?closest_prov rdf:type italy:Province ;
                      geo:lat ?latitude ;
                      geo:long ?longitude .
    } 
    order by ?dist
    limit 1
    """

    return sparql.unpack_row(sparql.query('http://localhost:8890/sparql', q).fetchall()[0])


def get_station_for_location(latitude, longitude):

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
               pol:pollutant ?pol_NO2 ;
               pol:pollutant ?pol_O3 .
        ?pol_NO2 pol:air_pollutant "NO2" . 
        ?pol_O3 pol:air_pollutant "O3" .
    }
    order by asc(?dist)
    limit 1
    """

    return sparql.unpack_row(sparql.query('http://localhost:8890/sparql', q).fetchall()[0])


def get_observations_for_province_station(province, station):
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

    select ?date ?NO2 ?O3 ?total_cases ?driving ?retail_recreation ?grocery_pharmacy ?parks ?transit_stations ?workplaces ?residential
    where {
        ?observation obs:date ?date . 

        ?observation obs:of ?p, ?r, ?s.
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
            select ?m_observation ?m_s avg(?concentration_NO2) as ?NO2 
            where {
                ?m_observation obs:of ?m_s .
                
                ?m_s pol:pollutant ?pol_NO2 .
                ?pol_NO2 pol:air_pollutant "NO2" ;
                         pol:measurement ?measurement_NO2 .
                ?measurement_NO2 pol:concentration ?concentration_NO2 .

            } group by ?m_observation ?m_s
        }
        filter(?observation = ?m_observation) .
        filter(?s = ?m_s) .

        { 
            select ?m_observation ?m_s avg(?concentration_O3) as ?O3
            where {
                ?m_observation obs:of ?m_s .
                
                ?m_s pol:pollutant ?pol_O3 .
                ?pol_O3 pol:air_pollutant "O3" ;
                         pol:measurement ?measurement_O3 .
                ?measurement_O3 pol:concentration ?concentration_O3 .

            } group by ?m_observation ?m_s
        }
        filter(?observation = ?m_observation) .
        filter(?s = ?m_s) .


    } order by asc(?date)
    """

    return sparql.query('http://localhost:8890/sparql', q)


def main():
    province = get_province_for_location(43, 16)[0]
    station = get_station_for_location(43, 16)[0]
    results = get_observations_for_province_station(province, station)
    print(results.variables)
    for row in results.fetchall():
        print(sparql.unpack_row(row))


if __name__ == "__main__":
    main()
