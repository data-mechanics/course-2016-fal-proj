#
# Functions that are used to obtain data for rendering in charts.
#
#

import pymongo

team_name = 'schiao_uvarovis'

lights_collection     = team_name + '.street_lights'
signals_collection    = team_name + '.traffic_signals'
accidents_collection  = team_name + '.car_accidents'
mbta_collection       = team_name + '.mbta_stops'
analysis_collection   = team_name + '.accidents_analysis'
districts_collection  = team_name + '.districts_overview'

def get_area_information(x, y):
    """ Returns information about the area near given point.
    """

    # create location object
    search_point = {
        "type": "Point",
        "coordinates": [float(x), float(y)]
    }

    # Set up the database connection.
    client = pymongo.MongoClient()
    repo = client.repo
    repo.authenticate(team_name, team_name)

    # distance from the point to search in
    max_distance = 300

    # build the dictionary
    info = {}

    info["num_lights"] = len(repo.command(
        'geoNear', lights_collection,
        near = search_point,
        spherical = True,
        maxDistance = max_distance)['results'])

    info["num_signals"] = len(repo.command(
        'geoNear', signals_collection,
        near = search_point,
        spherical = True,
        maxDistance = max_distance)['results'])

    info["num_mbta"] = len(repo.command(
        'geoNear', mbta_collection,
        near = search_point,
        spherical = True,
        maxDistance = max_distance)['results'])

    # Wrap up
    repo.logout()

    return info


def get_streetlights_data():
    """ Number of streetlights near car accidents.
    """
    return {
        "labels": ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"],
        "values": [778, 1180, 697, 450, 378, 170, 138, 102, 28, 9]
    }


def get_trafficsignals_data():
    """ Number of traffic signals near car accidents.
    """
    return {
        "labels": ["0", "1", "2", "3", "4", "5"],
        "values": [2380, 831, 325, 153, 239, 6]
    }


def get_districts_data():
    """ Information about districts.
    """
    # Set up the database connection.
    client = pymongo.MongoClient()
    repo = client.repo
    repo.authenticate(team_name, team_name)

    districts = []
    d_id = 0

    for doc in repo[districts_collection].find():
        district = {}
        d_id += 1
        district["id"] = str(d_id)
        district["num_accidents"] = doc["numOfAccidents"]
        district["num_signals"] = doc["numOfTrafficSignals"]
        district["num_mbta"] = doc["numOfMbtaStops"]
        district["num_lights"] = doc["numOfLights"]
        districts.append(district)

    repo.logout()

    return districts
