import urllib.request
import xml.etree.ElementTree as etree

'''
class example(dml.ALgorithm):
    contributor = 'tyrone_hou'
    reads = []
    writes = ['tyrone_hou.nextbus']

    @staticmethod
    def execute(trial = False):
        Retrieve some data sets (not using the API here for the sake of simplicity).
        startTime = datetime.datetime.now()
'''

#def execute(trial=False):

# Retrieve agency list???
agencytag = '<agency tag="mbta" title="MBTA" regionTitle="Massachusetts"/>'
agency = 'mbta'
base = 'http://webservices.nextbus.com/service/publicXMLFeed'

# Retrieve route list data
url = '{}?command={}&a={}'.format(base, 'routeList', agency)
response = urllib.request.urlopen(url).read().decode("utf-8")
routesList = etree.fromstring(response)

if(routesList[0].tag == 'Error'):
    print('Routes not correctly retrieved')
else:
    # Retrive route config data
    command = 'routeConfig'
    for child in [routesList[0]]:
        route = child.attrib
        url = '{}?command={}&a={}&r={}'.format(base, 'routeConfig', agency, route['tag'])
        response = urllib.request.urlopen(url).read().decode("utf-8")
        route_data = etree.fromstring(response)
