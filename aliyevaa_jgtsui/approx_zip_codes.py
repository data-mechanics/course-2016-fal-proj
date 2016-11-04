import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from scipy.spatial import Voronoi


class transformation1(dml.Algorithm):
    contributor = 'aliyevaa_jgtsui'
    reads = ['aliyevaa_jgtsui.wazeAlertsData', 'aliyevaa_jgtsui.crimeData']
    writes = ['aliyevaa_jgtsui.wazeAlertsData', 'aliyevaa_jgtsui.crimeData']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aliyevaa_jgtsui', 'aliyevaa_jgtsui')
	points = np.array([[0, 0], [0, 1], [0, 2], [1, 0], [1, 1], [1, 2])
	vor = Voronoi(points)
	
