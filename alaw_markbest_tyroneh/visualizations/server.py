from flask import Flask
from flask import request
from flask import jsonify
from flask import render_template
import json

app = Flask(__name__)

@app.route('/')
def index():
	return 'hello world! ;)'

@app.route('/routeScores')
def getRouteScores():
	'''returns json object of all allocation scores per route'''
	routeScores = json.load(open('../jsons/routeScores.json','r'))
	return jsonify(routeScores)

@app.route('/allocationScores')
def getAllocationScores():
	'''returns json object of all allocation scores per route'''
	allocationScores = json.load(open('../jsons/allocationScores.json','r'))
	return jsonify(allocationScores)

@app.route('/routes')
def getRoutes():
	routes = json.load(open('../jsons/routesGeoJSONs.json', 'r'))
	return jsonify(routes)

@app.route('/allocation', methods=['GET'])
def loadAllocation():
	'''loads the visualization for bus allocation'''

	return render_template('allocation.html')

@app.route('/kmeans', methods=['GET'])
def loadKMeans():
	'''loads the visualization for bus allocation'''

	return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
