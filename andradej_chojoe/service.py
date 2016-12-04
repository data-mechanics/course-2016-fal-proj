import jsonschema
from flask import Flask, jsonify, abort, make_response, request, render_template
from flask.ext.httpauth import HTTPBasicAuth
from flask.ext.pymongo import PyMongo
from optimization import optimization
import dml

app = Flask(__name__)

app.config['MONGO_DBNAME'] = 'repo'
app.config['MONGO_USERNAME'] = 'andradej_chojoe'
app.config['MONGO_PASSWORD'] = 'andradej_chojoe'

mongo = PyMongo(app, config_prefix='MONGO')

auth = HTTPBasicAuth()

@app.route('/')
def index():
   return render_template('index.html')

@app.route('/bigbelly', methods=["GET"])
def get_big_belly():
	client = dml.pymongo.MongoClient()
	repo = client.repo
	repo.authenticate('andradej_chojoe', 'andradej_chojoe')

	bigbellyinfo = repo['andradej_chojoe.bigbelly_transf'].find()

	bigbelly_entries = []
	for data in bigbellyinfo:
		bigbelly_entries.append({ 'location': data['location'], 'count': data['count'], 'percentage': data['percentage']})

	return jsonify({'results': bigbelly_entries})

@app.route('/hotline', methods=["GET"])
def get_hotline():
	client = dml.pymongo.MongoClient()
	repo = client.repo
	repo.authenticate('andradej_chojoe', 'andradej_chojoe')

	hotline = repo['andradej_chojoe.hotline_transf'].find()

	hotline_entries = []
	for data in hotline:
		hotline_entries.append({ 'location': data['location'], 'count': data['count'], 'type': data['type']})

	return jsonify({'results': hotline_entries})

@app.route('/violations', methods=["GET"])
def get_violations():
	client = dml.pymongo.MongoClient()
	repo = client.repo
	repo.authenticate('andradej_chojoe', 'andradej_chojoe')

	violations = repo['andradej_chojoe.codeViol_transf'].find()

	violation_entries = []
	for data in violations:
		violation_entries.append({ 'location': data['location'], 'count': data['count'], 'type': data['type']})

	return jsonify({'results': violation_entries})

# @app.errorhandler(404)
# def not_found(error):
#     return make_response(jsonify({'error': 'Not found foo.'}), 404)

@app.route('/optimization/<num_trash>/<radius>', methods=["GET"])
def get_coordinates(num_trash, radius):
	print(num_trash)
	print(radius)
	coordinates = optimization.execute(int(num_trash), float(radius), True)
	coordinates = [{'location' : x['location'], 'weight': x['weight']} for x in coordinates]
	print(coordinates)
	return jsonify({'results': coordinates})

if __name__ == '__main__':
    app.run(debug=True)


