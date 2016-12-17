import flask
from flask import Flask, Response, request, render_template, redirect, url_for
from flask import jsonify
from flask import request
from flask_pymongo import PyMongo
from jinja2 import Template

app = Flask(__name__)

app.config['MONGO_DBNAME'] = 'repo'
app.config['MONGO_URI'] = 'mongodb://localhost:27017/repo'

mongo = PyMongo(app)

@app.route("/index", methods=['GET'])
def get_school_to_crime():
  crime = mongo.db.jzhou94_katerin.crime
  schools = mongo.db.jzhou94_katerin.schools
  output1 = []
  output2 = []
  output3 = []
  for s in crime.find():
    output1.append((s["_id"], s["value"]["crime"]))
  for s in schools.find():
    output2.append((s["_id"], s["value"]["schools"]))
  for i in output1:
  	for j in output2:
  		if i[0] == j[0]:
  			output3.append((i[1], j[1]))
  
  #return render_template('index.html', data = output3)
  return jsonify({'result' : output3})

if __name__ == "__main__":
	#this is invoked when in the shell  you run 
	#$ python app.py 
	app.run(port=5000, debug=True)