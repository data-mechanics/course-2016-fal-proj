import jsonschema, json
from urllib.request import urlopen
from flask import Flask, jsonify, abort, make_response, request, render_template
from flask_httpauth import HTTPBasicAuth
from optimize import optimize
import dml

app = Flask(__name__)
auth = HTTPBasicAuth()

app.config['MONGO_DBNAME'] = 'repo'
app.config['MONGO_USERNAME'] = 'emilyh23_ktan_ngurung_yazhang'
app.config['MONGO_PASSWORD'] = 'emilyh23_ktan_ngurung_yazhang'

@app.route('/index.html')
def index():
    return render_template('index.html')

@app.route('/visualization.html')
def visualization():
    return render_template("visualization.html")

@app.route('/optimization/<busstops>/<tstops>/<colleges>/<bigbelly>/<hubways>/<num_zip>', methods=["GET"])
def optimization(busstops, tstops, colleges, bigbelly, hubways, num_zip):
    optimal_zc = optimize.user_query(int(busstops), int(tstops), int(colleges), int(bigbelly), int(hubways), int(num_zip))
    results = [{'zipcode':i[0], 'overall_rating':i[1]['overall_rating'], 'neighborhood income':i[1]['income_star']} for i in optimal_zc.items()]
    print(results)
    return jsonify({'results': results})

@app.route('/treemap', methods=["GET"])
def get_hotline():
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('emilyh23_ktan_ngurung_yazhang', 'emilyh23_ktan_ngurung_yazhang')
    treemap_data = repo['emilyh23_ktan_ngurung_yazhang.treeMap'].find_one()
    treemap_data.pop("_id")
    print(treemap_data)
    return jsonify(treemap_data)

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found foo.'}), 404)

if __name__ == '__main__':
    app.run(debug=True)
