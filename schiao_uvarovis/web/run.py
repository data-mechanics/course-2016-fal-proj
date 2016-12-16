from flask import Flask, render_template, request
import data_analysis

app = Flask(__name__)

@app.route("/")
def home_page():
    sl_data = data_analysis.get_streetlights_data()
    ts_data = data_analysis.get_trafficsignals_data()
    td_data = data_analysis.get_districts_data()
    return render_template('index.html', sl=sl_data, ts=ts_data, td=td_data)

@app.route("/area-information/", methods=["GET"])
def area_info_page():
    info = data_analysis.get_area_information(request.args['x'], request.args['y'])
    return render_template('area-information.html', info=info, x=request.args['x'], y=request.args['y'])

if __name__ == '__main__':
    app.run(debug=True)
