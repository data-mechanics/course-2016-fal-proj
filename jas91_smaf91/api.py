from flask import request, url_for
from flask_api import FlaskAPI, status, exceptions
from kmeans import kmeans

app = FlaskAPI(__name__)
@app.route("/patrols_coordinates", methods=['GET'])
def patrols_coordinates():

    params = ['min_distance', 'min_patrols', 'max_patrols', 'codes'] 

    errors = []

    for param in params:
        if param not in request.args:
            errors.append({'error': 'Missing ' + param})

    if errors != []:
        return {'errors': errors}, status.HTTP_400_BAD_REQUEST

    try:
        min_distance = int(request.args['min_distance'])
        min_patrols = int(request.args['min_patrols'])
        max_patrols = int(request.args['max_patrols'])
        codes = request.args['codes']
    except:
        return {'errors': 'Parameters should be numeric'}, status.HTTP_400_BAD_REQUEST


    patrol_coordinates, crime_coordinates = kmeans.api_execute(min_distance, min_patrols, max_patrols, codes)
    data = {
        'patrol_coordinates': patrol_coordinates,
        'crime_coordinates': crime_coordinates
    }
    return {'data':data}, status.HTTP_200_OK


if __name__ == "__main__":
    app.run(debug=True)
