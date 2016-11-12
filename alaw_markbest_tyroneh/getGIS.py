import urllib.request
import zipfile
import shapefile
import io
import sys

#GIS data/shapefiles urls, converts them to python dictionaries
gisURLs = {"CensusPopulation":'http://wsgw.mass.gov/data/gispub/shape/census2010/CENSUS2010TOWNS_SHP.zip'}
gisTowns = ['BOSTON','BROOKLINE','CAMBRIDGE','SOMERVILLE']

for key in gisURLs:
        url = gisURLs[key]

        # Get the data from the server.
        response = urllib.request.urlopen(url)

	# Unzip the file into the working directory.
        zip_ref = zipfile.ZipFile(io.BytesIO(response.read()))
        zip_ref.extractall("./")
        zip_ref.close()

	# Read the file into the Python shapefile library.
        sf = shapefile.Reader("CENSUS2010TOWNS_POLY")

        # Pull out the specific records for the four areas of interest, listed in gisTowns above.
        # NOTE: The attribute at index 1 of any record is the uppercase town name.
	boston_area = [x for x in sf.iterRecords() if x[1] in gisTowns]

	gis_result = {key:boston_area}

        if(trial == True):
                print(key)
                print(boston_area)
                print('-----------------')

        else:
                # Set up the database connection
                repo.dropPermanent(key)
                repo.createPermanent(key)
                repo['alaw_markbest_tyroneh.'+key].insert_many(csvfile)
