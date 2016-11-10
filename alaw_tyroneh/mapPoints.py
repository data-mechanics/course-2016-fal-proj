import dml
import matplotlib.pyplot as plt

class mapPoints():
	contributor = 'alaw_tyroneh'
	reads = ['alaw_tyroneh.PropertyGeoJSONs','alaw_tyroneh.StationsGeoJSONs']
	writes = []

	@staticmethod
	def data():
		'''Opens and retrieves data from Residental and Stations GeoJSONs'''

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('alaw_tyroneh', 'alaw_tyroneh')
		
		#pull Property and stations data
		data = (repo['alaw_tyroneh.PropertyGeoJSONs'].find(),repo['alaw_tyroneh.StationsGeoJSONs'].find())

		return data

	def visualize():
		'''Outputs matplotlib scatterplot'''

		res_data,stat_data = mapPoints.data()

		rx = []
		ry = []

		for json in res_data:
			if(json['value']['properties']['area'] != 'Somerville'):
				y = json['value']['geometry']['coordinates'][0]
				x = json['value']['geometry']['coordinates'][1]
				rx.append(x)
				ry.append(y)

		sx = []
		sy = []

		print(len(rx))

		for json in stat_data:
			y = json['value']['geometry']['coordinates'][0]
			x = json['value']['geometry']['coordinates'][1]
			sx.append(x)
			sy.append(y)

		print(len(sx))
		plt.figure(figsize=(10,10))
		plt.scatter(rx, ry)
		plt.scatter(sx, sy, color='red')
		plt.ylim(42.23,42.43)
		plt.xlim(-71.2,-71.0)
		plt.show()

if __name__ == '__main__':
	mapPoints.visualize()
## eof