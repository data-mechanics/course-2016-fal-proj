import dml
import matplotlib.pyplot as plt

class mapData():
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

		#repo.logout()

		return data

	def visualize():
		'''Outputs matplotlib scatterplot'''

		prop_data,stat_data = mapPoints.data()

		rx = []
		ry = []

		cx = []
		cy = []

		for json in prop_data:
			y = json['value']['geometry']['coordinates'][0]
			x = json['value']['geometry']['coordinates'][1]

			if(json['value']['properties']['type'] == 'Residential'):
				rx.append(x)
				ry.append(y)
			else:
				cx.append(x)
				cy.append(y)

		sx = []
		sy = []

		print(len(rx))
		print(len(cx))

		for json in stat_data:
			y = json['value']['geometry']['coordinates'][0]
			x = json['value']['geometry']['coordinates'][1]
			sx.append(x)
			sy.append(y)

		print(len(sx))
		plt.figure(figsize=(10,10))
		plt.scatter(rx, ry, color='blue')
		plt.scatter(cx, cy, color='green')
		plt.scatter(sx, sy, color='red')
		#plt.ylim(42.23,42.41)
		#plt.xlim(-71.18,-70.993)
		plt.show()

if __name__ == '__main__':
	mapData.visualize()
## eof