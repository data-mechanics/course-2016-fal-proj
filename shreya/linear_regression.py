import trends
import matplotlib.pyplot as plt
import numpy as np
from sklearn import linear_model

def get_linear_regression(trends,trial):
	result = []
	
	for trend in trends:
		data = trend
		x = [xi for (xi, yi) in data]
		x = np.array(x).reshape(-1,1)
		y = [yi for (xi, yi) in data]
		y = np.array(y).reshape(-1,1)
		regr = linear_model.LinearRegression()
		regr.fit(x,y)
		# The coefficients
		# The mean squared error
		# Explained variance score: 1 is perfect prediction
		result += [(regr.coef_.tolist(), np.mean((regr.predict(x) - y) ** 2).tolist(),regr.score(x, y).tolist())]
		#To predict and graph
		if not trial:
			plt.scatter(x,y,color='black')
			plt.plot(x,regr.predict(x),color='blue',linewidth=3)
			plt.xticks(())
			plt.yticks(())
			plt.show()
	
	return list(zip(result,trends))