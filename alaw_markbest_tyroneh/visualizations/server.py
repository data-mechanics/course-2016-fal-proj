from flask import Flask
from flask import request
from flask import send_from_directory
from flask import render_template
import os

app = Flask(__name__)

def dir(): 
	return os.path.dirname(os.path.realpath(__file__))

@app.route('/')
def index():
	return 'hello world!'

@app.route('/allocation', methods=['GET'])
def loadAllocation():
	'''loads the visualization for bus allocation'''
	return render_template('allocation.html')

if __name__ == '__main__':
    app.run(debug=True)