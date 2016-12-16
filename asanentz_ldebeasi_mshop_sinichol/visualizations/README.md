# Visualizations

## How to Use
* Download/Clone this repo
* Start mongo using `mongod --auth`. macOS users may need `mongod --auth --dbpath /data/db"`.
* Be sure to run all files under "To Run" here: <https://github.com/liamdebeasi/course-2016-fal-proj/tree/master/asanentz_ldebeasi_mshop_sinichol>
* Navigate to `visualizations/`
* Run `npm install` to install all Node dependencies. macOS users may need to run `sudo npm install`.
* Run `node index.js` to start the webserver
* In a browser, navigate to <http://localhost:8888/data> or <http://localhost:8888/lookup>

## Endpoints

### /data
This contains a map with all of our data points overlayed on the map. Also included are markers which indicate the optimal locations for new transit stops to be placed.

### /lookup
This allows a user to enter an address and see the number of subway, bus, and Hubway stops in their area.
