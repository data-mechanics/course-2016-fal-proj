/*
 *	MAP-REDUCE FILE
 *	---------------
 *	
 *	This Javascript script is intended to be executed through the transformData.py script.
 *	Please run this file inside there so that the provenance documentation can also be 
 *	recorded. 
 *	
 *	List of Transformations:
 *		- Convert all residential property data to the GeoJSON format
 *			1) BostonProperty, CambridgeProperty, and SomervilleProperty coordinates
 *			   to GeoJSON points
 *			2) Convert BrooklineProperty GeoJSON polygons to GeoJSON points by finding 
 *			   the average of all of the corners to find the polygon's middle point
 *		- Convert all Public Transportation Stations data to GeoJSON format
 *			3) Convert HubwayStations coordinates to GeoJSON points
 *			4) Change TCStops stops per route to GeoJSON points per stop with routes
 *			   properties
 *
 *	
*/

db.loadServerScripts();

//store transformed property data in collection "ResidentialGeoJSON"

dropPerm("alaw_tyroneh.ResidentialGeoJSONs");
createPerm("alaw_tyroneh.ResidentialGeoJSONs");


function flatten(X) {
	//reduces id:{id,values} to id: {values}
    db[X].find().forEach(function(x) { db[X].update({_id: x._id}, x.value); });
}

db.alaw_tyroneh.BostonProperty.mapReduce(
	//map location data (lat,long) to geoJSON format
	function() {
		var location = String(this.location).replace("(","").replace(")","");
		var lat = parseFloat(location.split(",")[0]);
		var long = parseFloat(location.split(",")[1]);
		var name = "Boston";
		emit(this._id, {
			"type":"Feature",
			"geometry":{
				"type":"Point",
				"coordinates": [lat,long]},
			"properties":{
				"type":"Residence",
				"area": name
			}
		})
	},
	//no reduce step, all ids are unique
	function(){},
	{out:{merge:"alaw_tyroneh.ResidentialGeoJSONs"}}
);

db.alaw_tyroneh.CambridgeProperty.mapReduce(
	//map location data (long,lat) to geoJSON format
	function() {
		var lat = parseFloat(this.location_1.coordinates[1]);
		var long = parseFloat(this.location_1.coordinates[0]);
		var name = "Cambridge";
		emit(this._id, {
			"type":"Feature",
			"geometry":{
				"type":"Point",
				"coordinates": [lat,long]},
			"properties":{
				"type":"Residence",
				"area": name
			}
		})
	},
	//no reduce step, all ids are unique
	function(){},
	{out:{merge:"alaw_tyroneh.ResidentialGeoJSONs"}}
);

db.alaw_tyroneh.SomervilleProperty.mapReduce(
	//map location data (long,lat) to geoJSON format
	function() {
		if(this.location_1 != undefined){
			var lat = parseFloat(this.location_1.coordinates[1]);
			var long = parseFloat(this.location_1.coordinates[0]);
			var name = "Somerville";
			emit(this._id, {
				"type":"Feature",
				"geometry":{
					"type":"Point",
					"coordinates": [lat,long]},
				"properties":{
					"type":"Residence",
					"area": name
				}
			})
		}
	},
	//no reduce step, all ids are unique
	function(){},
	{out:{merge:"alaw_tyroneh.ResidentialGeoJSONs"}}
);

db.alaw_tyroneh.BrooklineProperty.mapReduce(
	//map rich geoJSON polygon to multiple singular coordinates of the polygon
	function() {
		var coors = this.geometry.coordinates[0];
		var name = "Brookline";

		for (var i = coors.length - 1; i >= 0; i--) {
			emit(this._id,{"coor":coors[i]})
		}
	},
	//reduce by averaging all coordinates per property and store as geoJSON
	function(k,vs){
		var c = 0;
		var lat = 0;
		var long = 0;
		var name = "Brookline";
		vs.forEach(function(v){
			c++;
			lat += v.coor[1];
			long += v.coor[0];
		});
		lat = lat / c;
		long = long / c;
		return {
				"type":"Feature",
				"geometry":{
				"type":"Point",
				"coordinates": [lat,long]},
				"properties":{
					"type":"Residence",
					"area": name
				}
			};
		},
	{out:{merge:"alaw_tyroneh.ResidentialGeoJSONs"}}
);

// flatten("alaw_tyroneh.ResidentialGeoJSONs")

//store transformed station data in collection "ResidentialGeoJSON"

dropPerm("alaw_tyroneh.StationsGeoJSONs");
createPerm("alaw_tyroneh.StationsGeoJSONs");

db.alaw_tyroneh.HubwayStations.mapReduce(
	//map location data (long,lat) and type to geoJSON format
	function() {
		emit(this._id, {
			"type":"Feature",
			"geometry":{
				"type":"Point",
				"coordinates": [parseFloat(this.Latitude),parseFloat(this.Longitude)]},
			"properties":{
				"type":"Hubway",
				"capacity": parseInt(this["# of Docks"]),
				"StationID": this["Station ID"],
				"Station": this.Station
			}
		})
	},
	//no reduce step, all ids are unique
	function(){},
	{out:{merge:"alaw_tyroneh.StationsGeoJSONs"}}
);

db.alaw_tyroneh.TCStops.mapReduce(
    //map location data (lat,long) to geoJSON format
    function() {
        var route_name = this.route.name
        var directions = this.route.path.direction
        var mode = this.route.mode
        
        directions.forEach(function(dir) {
            dir.stop.forEach(function(s) {
                var lat = parseFloat(s.stop_lat);
                var lon = parseFloat(s.stop_lon);
                emit(s.stop_name, {
                	"type":"Feature",
                    "geometry":{
                        "type":"Point",
                        "coordinates": [lat, lon]},
                    "properties":{
                        "type": mode,
                        "line": [route_name]}});
            });
        });
    },
    function(k, vs){
    	var routeList = Set()
    	var lat = vs[0].geometry.coordinates[0];
    	var lon = vs[0].geometry.coordinates[1];
    	var mode = vs[0].properties.type;
    	vs.forEach(function(v){
    		routeList.add(v.properties.line[0]);
    	});
    	routeList = [r for (r of routeList)];
    	return {"type":"Feature",
                    "geometry":{
                        "type":"Point",
                        "coordinates": [lat, lon]},
                    "properties":{
                        "type": mode,
                        "line": routeList}};
    },
    {out:{merge:"alaw_tyroneh.StationsGeoJSONs"}}
);


