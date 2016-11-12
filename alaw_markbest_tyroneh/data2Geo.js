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

//store transformed property data in collection "PropertyGeoJSONs"

dropPerm("alaw_markbest_tyroneh.PropertyGeoJSONs");
createPerm("alaw_markbest_tyroneh.PropertyGeoJSONs");


function flatten(X) {
	//reduces id:{id,values} to id: {values}
    db[X].find().forEach(function(x) { db[X].update({_id: x._id}, x.value); });
}

db.alaw_markbest_tyroneh.BostonProperty.mapReduce(
	//map location data (lat,long) to geoJSON format
	function() {
		var location = String(this.location).replace("(","").replace(")","");
		var lat = parseFloat(location.split(",")[0]);
		var long = parseFloat(location.split(",")[1]);
		var name = "Boston";
		var property;
		var rooms;
		if(parseInt(this.living_area) <= 0.0){
			property = 'Commercial';
			rooms = 0;
		}
		else{
			property = 'Residential';
			rooms = Math.max(parseInt(this.r_bdrms),1);
		}
		if((lat >= 42.23) && (lat <= 42.41) && (long >= -71.18) && (long <= -70.993)){
			emit(this._id, {
				"type":"Feature",
				"geometry":{
					"type":"Point",
					"coordinates": [lat,long]},
				"properties":{
					"type": property,
					"rooms": rooms,
					"area": name
				}
			})
		}
	},
	//no reduce step, all ids are unique
	function(){},
	{out:{merge:"alaw_markbest_tyroneh.PropertyGeoJSONs"}}
);

db.alaw_markbest_tyroneh.CambridgeProperty.mapReduce(
	//map location data (long,lat) to geoJSON format
	function() {
		var lat = parseFloat(this.location_1.coordinates[1]);
		var long = parseFloat(this.location_1.coordinates[0]);
		var name = "Cambridge";
		var property;
		var rooms;
		if(this.land_use_category.includes('Residential')){
			property = 'Residential';
			rooms = Math.max(parseInt(this.existing_units),1.0);
		}
		else{
			property = 'Commercial';
			rooms = 0;
		}
		if((lat >= 42.23) && (lat <= 42.41) && (long >= -71.18) && (long <= -70.993)){
			emit(this._id, {
				"type":"Feature",
				"geometry":{
					"type":"Point",
					"coordinates": [lat,long]},
				"properties":{
					"type":property,
					"rooms": rooms,
					"area": name
				}
			})
		}
	},
	//no reduce step, all ids are unique
	function(){},
	{out:{merge:"alaw_markbest_tyroneh.PropertyGeoJSONs"}}
);

db.alaw_markbest_tyroneh.SomervilleProperty.mapReduce(
	//map location data (long,lat) to geoJSON format
	function() {
		if(this.location_1 != undefined){
			var lat = parseFloat(this.location_1.coordinates[1]);
			var long = parseFloat(this.location_1.coordinates[0]);
			var name = "Somerville";
			var property;
			var rooms;
			if(this.building_type != 'Commercial'){
				property = 'Residential';
				rooms = Math.max(this.num_bedrms,1.0);
			}
			else{
				property = 'Commercial';
				rooms = 0;
			}
			if((lat >= 42.23) && (lat <= 42.41) && (long >= -71.18) && (long <= -70.993)){
				emit(this._id, {
					"type":"Feature",
					"geometry":{
						"type":"Point",
						"coordinates": [lat,long]},
					"properties":{
						"type":property,
						"rooms": rooms,
						"area": name
					}
				})
			}
		}
	},
	//no reduce step, all ids are unique
	function(){},
	{out:{merge:"alaw_markbest_tyroneh.PropertyGeoJSONs"}}
);

db.alaw_markbest_tyroneh.BrooklineProperty.mapReduce(
	//map rich geoJSON polygon to multiple singular coordinates of the polygon
	function() {
		var coors = this.geometry.coordinates[0];
		var name = "Brookline";
		var property;
		var rooms;
		if(this.properties.FEATURECODE == 'Building General'){
			property = 'Residential';
			rooms = Math.max(parseFloat(this.properties.NUMSTORIES) * 2.0, 1.0);
		}
		else{
			property = 'Commercial';
			rooms = 0;
		}
		for (var i = coors.length - 1; i >= 0; i--) {
			emit(this._id,{"coor":coors[i],"property":property,"rooms":rooms})
		}
	},
	//reduce by averaging all coordinates per property and store as geoJSON
	function(k,vs){
		var c = 0;
		var lat = 0;
		var long = 0;
		var name = "Brookline";
		var property;
		var rooms;
		vs.forEach(function(v){
			c++;
			lat += v.coor[1];
			long += v.coor[0];
			property = v.property;
			rooms = v.rooms;
		});
		lat = lat / c;
		long = long / c;
		if((lat >= 42.23) && (lat <= 42.41) && (long >= -71.18) && (long <= -70.993)){
			return {
					"type":"Feature",
					"geometry":{
					"type":"Point",
					"coordinates": [lat,long]},
					"properties":{
						"type": property,
						"rooms": rooms,
						"area": name
					}
				};
			}
		},
	{out:{merge:"alaw_markbest_tyroneh.PropertyGeoJSONs"}}
);

// flatten("alaw_markbest_tyroneh.PropertyGeoJSONs")

//store transformed station data in collection "StationsGeoJSONs"

dropPerm("alaw_markbest_tyroneh.StationsGeoJSONs");
createPerm("alaw_markbest_tyroneh.StationsGeoJSONs");

db.alaw_markbest_tyroneh.HubwayStations.mapReduce(
	//map location data (long,lat) and type to geoJSON format
	function() {
		if((this.Latitude >= 42.23) && (this.Latitude <= 42.41) && (this.Longitude >= -71.18) && (this.Longitude <= -70.993)){
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
		}
	},
	//no reduce step, all ids are unique
	function(){},
	{out:{merge:"alaw_markbest_tyroneh.StationsGeoJSONs"}}
);

db.alaw_markbest_tyroneh.TCStops.mapReduce(
    //map location data (lat,long) to geoJSON format
    function() {
        var route_name = this.route.name
        var directions = this.route.path.direction
        var mode = this.route.mode
        
        directions.forEach(function(dir) {
            dir.stop.forEach(function(s) {
                var lat = parseFloat(s.stop_lat);
                var long = parseFloat(s.stop_lon);
                if((lat >= 42.23) && (lat <= 42.41) && (long >= -71.18) && (long <= -70.993)){
	                emit(s.stop_name, {
	                	"type":"Feature",
	                    "geometry":{
	                        "type":"Point",
	                        "coordinates": [lat, long]},
	                    "properties":{
	                        "type": mode,
	                        "line": [route_name]
	                    }
	                });
            	}
            });
        });
    },
    function(k, vs){
    	var routeList = Set()
    	var lat = vs[0].geometry.coordinates[0];
    	var long = vs[0].geometry.coordinates[1];
    	var mode = vs[0].properties.type;
    	vs.forEach(function(v){
    		routeList.add(v.properties.line[0]);
    	});
    	routeList = [r for (r of routeList)];
    	return {"type":"Feature",
                    "geometry":{
                        "type":"Point",
                        "coordinates": [lat, long]},
                    "properties":{
                        "type": mode,
                        "line": routeList}};
    },
    {out:{merge:"alaw_markbest_tyroneh.StationsGeoJSONs"}}
);


