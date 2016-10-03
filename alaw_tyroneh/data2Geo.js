//store transformed data in collection "ResidentialGeoJSON"
db.alaw_tyroneh.ResidentialGeoJSONs.remove({});
db.alaw_tyroneh.createCollection("ResidentialGeoJSONs");

function flatten(X) {
	//reduces id:{id,values} to id: {values}
    db.alaw_tyroneh[X].find().forEach(function(x) { db[X].update({_id: x._id}, x.value); });
}

db.alaw_tyroneh.BostonProperty.mapReduce(
	//map location data (lat,long) to geoJSON format
	function() {
		var lat = parseFloat(this.location.split(", ")[0].replace("(",""));
		var long = parseFloat(this.location.split(", ")[1].replace(")",""));
		var name = "Boston";
		emit(this.id, {
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
	function(){},
	{out:"ResidentialGeoJSONs"}
);

db.alaw_tyroneh.CambridgeProperty.mapReduce(
	//map location data (long,lat) to geoJSON format
	function() {
		var lat = this.location_1.coordinates[1];
		var long = this.location_1.coordinates[0];
		var name = "Cambridge";
		emit(this.id, {
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
	function(){},
	{out:"ResidentialGeoJSONs"}
);

db.alaw_tyroneh.SomervilleProperty.mapReduce(
	//map location data (long,lat) to geoJSON format
	function() {
		var lat = this.location_1.coordinates[1];
		var long = this.location_1.coordinates[0];
		var name = "Somerville";
		emit(this.id, {
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
	function(){},
	{out:"ResidentialGeoJSONs"}
);

db.alaw_tyroneh.BrookLineProperty.mapReduce(
	//map rich geoJSON to simple geoJSON
	function() {
		var coordinates = this.geometry.coordinates[0]
		var lat;
		var long;
		var c;
		coordinates.forEach(function(coor)){
			c++;
			lat += coor[1]
			long += coor[0]

		};
		lat = lat / c;
		long = long / c;
		var name = "Brookline";
		emit(this.id, {
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
	function(){},
	{out:"ResidentialGeoJSONs"}
);

flatten("ResidentialGeoJSONs")
