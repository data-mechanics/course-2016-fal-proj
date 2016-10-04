//store transformed data in collection "ResidentialGeoJSON"
db.alaw_tyroneh.MBTAGeoJSONs.remove({});
db.createCollection("alaw_tyroneh.MBTAGeoJSONs");

function flatten(X) {
    //reduces id:{id,values} to id: {values}
    db[X].find().forEach(function(x) { db[X].update({_id: x._id}, x.value); });
}

db.alaw_tyroneh.TCStops.mapReduce(
    //map location data (lat,long) to geoJSON format
    function() {
        var route_name = this.name
        var directions = this.route.path.direction
        var mode = this.route.mode
        
        directions.forEach(function(dir) {
            dir.stop.forEach(function(s) {
                var lat = parseFloat(s.stop_lat);
                var lon = parseFloat(s.stop_lon);
                emit(this._id, {
                    "type":"Feature",
                    "geometry":{
                        "type":"Point",
                        "coordinates": [lat, lon]},
                    "properties":{
                        "type": mode,
                        "line": route_name}
                });
            });
        });
    },
    function(){
    },
    {out:"alaw_tyroneh.MBTAGeoJSONs"}
);
