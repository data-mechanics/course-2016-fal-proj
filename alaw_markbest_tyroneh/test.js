// Write a weight kmeans script
db.loadServerScripts();

dropPerm("alaw_tyroneh.M");
createPerm("alaw_tyroneh.M");

dropPerm("alaw_tyroneh.P");
createPerm("alaw_tyroneh.P");

// We first want to initialize the means and points 

var stop_weight = 2;
var residential_weight = 1;

db.alaw_tyroneh.ResidentialGeoJSONs.mapReduce(
    function() {
        var coords = this.value.geometry.coordinates;
        var lat = coords[0];
        var lon = coords[1];
        emit(this._id, {
            "lat":lat
            "lon":lon
        })
    },
    // no reduce step, all ids are unique
    function(){},
    {out:"alaw_tyroneh.P"}
);

db.alaw_tyroneh.ResidentialGeoJSONs.mapReduce(
    function() {
    }
    function() {
    }
);

var iter = 0;
do {
    db.M.mapReduce(
        
    )
}
while (o != n)
