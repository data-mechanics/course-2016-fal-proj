/*
 * Transformation 2
 * Description:
 */

// Get number of crimes per coordinates
//db.crimes_per_location.drop();
db.jas91_smaf91.crime.mapReduce(
        function(){
            emit(this.geo_info.geometry.coordinates, 1);
        },
        function(k,vs) {
            return Array.sum(vs)
        },
        {out: "crimes_per_location"});

// Get number of 311 requests per coordinates
//db.sr311_per_location.drop();
db.jas91_smaf91.sr311.mapReduce(
        function(){
            emit(this.geo_info.geometry.coordinates, 1);
        },
        function(k,vs) {
            return Array.sum(vs)
        },
        {out: "sr311_per_location"});

db.jas91_smaf91.EXAMPLE.mapReduce(
        function(){
            emit(this._id, 1)
        },
        function(){},
        {out: "jas91_smaf91.test"}
    
        );
