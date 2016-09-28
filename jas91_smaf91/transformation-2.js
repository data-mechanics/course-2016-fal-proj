/*
 * Transformation 2
 *
 * Description:
 *       Finds the number of crimes and 311 reports in a 100ft sqare area. Based
 *       on latitude and longitude.
 */

// Get number of crimes per coordinates
//db.jas91_smaf91.crimes_per_location.drop();
db.jas91_smaf91.crime.mapReduce(
    function(){
        var id = {
            latitude:  this.geo_info.geometry.coordinates[0], 
            longitude: this.geo_info.geometry.coordinates[1],
            type: 'crime'
        }
        emit(id, 1);
    },
    function(k,vs) {
        return Array.sum(vs)
    },
    { 
        out: "jas91_smaf91.crime_per_location"
    }
);

// Get number of 311 requests per coordinates
//db.jas91_smaf91.sr311_per_location.drop();
db.jas91_smaf91.sr311.mapReduce(
    function(){
        var id = {
            latitude:  this.geo_info.geometry.coordinates[0], 
            longitude: this.geo_info.geometry.coordinates[1],
            type: 'sr311'
        }
        emit(id, 1);
    },
    function(k,vs) {
        return Array.sum(vs)
    },
    { 
        out: "jas91_smaf91.sr311_per_location"
    }
);

function union(X, Y, XY) {
    db[XY].remove({});
    db.createCollection(XY);
    db[X].find().forEach(function(x) {
        db[XY].insert(x);
    });
    db[Y].find().forEach(function(y) {
        db[XY].insert(y);
    });
}

//db.jas91_smaf91.union_crime_sr311.drop();
union('jas91_smaf91.crime_per_location', 'jas91_smaf91.sr311_per_location', 'jas91_smaf91.union_crime_sr311')

//db.jas91_smaf91.sr311_crime_per_location.drop();
db.jas91_smaf91.union_crime_sr311.mapReduce(
    function(){
        var id = {
            latitude:  this._id.latitude, 
            longitude: this._id.longitude
        }
        if (this._id.type == 'sr311') {
            emit(id, {crime: 0, sr311: this.value});
        } else {
            emit(id, {crime: this.value, sr311: 0});
        }
    },
    function(k,vs) {
        var total_crimes = 0
        var total_sr311 = 0
        vs.forEach(function(v, i) {
            total_crimes += v.crime;
            total_sr311 += v.sr311;
        });
        return {crime: total_crimes, sr311: total_sr311}
    },
    { 
        out: "jas91_smaf91.sr311_crime_per_location"
    }
);
