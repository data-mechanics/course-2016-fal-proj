/*
 * Transformation 2
 * Description:
 */

// Get number of crimes per coordinates
//db.crimes_per_location.drop();
db.jas91_smaf91.crime.mapReduce(
        function(){
            emit(this.geo_info.geometry, 1);
        },
        function(k,vs) {
            return Array.sum(vs)
        },
        { 
            finalize: function(k, v) { 
                return {total: v, type: 'crime'}; 
            },
            out: "jas91_smaf91.crime_per_location"
        }
);

// Get number of 311 requests per coordinates
//db.sr311_per_location.drop();
db.jas91_smaf91.sr311.mapReduce(
        function(){
            emit(this.geo_info.geometry, 1);
        },
        function(k,vs) {
            return Array.sum(vs)
        },
        { 
            finalize: function(k, v) { 
                return {total: v, type: 'sr311'}; 
            },
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

union('jas91_smaf91.crime_per_location', 'jas91_smaf91.sr311_per_location', 'jas91_smaf91.union_crime_sr311')

db.jas91_smaf91.union_crime_sr311.mapReduce(
        function(){
            emit(this._id, {total: this.value.total, type: this.value.type});
        },
        function(k,vs) {
            total_crimes = 0
            total_sr311 = 0
            for (i = 0; i < vs.length; i++) {
                if (vs[i].type == 'sr311') {
                    total_sr311++;
                } else {
                    total_crimes++;
                }
            }
            return {crime: total_crimes, sr311: total_sr311};
        },
        { 
            finalize: function(k, v) { 
                if (v.type == 'crime') {
                    return {crime: v.total, sr311: 0}; 
                } else {
                    return {crime: 0, sr311: v.total}; 
                }
                return {crime: v.crime, sr311: v.sr311}; 
            },
            out: "jas91_smaf91.sr311_crime_per_location"
        }
);
