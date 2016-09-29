/*
 * Transformation 3
 *
 * Description:
 *       Finds the number of crimes and 311 reports in a 100ft sqare area. Based
 *       on latitude and longitude.
 */

db.system.js.save({ _id: "transformation3", value: function() {
    function project(x) {
        return {geo_info: x.geo_info}
    }

    function union(X, Y, XY, f) {
        db[X].find().forEach(function(x) {
            db[XY].insert(f(x));
        });
        db[Y].find().forEach(function(y) {
            db[XY].insert(f(y));
        });
    }

    db.jas91_smaf91.union_temp.drop();
    
    // Find the union of all the datasets and project their coordinates and zip codes
    db.createCollection('jas91_smaf91.union_temp');
    union('jas91_smaf91.sr311', 'jas91_smaf91.food', 'jas91_smaf91.union_temp', project);
    union('jas91_smaf91.schools', 'jas91_smaf91.hospitals', 'jas91_smaf91.union_temp', project);

    db.jas91_smaf91.union_temp.mapReduce(
        function(){
            var id = {
                latitude:  this.geo_info.geometry.coordinates[0], 
                longitude: this.geo_info.geometry.coordinates[1],
            }
            emit(id, {zip_code:this.geo_info.properties.zip_code});
        },
        function(k,vs) {
            return vs[0] 
        },
        { 
            out: "jas91_smaf91.coordinates_zip_codes"
        }
    );

    db.jas91_smaf91.union_temp.drop();

    db.jas91_smaf91.crime.find().forEach(
        function (x) {
            var id = {
                latitude:  x.geo_info.geometry.coordinates[0], 
                longitude: x.geo_info.geometry.coordinates[1]
            };
            match = db.jas91_smaf91.coordinates_zip_codes.findOne({_id: id});
            if (match) {
                zip_code = match.value.zip_code; 
                x.geo_info.properties.zip_code = zip_code;
                db.jas91_smaf91.crime.save(x);
            }
        }
    );

    db.jas91_smaf91.coordinates_zip_codes.drop();
}});
