/*
 * Transformation 1
 * Description:
 *      Standarizes geo location information for each dataset following the 
 *      GeoJSON standard
 */

const precision = 3;

function transform_geo_info(zip_code, latitude, longitude) {
    return {
        type: "Feature",
        geometry: {
            type: "Point",
            coordinates: [parseFloat(latitude), parseFloat(longitude)]
        },
        properties: {
            zip_code: zip_code
        }
   }
}

// food
db.jas91_smaf91.food.find().forEach(function(x) {

    var zip_code = x.zip;
    var latitude = x.location ? x.location.coordinates[1].toFixed(precision) : null; 
    var longitude = x.location ? x.location.coordinates[0].toFixed(precision) : null; 

    db.jas91_smaf91.food.update({_id: x._id},
        {
            '$set': {
                geo_info: transform_geo_info(zip_code, latitude,longitude)
            }, 
            '$unset': {
                location: "",
                zip: ""
            }
        }
   );
});

db.jas91_smaf91.schools.find().forEach(function(x) {
    
    var zip_code = x.zip_code;
    var latitude = x.map_location ? x.map_location.coordinates[1].toFixed(precision) : null;
    var longitude = x.map_location ? x.map_location.coordinates[0].toFixed(precision) : null;

    db.jas91_smaf91.schools.update({_id: x._id},
        {
            '$set': {
                geo_info: transform_geo_info(zip_code, latitude,longitude)
            },
            '$unset': {
                zip_code: "",
                map_location: "",
                map_locations: "",
                coordinates: ""
            }
        }
    )
});

db.jas91_smaf91.hospitals.find().forEach(function(x) {
    
    var zip_code = x.zipcode;
    var latitude  = x.location ? x.location.coordinates[1].toFixed(precision) : null;
    var longitude = x.location ? x.location.coordinates[0].toFixed(precision) : null;

    db.jas91_smaf91.hospitals.update( {_id: x._id},
        {
            '$set': {
                geo_info: transform_geo_info(zip_code, latitude,longitude)
            },
            '$unset': {
                zipcode: "",
                location: "",
                location_zip: "",
                xcoord: "",
                ycoord: ""
            }
        }
    )
});

db.jas91_smaf91.crime.find().forEach(function(x) {
    
    var zip_code = null;
    var latitude  = x.location ? x.location.coordinates[1].toFixed(precision) : null;
    var longitude = x.location ? x.location.coordinates[0].toFixed(precision) : null;

    db.jas91_smaf91.crime.update( {_id: x._id},
        {
            '$set': {
                geo_info: transform_geo_info(zip_code, latitude,longitude)
            },
            '$unset': {
                location: ""
            }
        }
    )
});

db.jas91_smaf91.sr311.find().forEach(function(x) {
    
    var zip_code = x.location_zipcode;
    var latitude  = x.geocoded_location ? parseFloat(x.geocoded_location.latitude).toFixed(precision) : null;
    var longitude = x.geocoded_location ? parseFloat(x.geocoded_location.longitude).toFixed(precision) : null;

    db.jas91_smaf91.sr311.update( {_id: x._id},
        {
            '$set': {
                geo_info: transform_geo_info(zip_code, latitude,longitude)
            },
            '$unset': {
                location_zipcode: "",
                geocoded_location: "",
                location_x: "",
                location_y: ""
            }
        }
    )
});
