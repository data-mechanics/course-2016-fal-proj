// Load MBTA subway data
$.ajax({
url: "mbta.json",
success: function(data) {
    // Loop through each stop and see if it's the closest point to one of the optimal points
    for (var i in data) {
        
        var distanceA = dist(data[i].latitude, data[i].longitude, pointA[0], pointA[1]);
        
        if (distanceA < minA.dist || !minA.dist) {
            minA.dist = distanceA;
            minA.coordinates = [data[i].latitude, data[i].longitude];
        }
        
        var distanceB = dist(data[i].latitude, data[i].longitude, pointB[0], pointB[1]);
        
        if (distanceB < minB.dist || !minD.dist) {
            minB.dist = distanceB;
            minB.coordinates = [data[i].latitude, data[i].longitude];
        }
        
        var distanceC = dist(data[i].latitude, data[i].longitude, pointC[0], pointC[1]);
        
        if (distanceC < minC.dist || !minC.dist) {
            minC.dist = distanceC;
            minC.coordinates = [data[i].latitude, data[i].longitude];
        }
        
        var distanceD = dist(data[i].latitude, data[i].longitude, pointD[0], pointD[1]);
        
        if (distanceD < minD.dist || !minD.dist) {
            minD.dist = distanceD;
            minD.coordinates = [data[i].latitude, data[i].longitude];
        }
        
        // add a point to the map
        L.circle([data[i].latitude,data[i].longitude], {
            color: 'blue',
            fillColor: 'blue',
            fillOpacity: 0.5,
            opacity: 0.5,
            radius: 50
        }).addTo(mymap);
    }
}
}).error(function(data) { });

// Load in MBTA Bus Stop data
$.ajax({
url: "busstop.json",
success: function(data) {
    
    // Loop through each stop and see if it's the closest point to one of the optimal points
    for (var i in data) {
        
        var distanceA = dist(data[i].geometry.coordinates[1], data[i].geometry.coordinates[0], pointA[0], pointA[1]);
        
        if (distanceA < minA.dist || !minA.dist) {
            minA.dist = distanceA;
            minA.coordinates = [data[i].geometry.coordinates[1], data[i].geometry.coordinates[0]];
        }
        
        var distanceB = dist(data[i].geometry.coordinates[1], data[i].geometry.coordinates[0], pointB[0], pointB[1]);
        
        if (distanceB < minB.dist || !minD.dist) {
            minB.dist = distanceB;
            minB.coordinates = [data[i].geometry.coordinates[1], data[i].geometry.coordinates[0]];
        }
        
        var distanceC = dist(data[i].geometry.coordinates[1], data[i].geometry.coordinates[0], pointC[0], pointC[1]);
        
        if (distanceC < minC.dist || !minC.dist) {
            minC.dist = distanceC;
            minC.coordinates = [data[i].geometry.coordinates[1], data[i].geometry.coordinates[0]];
        }
        
        var distanceD = dist(data[i].geometry.coordinates[1], data[i].geometry.coordinates[0], pointD[0], pointD[1]);
        
        if (distanceD < minD.dist || !minD.dist) {
            minD.dist = distanceD;
            minD.coordinates = [data[i].geometry.coordinates[1], data[i].geometry.coordinates[0]];
        }

        // add a point to the map
        L.circle([data[i].geometry.coordinates[1], data[i].geometry.coordinates[0]], {
            color: 'green',
            fillColor: 'green',
            fillOpacity: 0.25,
            opacity: 0.25,
            radius: 50
        }).addTo(mymap);
    }
}
}).error(function(data) { });


// Load in Hubway data
$.ajax({
url: "hubway.json",
success: function(data) {
    
    // Loop through each hubway pickup and see if it's the closest point to one of the optimal points
    for (var i in data) {
        
        var distanceA = dist(data[i].properties.lat, data[i].properties.long_, pointA[0], pointA[1]);
        
        if (distanceA < minA.dist || !minA.dist) {
            minA.dist = distanceA;
            minA.coordinates = [data[i].properties.lat, data[i].properties.long_];
        }
        
        var distanceB = dist(data[i].properties.lat, data[i].properties.long_, pointB[0], pointB[1]);
        
        if (distanceB < minB.dist || !minD.dist) {
            minB.dist = distanceB;
            minB.coordinates = [data[i].properties.lat, data[i].properties.long_];
        }
        
        var distanceC = dist(data[i].properties.lat, data[i].properties.long_, pointC[0], pointC[1]);
        
        if (distanceC < minC.dist || !minC.dist) {
            minC.dist = distanceC;
            minC.coordinates = [data[i].properties.lat, data[i].properties.long_];
        }
        
        var distanceD = dist(data[i].properties.lat, data[i].properties.long_, pointD[0], pointD[1]);
        
        if (distanceD < minD.dist || !minD.dist) {
            minD.dist = distanceD;
            minD.coordinates = [data[i].properties.lat, data[i].properties.long_];
        }
        
        // add a point to the map
        L.circle([data[i].properties.lat, data[i].properties.long_], {
            color: 'red',
            fillColor: 'red',
            fillOpacity: 0.5,
            opacity: 0.5,
            radius: 50
        }).addTo(mymap);
    }
}
}).error(function(data) { });


// set to center of boston
var mymap = L.map('mapid').setView([42.3132882,-71.0972408], 12);


L.tileLayer('https://api.mapbox.com/v4/mapbox.streets/{z}/{x}/{y}@2x.png?access_token={accessToken}', {
    attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="http://mapbox.com">Mapbox</a>',
    maxZoom: 18,
    accessToken: 'pk.eyJ1IjoibGRlYmVhc2kiLCJhIjoiY2l3ZTd0YzhnMGFzeDJ5bWgwbTRrc2NmZyJ9.XLWgyqsOznkQ3boue9qnUw'
}).addTo(mymap);

// four optimal locations
var pointA = [42.27923995, -71.09808465]; 
var pointB = [42.34762953, -70.98903512];
var pointC = [42.233738, -71.13185973]; 
var pointD = [42.30611988, -71.1327714];

// setup minimum objects
var minA = {
    dist: null,
    coordinates: [],
    marker: pointA
};
var minB = {
    dist: null,
    coordinates: [],
    marker: pointB
};

var minC = {
    dist: null,
    coordinates: [],
    marker: pointC
};

var minD = {
    dist: null,
    coordinates: [],
    marker: pointD
};

// add optimal locations to map as markers
L.marker(pointA).addTo(mymap);
L.marker(pointB).addTo(mymap);
L.marker(pointC).addTo(mymap);
L.marker(pointD).addTo(mymap);

// convert degrees -> radians
function toRadians(deg) {
    return deg * (Math.PI/180);
}

// computes the distance between (lat1,lon1) and (lat2,lon2)
// uses the Haversine formula: https://en.wikipedia.org/wiki/Haversine_formula
function dist(lat1, lon1, lat2, lon2) {
    var R = 6371e3; // metres
    var thetaOne = toRadians(lat1);
    var thetaTwo = toRadians(lat2);
    var deltaTheta = toRadians(lat2-lat1);
    var deltaLambda = toRadians(lon2-lon1);
    
    var a = Math.sin(deltaTheta/2) * Math.sin(deltaTheta/2) +
            Math.cos(thetaOne) * Math.cos(thetaTwo) *
            Math.sin(deltaLambda/2) * Math.sin(deltaLambda/2);
    var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    
    var d = R * c;
    
    return d;
}