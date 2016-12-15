// Initialize FastClick.js
if ('addEventListener' in document) {
    document.addEventListener('DOMContentLoaded', function() {
        FastClick.attach(document.body);
    }, false);
} 

$.ajax({
url: "mbta.json",
success: function(data) {
    for (var i in data) {
        L.circle([data[i].latitude,data[i].longitude], {
            color: 'blue',
            fillColor: 'blue',
            fillOpacity: 0.5,
            opacity: 0.5,
            radius: 50
        }).addTo(mymap);
    }
}
}).error(function(data) {
    console.log(typeof(data));
    for (var i in data) {
        console.log('pebis',i);
        break;
    }
});

$.ajax({
url: "busstop.json",
success: function(data) {
    for (var i in data) {
        console.log((data[i].geometry.coordinates[1]).toFixed(6), (data[i].geometry.coordinates[0]).toFixed(6));
        L.circle([(data[i].geometry.coordinates[1]).toFixed(6), (data[i].geometry.coordinates[0]).toFixed(6)], {
            color: 'green',
            fillColor: 'green',
            fillOpacity: 0.25,
            opacity: 0.25,
            radius: 50
        }).addTo(mymap);
    }
}
}).error(function(data) {
    console.log(typeof(data));
    for (var i in data) {
        console.log('pebis',i);
        break;
    }
});


$.ajax({
url: "hubway.json",
success: function(data) {
    for (var i in data) {
        L.circle([data[i].properties.lat, data[i].properties.long_], {
            color: 'red',
            fillColor: 'red',
            fillOpacity: 0.5,
            opacity: 0.5,
            radius: 50
        }).addTo(mymap);
    }
}
}).error(function(data) {
    console.log(typeof(data));
    for (var i in data) {
        console.log('pebis',i);
        break;
    }
});
/*

$.ajax({
url: "busstop.json",
success: function(data) {
    console.log(data);
}
}).error(function(err) {console.log(err);});

$.ajax({
url: "hubway.json",
success: function(data) {
    console.log(data);
}
}).error(function(err) {console.log(err);});
*/


var mymap = L.map('mapid').setView([42.3132882,-71.1972408], 11);

L.tileLayer('https://api.mapbox.com/v4/mapbox.streets/{z}/{x}/{y}@2x.png?access_token={accessToken}', {
    attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="http://mapbox.com">Mapbox</a>',
    maxZoom: 18,
    accessToken: 'pk.eyJ1IjoibGRlYmVhc2kiLCJhIjoiY2l3ZTd0YzhnMGFzeDJ5bWgwbTRrc2NmZyJ9.XLWgyqsOznkQ3boue9qnUw'
}).addTo(mymap);

L.marker([42.27923995, -71.09808465]).addTo(mymap);
L.marker([42.34762953, -70.98903512]).addTo(mymap);
L.marker([42.233738  , -71.13185973]).addTo(mymap);
L.marker([42.30611988, -71.1327714]).addTo(mymap);
