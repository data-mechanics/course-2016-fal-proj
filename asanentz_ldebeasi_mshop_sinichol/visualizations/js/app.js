// Initialize FastClick.js
if ('addEventListener' in document) {
    document.addEventListener('DOMContentLoaded', function() {
        FastClick.attach(document.body);
    }, false);
} 

var req = $.getJSON("dataset.json", function() {
   console.log('succ'); 
});

var mymap = L.map('mapid').setView([42.3132882,-71.1972408], 11);

L.tileLayer('https://api.mapbox.com/v4/mapbox.streets/{z}/{x}/{y}@2x.png?access_token={accessToken}', {
    attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="http://mapbox.com">Mapbox</a>',
    maxZoom: 18,
    accessToken: 'pk.eyJ1IjoibGRlYmVhc2kiLCJhIjoiY2l3ZTd0YzhnMGFzeDJ5bWgwbTRrc2NmZyJ9.XLWgyqsOznkQ3boue9qnUw'
}).addTo(mymap);