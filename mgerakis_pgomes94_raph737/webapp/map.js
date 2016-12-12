
// In the following example, markers appear when the user clicks on the map.
// Each marker is labeled with a single alphabetical character.
var labels = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
var labelIndex = 0;
let map;


function initialize() {
  url = "http://localhost:3000/api/hospitals"
	fetch(url)
  .then(res => res.json())
  .then(function(hospitalData) {
		var boston = { lat: 42.3601, lng: -71.0589 };
		map = new google.maps.Map(document.getElementById('map'), {
  		zoom: 11,
  		center: boston
		});

    addHospitalMarkers(hospitalData.hospitals, map);

    url = "http://localhost:3000/api/clusters";
    return fetch(url);
  })
  .then(res => res.json())
  .then(function(clusters) {
    addClusterMarkers(clusters.c_clusters, map, 'green');
    addClusterMarkers(clusters.f_clusters, map, 'red');
  });
};

function addOptimalCoordsMarker(optimalcoord,map){
	addMarker('',{lat:parseFloat(optimalcoord[0].optimal_coord[0]),lng:parseFloat(optimalcoord[0].optimal_coord[1])},map,'blue')
}
function addtrafficlocsMarkers(trafficlocs,map){
	for(var i = 0; i < trafficlocs.length; i++){
		addMarker('',{lat:parseFloat(trafficlocs[i].location[0]),lng:parseFloat(trafficlocs[i].location[1])},map,'red')
	}
}

function addPoliceStationMarkers(policestations,map){
	for(var i = 0; i < policestations.length; i++){
		addMarker(policestations[i].identifier,{lat:parseFloat(policestations[i].location[0]),lng:parseFloat(policestations[i].location[1])},map,'green')
	}
}
function addMbtaMarkers(mbtaStops,map){
	for(var i = 0; i < mbtaStops.length; i++){
		addMarker('',{lat:parseFloat(mbtaStops[i].location[0]),lng:parseFloat(mbtaStops[i].location[1])},map,'green')
	}
}
function addCrimeMarkers(crimeData,map){
	for(var i = 0; i < crimeData.length; i++){
		addMarker('',{lat:parseFloat(crimeData[i].location[0]),lng:parseFloat(crimeData[i].location[1])},map,'red')
	}
}

function addHospitalMarkers(hospitalData,map){
	for(var i = 0; i < hospitalData.length; i++){
		addMarker('',{lat:hospitalData[i].location[0],lng:hospitalData[i].location[1]}, map,'yellow');	
	}
}

function addClusterMarkers(clusters, map, color){
  console.log(clusters);
  console.log(color);
  clusters.forEach(cluster => {
    addMarker('', {
      lat: cluster.location[0], lng: cluster.location[1]
    }, map, color);
  });
}

// Adds a marker to the map.
function addMarker(locationName,location, map,color) {
	// Add the marker at the clicked location, and add the next-available label
	// from the array of alphabetical characters.
	var marker = new google.maps.Marker({
	  position: location,
	  label: locationName,
	  map: map
	});
	marker.setIcon("http://labs.google.com/ridefinder/images/mm_20_".concat(color,'.png'))
}

google.maps.event.addDomListener(window, 'load', initialize);

	// // This event listener calls addMarker() when the map is clicked.
	// google.maps.event.addListener(map, 'click', function(event) {
	//   addMarker("Hospital",event.latLng, map);
	// });
