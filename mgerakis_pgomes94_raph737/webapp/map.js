
// In the following example, markers appear when the user clicks on the map.
// Each marker is labeled with a single alphabetical character.
var labels = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
var labelIndex = 0;


function initialize() {
	url = "http://localhost:3000/api/hospitals"
	data = fetch(url).then(res => res.json());
	console.log(data)
	var boston = { lat: 42.3601, lng: -71.0589 };
	var map = new google.maps.Map(document.getElementById('map'), {
	  zoom: 12,
	  center: boston
	});

	// This event listener calls addMarker() when the map is clicked.
	google.maps.event.addListener(map, 'click', function(event) {
	  addMarker("Hospital",event.latLng, map);
	});

	// Add a marker at the center of the map.
	addMarker("Boston", boston, map);
}


// Adds a marker to the map.
function addMarker(locationName,location, map) {
	// Add the marker at the clicked location, and add the next-available label
	// from the array of alphabetical characters.
	var marker = new google.maps.Marker({
	  position: location,
	  label: locationName,
	  map: map
	});
	marker.setIcon('http://maps.google.com/mapfiles/ms/icons/green-dot.png')
}

google.maps.event.addDomListener(window, 'load', initialize);