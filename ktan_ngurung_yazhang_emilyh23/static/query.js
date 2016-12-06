$(document).ready(
    initMap
);

function initMap() {

  var map = L.map('mapid').setView([42.3601, -71.0589], 12);
  L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {
    attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
  }).addTo(map);

  L.marker([42.3601, -71.0589]).addTo(map)
        .bindPopup('This is Boston.')
        .openPopup();
}

function getResult() {
  
  var num_zc = $("#num-zip").val();
  var hubway_r = $("#hubways").val();
  var tstop_r = $("#tstops").val();
  var busstop_r = $("#busstops").val();
  var colleges_r = $("#colleges").val();
  var bigbelly_r = $("#bigbelly").val();
  console.log(num_zc)
  console.log(hubway_r)
  console.log(tstop_r)
  console.log(busstop_r)
  console.log(colleges_r)
  console.log(bigbelly_r)

  var url = "http://localhost:5000/optimization/" + busstop_r + "/" + tstop_r + "/" + colleges_r + "/" + bigbelly_r + "/" + hubway_r + "/" + num_zc;
  console.log(url);
  var http = new XMLHttpRequest();
  http.open("GET", url, true);
  http.setRequestHeader("Content-type", "application/json");
  http.onreadystatechange = function() {
    if (http.readyState == 4 && http.status == 200) {
      var data = JSON.parse(http.responseText);
      console.log(data)
    }
  }
  http.send();
}