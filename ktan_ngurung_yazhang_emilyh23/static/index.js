$(document).ready(function() {

  if (!$("#mapid").size()) {
      console.log("here"); 
    } else { 
      initMap(42.3294,-71.0632, 12); 
    }

});

function initMap() {

  window.map = L.map('mapid', { 
    center:[arguments[0], arguments[1]], 
    zoom:arguments[2],
  });

  var hubwayRatingLayer = L.geoJson(hubwaydata, {style: style}).addTo(map);
  var tstopRatingLayer = L.geoJson(tstopdata, {style: style}).addTo(map);
  var busstopRatingLayer = L.geoJson(busstopdata, {style: style}).addTo(map);
  var collegeRatingLayer = L.geoJson(collegedata, {style: style}).addTo(map);
  var bigbellyRatingLayer = L.geoJson(bigbellydata, {style: style}).addTo(map);
  var overallRatingLayer = L.geoJson(zcdata, {style: style}).addTo(map);

  
  var baseLayers = {
    "overall": overallRatingLayer,
    "hubway": hubwayRatingLayer,
    "tstop": tstopRatingLayer,
    "busstop": busstopRatingLayer,
    "colleges": collegeRatingLayer,
    "bigbelly": bigbellyRatingLayer
  };

  L.control.layers(baseLayers).addTo(map);

  L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {
    attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
  }).addTo(map);

  // window.pins = L.marker([arguments[0], arguments[1]]).addTo(map)
  //       .bindPopup('This is Boston.')
  //       .openPopup();

  window.pin_dict = new Array();
  pin_dict.push(new L.marker([arguments[0], arguments[1]]));
  pin_dict[0].addTo(map).bindPopup("init!!");

  function getColor(d) {
    return d > 200  ? '#E31A1C' :
           d > 100  ? '#FC4E2A' :
           d > 50   ? '#FD8D3C' :
                        '#FFEDA0';
  }

  function style(feature) {
      return {
          fillColor: getColor(feature.properties.density),
          weight: 2,
          opacity: 1,
          color: 'white',
          dashArray: '3',
          fillOpacity: 0.7
      };
  }

  var legend = L.control({position: 'bottomright'});

  legend.onAdd = function (map) {

    var div = L.DomUtil.create('div', 'info legend'),
      grades = [50, 100, 200],
      labels = [],
      from, to;

    for (var i = 0; i < grades.length; i++) {
      from = grades[i] + 1;
      to = grades[i + 1];
      rating = i + 1 

      labels.push(
        '<i style="background:' + getColor(from) + '"></i> ' +
        rating);
    }

    div.innerHTML = labels.join('<br>');
    return div;
  };

  legend.addTo(map);
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

      // getting zipcodes
      for (var i = 0; i < pin_dict.length; i++) {
        console.log('yes')
        map.removeLayer(pin_dict[i])
        pin_dict.shift()
      }
      console.log("for loop done")
      console.log(pin_dict)

      var results = data.results
      for (var i = 0; i < results.length; i++) {
        zc = results[i]['zipcode']
        lat = latLng[zc]['lat']
        long = latLng[zc]['lng']


        pin_dict.push(new L.marker([lat, long]));
        pin_dict[i].addTo(map).bindPopup(zc);

        // console.log(lat)
        // console.log(zc)
        // console.log(long)
      }

      console.log(data)
    }
  }
  http.send();
}