  
var map = L.map('map').setView([42.32, -71.09], 13);//[-41.2858, 174.7868], 13);
mapLink = 
    '<a href="http://openstreetmap.org">OpenStreetMap</a>';
L.tileLayer(
    'http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; ' + mapLink + ' Contributors',
    maxZoom: 18,
    }).addTo(map);
        
  /* Initialize the SVG layer */
  map._initPathRoot()    

  /* We simply pick up the SVG from the map object */
  var svg = d3.select("#map").select("svg"),
  g = svg.append("g");
  
  d3.json("/routes", function(collection) {
    var route = collection["39300"].route,
        old_stops = collection["39300"].old_stops,
        new_stops = collection["39300"].new_stops;

    var route_points = [];
    route.geometry.coordinates.forEach(function(d) {
      route_points.push({"point":{"LatLng": new L.LatLng(d[0], d[1])}});
    });

    var old_stop_points = [];
    old_stops.geometry.coordinates.forEach(function(d) {
      old_stop_points.push({"stop":{"LatLng": new L.LatLng(d[0], d[1])}});
    });

    var new_stop_points = [];
    new_stops.geometry.coordinates.forEach(function(d) {
      new_stop_points.push({"stop":{"LatLng": new L.LatLng(d[0], d[1])}});
    });

    var links = [];
    for(var i = 0; i < route.geometry.coordinates.length-1; i++) {
      p1 = route.geometry.coordinates[i];
      p2 = route.geometry.coordinates[i+1];

      latlon1 = new L.LatLng(p1[0], p1[1]);
      latlon2 = new L.LatLng(p2[0], p2[1]);
      links.push({"link":{"LatLng": [latlon1, latlon2]}});
    }

    var links_svg = g.selectAll("links")
          .data(links)
          .enter().append("line")
          .style("stroke", "black") 
          .style("stroke-width", 4) 
          .style("opacity", 1);
    var route_svg = g.selectAll("point")
      .data(route_points)
      .enter().append("circle")
      .style("stroke", "black")  
      .style("opacity", 1) 
      .style("fill", "blue")
      .attr("r", 1);  
    
    var old_stops_svg = g.selectAll("oldstops")
      .data(old_stop_points)
      .enter().append("circle")
      .style("stroke", "black")  
      .style("opacity", 1) 
      .style("fill", "#ffc091")
      .attr("r", 5);

    var new_stops_svg = g.selectAll("newstops")
      .data(new_stop_points)
      .enter().append("circle")
      .style("stroke", "black")  
      .style("opacity", 1) 
      .style("fill", "#ff6e00")
      .attr("r", 5);

    

    map.on("viewreset", update);
    update();

    function update() {
      route_svg.attr("transform", 
      function(d) {
        return "translate("+ 
          map.latLngToLayerPoint(d.point.LatLng).x +","+ 
          map.latLngToLayerPoint(d.point.LatLng).y +")";
        }
      )

      old_stops_svg.attr("transform", 
      function(d) {
        return "translate("+ 
          map.latLngToLayerPoint(d.stop.LatLng).x +","+ 
          map.latLngToLayerPoint(d.stop.LatLng).y +")";
        }
      )

      new_stops_svg.attr("transform", 
      function(d) {
        return "translate("+ 
          map.latLngToLayerPoint(d.stop.LatLng).x +","+ 
          map.latLngToLayerPoint(d.stop.LatLng).y +")";
        }
      )

      links_svg.attr("x1", 
        function(d) { return map.latLngToLayerPoint(d.link.LatLng[0]).x
      });
      links_svg.attr("y1", 
        function(d) { return map.latLngToLayerPoint(d.link.LatLng[0]).y
      });
      links_svg.attr("x2", 
        function(d) { return map.latLngToLayerPoint(d.link.LatLng[1]).x
      });
      links_svg.attr("y2", 
        function(d) { return map.latLngToLayerPoint(d.link.LatLng[1]).y
      });
    }
  })