var settings = {
  "async": true,
  "crossDomain": true,
  "url": "",
  "method": "GET"
}

endpoint = "http://localhost:5000/patrols_coordinates?"

var map

function hasErrors(min_distance, min_patrols, max_patrols, crime_codes) {
  $('.has-error').removeClass('has-error')
  $('.show').addClass('hide')
  $('.show').removeClass('show')

  if (min_patrols == '') {
    $('#min-patrols-group').addClass('has-error')
    $('#help-block-1').removeClass('hide')
    $('#help-block-1').addClass('show')
    return true
  }

  if (max_patrols == '') {
    $('#max-patrols-group').addClass('has-error')
    $('#help-block-2').removeClass('hide')
    $('#help-block-2').addClass('show')
    return true
  }

  if (min_distance == '') {
    $('#min-distance-group').addClass('has-error')
    $('#help-block-3').removeClass('hide')
    $('#help-block-3').addClass('show')
    return true
  }

  if (crime_codes == null) {
    $('#crime-codes-group').addClass('has-error')
    $('#help-block-4').removeClass('hide')
    $('#help-block-4').addClass('show')
    return true
  }

  return false
}

function getCrimeCodes(crime_codes) {
  crime_code_list = "codes="

  for (var i = 0; i < crime_codes.length; i++) {
    if (i == crime_codes.length -1) {
      crime_code_list = crime_code_list + crime_codes[i]
    } else {
      crime_code_list = crime_code_list + crime_codes[i] + ','
    }
  };

  return crime_code_list
}

function findPatrols() {

  $('#execute').button('loading')

  min_distance = $('#min-distance').val();
  min_patrols = $('#min-patrols').val();
  max_patrols = $('#max-patrols').val();
  crime_codes = $('#crime-codes').val();

  if (hasErrors(min_distance, min_patrols, max_patrols, crime_codes)) {
    $('#execute').button('reset');
    return
  }

  max_patrols = "max_patrols=" + max_patrols + '&'
  min_patrols = "min_patrols=" + min_patrols + '&'
  min_distance = "min_distance=" + min_distance + '&'
  crime_codes = getCrimeCodes(crime_codes)

  settings.url = endpoint + min_patrols + max_patrols + min_distance + crime_codes
  
  $.ajax(settings).done(function (response) {
    $('#execute').button('reset');
    fillMap(response.data)
  }).fail(function(response) {
    errors = response.responseJSON.errors;
    toastr.error(errors[0].msg)
    $('#execute').button('reset');
  });
}

function fillMap(data) {

  if (!data.coordinates) {
    toastr.error("It is not feasible to locate patrols within the specified parameters");
  }

  d3.selectAll(".marker").remove()

  var overlay = new google.maps.OverlayView();

  // Add the container when the overlay is added to the map.
  overlay.onAdd = function() {
    var layer = d3.select(this.getPanes().overlayLayer).append("div")
        .attr("class", "patrols");

    // Draw each marker as a separate SVG element.
    // We could use a single SVG, but what size would it have?
    overlay.draw = function() {
      var projection = this.getProjection(),
          padding = 0;

      var marker = layer.selectAll("svg")
          .data(d3.entries(data.patrol_coordinates))
          .each(get_coordinates) // update existing markers
          .enter().append("svg")
          .each(get_coordinates)
          .attr("class", "marker");

       marker.append("image")
          .attr("xlink:href", "http://www.free-icons-download.net/images/police-car-icon-86554.png")
          .attr("cx", padding)
          .attr("cy", padding)
          .attr("width", 25)
          .attr("height", 25);

      function get_coordinates(d) {
        //console.log(d)
        d = new google.maps.LatLng(d.value[0], d.value[1]);
        d = projection.fromLatLngToDivPixel(d);
        return d3.select(this)
            .style("left", (d.x - padding) + "px")
            .style("top", (d.y - padding) + "px");
      }
    };
  };

  // Bind our overlay to the map…
  overlay.setMap(map);
}

function initMap() {
  // Create the Google Map…
  map = new google.maps.Map(d3.select("#map").node(), {
    zoom: 12,
    center: new google.maps.LatLng(42.349967, -71.105456),
    mapTypeId: google.maps.MapTypeId.TERRAIN
  });
}