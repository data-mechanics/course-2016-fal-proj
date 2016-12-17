(function() {
	var margin = { top: 0, left: 0, right: 0, bottom: 0}
	var height = 400 - margin.top - margin.bottom
	var width = 800 - margin.left - margin.right


	var svg = d3.select("#map")
		.append("svg")
		.attr("height", height + margin.top + margin.bottom)
		.attr("width", width + margin.left + margin.right)
		.append("g")
		.attr("transform", "translate(" + margin.left + "," + margin.top + ")");
		


	d3.queue()
		.defer(d3.json, "zips.json")
		.await(ready)




	// var projection = d3.geoMercator()
	// 	.translate([width / 2, height / 2]) // center it

	var projection = d3.geoMercator()
		.translate([width/2, height/2])

	var path = d3.geoPath()
		.projection(projection)





	function ready(error, data) {
		console.log(data);

		// every time you pull out topojson
		var zips = topojson.feature(data, data.objects.zips).features
		console.log(zips);

		svg.selectAll(".zip")
			.data(zips)
			.enter().append("path")
			.attr("class", "zip")
			.attr("d", path)
			.style("color", "red");



	}


})();