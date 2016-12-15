
function reduceAllocation(routes){
	//recompute optimal for each route after reducing allocation k by 1; routes = object

	var optKey = '';
	var bestScore = Number.MAX_VALUE;

	//find key which results in the smallest score increase by decreasing allocation
	for(var key in routes){
		var scores = routes.key.scores;
		var k = routes.key.optimal - 1;
		if(k - 1 >= 0){
			if ((scores[k - 1] - scores[k]) < bestScore) {
				bestScore = (scores[k - 1] - scores[k]);
				optKey = key;
			}
		}
	}

	if (optKey != '') {
		routes.optKey.optimal -= 1;
	}

	return true;
}

function increaseAllocation(routes){
	//recompute optimal for each route after increaseing allocation k by 1; routes = object

	var optKey = '';
	var bestScore = 0;

	//find key which results in the largest score decrease by decreasing allocation
	for(var key in routes){
		var scores = routes.key.scores;
		var k = routes.key.optimal - 1;

		if ((scores[k + 1] - scores[k]) < bestScore) {
			bestScore = (scores[k + 1] - scores[k]);
			optKey = key;
		}
		
	}

	if (optKey != '') {
		routes.optKey.optimal += 1;
	}

	return true;
}


function x(routes,key) { return routes.key.optimal; }
function y(routes,key) { return routes.key.scores[routes.key.optimal - 1];}
function radius(routes,key) { return routes.key.numStops; }


