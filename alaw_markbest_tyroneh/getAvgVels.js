db.loadServerScripts();

//store transformed property data in collection "PropertyGeoJSONs"

dropPerm("alaw_markbest_tyroneh.BusesJSON");
createPerm("alaw_markbest_tyroneh.BusesJSON");

function flatten(X) {
	//reduces id:{id,values} to id: {values}
    db[X].find().forEach(function(x) { db[X].update({_id: x._id}, x.value); });
}

db.alaw_markbest_tyroneh.TimedBuses.mapReduce(
	//map location data (lat,long) to geoJSON format
	function() {
        var routes = this.route;
        var timestamp = this.timestamp;
        routes.forEach(function(r) {
            var vehicles = r.vehicle;
            var routeTag = r['@tag'];
            if(vehicles) {
                var timeForRoute = r.lastTime['@time'];
                vehicles.forEach(function(v) {
                    var lat = parseFloat(v['@lat']);
                    var lon = parseFloat(v['@lon']);
                    emit({'route':routeTag, 'id':v['@id']}, {
                        "times":[{
                            "lat": lat,
                            "lon": lon,
                            "timestamp": timestamp
                        }]
                    });
                });
            };
        });
    },

	function(k, vs){
        var times = [];
        vs.forEach(function(v_times) {
            v_times["times"].forEach(function(v) {
                times.push(v);
            });
        });

        times = times.sort(function(a, b) {
            x = a['timestamp']; y = b['timestamp'];
            return ((x > y) ? 1 : ((x < y) ? -1 : 0));
        });

        return {"times":times};
    },
	{out:{merge:"alaw_markbest_tyroneh.BusesJSON"}}
);

flatten("alaw_markbest_tyroneh.BusesJSON");

dropPerm("alaw_markbest_tyroneh.AvgRouteVelocity");
createPerm("alaw_markbest_tyroneh.AvgRouteVelocity");

db.alaw_markbest_tyroneh.BusesJSON.mapReduce(
    function() {
        var routeTag = this._id.route;
        var vid = this._id.id;
        var times = this.times;
        var vel = 0.000001;

        for(var i = 1; i < times.length; i++) {
            var bus0 = times[i-1]; var bus1 = times[i];

            var t = bus1.timestamp - bus0.timestamp;

            var lat_diff = Math.pow(bus1.lat - bus0.lat, 2);
            var lon_diff = Math.pow(bus1.lon - bus0.lon, 2);
            var d = Math.pow(lat_diff + lon_diff, 0.5);
            
            var vel = d/t;
        }

        emit(routeTag, {
            "total_vel": vel,
            "vel": [vel],
            "count": 1
        });
    },
    function(k, vs) {
        var total_vel = 0;
        var vel = [];
        var count = 0;

        vs.forEach(function(bus) {
            total_vel += bus.total_vel;
            bus.vel.forEach(function(v) {
                vel.push(v);
            });
            count += bus.count;
        });
        return {"total_vel": total_vel, "vel": vel, "count": count};
    },
    {out:{merge:"alaw_markbest_tyroneh.AvgRouteVelocity"}}
);
