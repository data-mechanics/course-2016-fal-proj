db.loadServerScripts();

//store transformed property data in collection "PropertyGeoJSONs"

dropPerm("alaw_markbest_tyroneh.BusesJSON");
createPerm("alaw_markbest_tyroneh.BusesJSON");

function flatten(X) {
	//reduces id:{id,values} to id: {values}
    db[X].find().forEach(function(x) { db[X].update({_id: x._id}, x.value); });
}

db.alaw_markbest_tyroneh.TimedBuses.mapReduce(
	//calculate times, coordinate data for each bus route 
	function() {
        //map to routes and times with coordinate data
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
        //reduce to routes and lists of times and coorinates
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
    //calculate km per millisecond using coordinate data
    function() {
        //User haversine formula to calculate distance
        var routeTag = this._id.route;
        var vid = this._id.id;
        var times = this.times;
        var vel = 0.000001;

        var R = 6371; // metres radius of Earth

        for(var i = 1; i < times.length; i++) {
            var bus0 = times[i-1]; 
            var bus1 = times[i];

            var lat1 = (bus0.lat) * (Math.PI / 180);
            var lat2 = (bus1.lat) * (Math.PI / 180);

            var deltalat = (bus1.lat - bus0.lat) * (Math.PI / 180);
            var deltalong = (bus1.lon - bus0.lon) * (Math.PI / 180);

            //haversine formula
            var a = Math.sin(deltalat/2) * Math.sin(deltalat/2) + Math.cos(lat1) * Math.cos(lat2) * Math.sin(deltalong/2) * Math.sin(deltalong/2);
            var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
            var d = R * c;

            //convert seconds to hours
            var t = (bus1.timestamp - bus0.timestamp) / 3600.0;
            
            //kmph
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

dropPerm("alaw_markbest_tyroneh.BusesJSON");

// b.alaw_markbest_tyroneh.BusesJSON.mapReduce(
//     function() {
//         var routeTag = this._id.route;
//         var vid = this._id.id;
//         var times = this.times;
//         var vel = 0.000001;

//         for(var i = 1; i < times.length; i++) {
//             var bus0 = times[i-1]; var bus1 = times[i];

//             var t = bus1.timestamp - bus0.timestamp;

//             var lat_diff = Math.pow(bus1.lat - bus0.lat, 2);
//             var lon_diff = Math.pow(bus1.lon - bus0.lon, 2);
//             var d = Math.pow(lat_diff + lon_diff, 0.5);
            
//             var vel = d/t;
//         }

//         emit(routeTag, {
//             "total_vel": vel,
//             "vel": [vel],
//             "count": 1
//         });
//     },
//     function(k, vs) {
//         var total_vel = 0;
//         var vel = [];
//         var count = 0;

//         vs.forEach(function(bus) {
//             total_vel += bus.total_vel;
//             bus.vel.forEach(function(v) {
//                 vel.push(v);
//             });
//             count += bus.count;
//         });
//         return {"total_vel": total_vel, "vel": vel, "count": count};
//     },
//     {out:{merge:"alaw_markbest_tyroneh.AvgRouteVelocity"}}
// );