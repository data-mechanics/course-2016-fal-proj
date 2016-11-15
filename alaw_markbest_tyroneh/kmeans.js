/*
We define an algorithm in order to perform kmeans on a set of weighted points
Initial means will be bus stops with a list of routes they could be on
(stop_id, coordinates, route_list)

Points will be (weight, coordinates)
The following weights are assigned to different types of points

Residential Property: avg population count
 Commercial Property: 10
      Hubway Station: bike capacity
              T stop: 50
  Commuter Rail stop: 50

Modified kmeans
1. Find clusters for each mean
2. Update means
3. Project onto route
    for route in routes:
        for variant in route:
            find closest projection to mean
*/

function _dist(u, v) {
    return Math.pow(u[0] - v[0], 2) + Math.pow(u[1] - v[1], 2);
}

function dot(u, v) {
    return u[0]*v[0] + u[1]*v[1];
}

function scale(v, c) {
    return [c*v[0], c*v[1]];
}

function add(u, v) {
    return [u[0] + v[0], u[1] + v[1]];
}

db.system.js.save({ _id:"dist", value:_dist});

// Finds the minimum projection 
db.system.js.save({_id:"findnorm", value:function(p, pline) {
    var min_norm = 0;
    for(var i = 1; i < pline.length; i++) {
        var q0 = pline[i-1]; var q1 = pline[i];
        q = [q1[0]-q0[0], q1[1]-q0[1]];
        var proj = scale(dot(p, q) / _dist(q, [0,0]), q);
        var norm = add(proj, scale(p, -1));
        if(i == 1) {
            min_norm = norm;
        }
        else {
            min_norm = Math.min(norm, min_norm);
        }
    }
    return min_norm;
}});

db.loadServerScripts();

// Alternatively, find the minimum distance point

function flatten(X) {
	//reduces id:{id,values} to id: {values}
    db[X].find().forEach(function(x) { db[X].update({_id: x._id}, x.value); });
}

function prod(X, Y, XY) {
    dropPerm(XY); 
    createPerm(XY);
    db[X].find().forEach(function(x) {
        db[Y].find().forEach(function(y) {
            db[XY].insert({left:x, right:y});
        });
    });
}

// To makes the datasets easier to pass in as strings
var M = "alaw_markbest_tyroneh.M";
var P = "alaw_markbest_tyroneh.P";
var R = "alaw_markbest_tyroneh.R";
var MP = "alaw_markbest_tyroneh.MP";
var MPD = "alaw_markbest_tyroneh.MPD";
var MPD2 = "alaw_markbest_tyroneh.MPD2";
var M2 = "alaw_markbest_tyroneh.M2";
var M2R = "alaw_markbest_tyroneh.M2R";

// Initialize the database of means

dropPerm(M);
createPerm(M);

db.alaw_markbest_tyroneh.BusStops.mapReduce(
    function() {
        var props = this.properties;
        var coordinates = this.geometry.coordinates;
        var lat = coordinates[0]; var lon = coordinates[1];
        if((lat >= 42.23) && (lat <= 42.41) && (lon >= -71.18) && (lon <= -70.993)) {

        for(var i = 0; i < props.route_list.length; i++) {
        if(props.route_list[i] == 34400) {
        emit(props.stop_id, {
                "coordinates": coordinates,
                "route_list": props.route_list
            });
        break;
        }
        }
        }
    },
    function(k, vs) {},
    {out: M}
);
flatten(M);

dropPerm(P);
createPerm(P);
/*
// Initialize the database of weighted kmeans points
// Residential and Commercial property points
db.alaw_markbest_tyroneh.PropertyGeoJSONs.mapReduce(
    function() {
        var type = this.value.properties.type;
        var coords = this.value.geometry.coordinates;
        var lat = coords[0]; var lon = coords[1];
        var weight;
        if((lat >= 42.23) && (lat <= 42.41) && (lon >= -71.18) && (lon <= -70.993)) {
            var willEmit = true;

            if(type == "Residential") {
                weight = this.value.properties.population;
            }
            else if (type == "Commercial") {
                weight = 10;
            } // T 50 CR 60
            else {
                willEmit = false;
            }

            if(willEmit) {
                emit(this._id, {
                    'weight': weight,
                    'coordnates': this.value.geometry.coordinates
                });
            }
        }
    },
    function(k, vs) {},
    {out: {merge: P}}
);
*/

// Hubway, Subway, and Commuter Rail station coordinates
db.alaw_markbest_tyroneh.StationsGeoJSONs.mapReduce(
    function() {
        var type = this.value.properties.type;
        var weight;
        var coords = this.value.geometry.coordinates;
        var lat = coords[0]; var lon = coords[1];
        if((lat >= 42.3) && (lat <= 42.41) && (lon >= -71.10) && (lon <= -70.993)) {
            var willEmit = true; 
            if(type == "Hubway") {
                weight = this.value.properties.capacity;
            }
            else if (type == "Subway") {
                weight = 50;
            }
            else if (type == "Commuter Rail") {
                weight = 50;
            }
            else {
                willEmit = false;
            }

            emit(this._id, {
                    "weight": weight,
                    "coordinates": coords
                });
        }
    },
    function(k, vs) {},
    {out:{merge: P}}
);
flatten(P);

dropPerm(R);
createPerm(R);

db.alaw_markbest_tyroneh.BusRoutes.mapReduce(
    function() {
        emit(this.properties.ctps_id, {
            "paths": [this.geometry.coordinates]
        });
    },
    function(k, vs) {
        var paths = [];
        vs.forEach(function(v) {
            v.paths.forEach(function(path) {
                var newpath = [];
                path.forEach(function(p) {
                    var lat = p[0]; var lon = p[1];
                    if((lat >= 42.23) && (lat <= 42.41) && (lon >= -71.18) && (lon <= -70.993)) {
                        newpath.push(p);
                    } 
                });
                paths.push(newpath);
            });
        });
        return { "paths": paths }
    },
    {out: R}
);

flatten(R);

dropPerm("alaw_markbest_tyroneh.HASHOLD");
dropPerm("alaw_markbest_tyroneh.HASHNEW");
createPerm("alaw_markbest_tyroneh.HASHOLD");
createPerm("alaw_markbest_tyroneh.HASHNEW");

var iter = 0;
do {
    
    db.alaw_markbest_tyroneh.M.mapReduce(
        function() { emit("hash", {hash: this.coordinates[0] + this.coordinates[1]})},
        function(k, vs) {
            var hash = 0;
            vs.forEach(function(v) {
                hash += v.hash;
            }); 
            return {hash: hash};
        },
        {out: "alaw_markbest_tyroneh.HASHOLD"}
    );
    
    prod(M, P, MP);
    dropPerm(MPD);
    createPerm(MPD);
    db.alaw_markbest_tyroneh.MP.mapReduce(
        function() {
            emit(this._id, {
                "m":this.left,
                "p":this.right,
                "d":dist(this.left.coordinates,
                         this.right.coordinates)
                })
        },
        function() {},
        { scope:{dist:_dist},
          out: MPD}
    );
    
    flatten(MPD);
    dropPerm(MPD2);
    createPerm(MPD2);
    db.alaw_markbest_tyroneh.MPD.mapReduce(
        function() { emit(this.p._id, this); },
        function(k, vs) {
            var j = 0;
            vs.forEach(function(v, i) {
                if (v.d < vs[j].d)
                    j = i;
            });
            return vs[j];
        },
        {out: MPD2}
    );
    dropPerm(M2);
    createPerm(M2);
    db.alaw_markbest_tyroneh.MPD2.mapReduce(
        function() {
            var coords = this.value.p.coordinates;
            var weight = this.value.p.weight;
            var weighted_coords = [coords[0]*weight, coords[1]*weight];
            emit(this.value.m._id, {
                "coordinates": weighted_coords,
                "route_list": this.value.m.route_list,
                "c": weight
            })
        },
        function(k, vs) {
            print("key", k);
            var lat = 0, lon = 0, c= 0;
            vs.forEach(function(v) {
                lat += v.coordinates[0];
                lon += v.coordinates[1];
                c += v.weight;
            });
            return {"coordinates":[lat, lon], "route_list": vs[0].route_list, "c":c};
        },
        { finalize: function(k, v) { return {"_id": k, "coordinates": [v.coordinates[0]/v.c, v.coordinates[1]/v.c], "route_list":v.route_list}; },
          out: M2
        }
    );
/*
    prod(M2, R, M2R);
    db.alaw_markbest_tyroneh.M2R.mapReduce(
        function() {
            var m = this.left
            var r = this.right
            for(var i = 0; i < m.route_list.length; i++) {
                var route = m.route_list[i]
                if(r._id == route) {
                    var min_norm_for_route = findnorm(m.coordinates, route);
                    emit(m.stop_id, {"min_norm": min_norm_for_route, "route_list":m.route_list});
                    break;
                }
            }
        },
        function(k, vs) {
            var min_norm = vs[0].min_norm;
            vs.forEach(function(stop) {
                min_norm = min(stop.min_norm, min_norm);
            });
            return {"min_norm": min_norm, "route_list":vs[0].route_list};
        },
        { finalize: function(k, v) {
                return {
                    "_id":k,
                    "coordinates":add(v.coordinates, v.min_norm),
                    "route_list":v.route_list};
            },
          out: M }
    );
   */ 
    var o = 0;
    var n = 0;
    print('Ummmmmm');
}
while(o != n);

dropPerm("alaw_markbest_tyroneh.HASHOLD");
dropPerm("alaw_markbest_tyroneh.HASHOLD");
