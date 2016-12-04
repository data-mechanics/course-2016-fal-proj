/*
This javascript file performs a kmeans on a set of weighted points in order to determine
clustered regions of interest. The kmeans is then reprojected onto the route to determine an optimal allocation of bus stops that covers the most regions.
We define an algorithm in order to perform kmeans on a set of weighted points

Initial means will be bus stops with a list of routes they could be on
(stop_id, coordinates)

Points will be (weight, coordinates)
The following weights are assigned to different types of points

      Hubway Station: bike capacity
              T stop: 50
  Commuter Rail stop: 50

As proof of concept we test our algorithm on Route 99800
*/

function _dist(u, v) {
    return Math.pow(u[0] - v[0], 2) + Math.pow(u[1] - v[1], 2);
}

function _dot(u, v) {
    return u[0]*v[0] + u[1]*v[1];
}

function _scale(v, c) {
    return [c*v[0], c*v[1]];
}

function _add(u, v) {
    return [u[0] + v[0], u[1] + v[1]];
}

// Finds the minimum projection 
function _findnorm(p, pline) {
    var min_norm = 0;
    for(var i = 1; i < pline.length; i++) {
        var q0 = pline[i-1]; var q1 = pline[i];
        q = [q1[0]-q0[0], q1[1]-q0[1]];

        var c = dot(p, q) / dist(q, [0,0]);
        var proj = scale(q, c);
        var norm = add(proj, scale(p, -1));
        if(i == 1) {
            min_norm = norm;
        }
        else {
            if(dist(norm, [0,0]) < dist(min_norm, [0,0])) {
                min_norm = norm;
            }
        }
    }
    return min_norm;
}


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
var MR = "alaw_markbest_tyroneh.MR";

// Initialize the database of means

dropPerm(M);
createPerm(M);

var route_id = 99800
var route = db.alaw_markbest_tyroneh.BusRoutes.find({"properties.ctps_id":route_id}).map(function(u) {return u})[0];

var means = [];

for(var i = 0; i < route.properties.route_stops.length; i++) {
    var lat = Math.random() * (42.41-42.23) + 42.23;
    var lon = Math.random() * (71.18 - 70.993) - 71.18;
    means.push({"coordinates":[lat, lon]});
}
db[M].insert(means);

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
        if((lat >= 42.23) && (lat <= 42.41) && (lon >= -71.18) && (lon <= -70.993)) {
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
        if(this.properties.ctps_id == rid) {
            emit(this.properties.ctps_id, {
                "paths": [this.geometry.coordinates]
            });
        }
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
    { scope: {rid:route_id},
      out: R}
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

    db.alaw_markbest_tyroneh.MPD2.mapReduce(
        function() {
            var coords = this.value.p.coordinates;
            var weight = this.value.p.weight;
            var weighted_coords = [coords[0]*weight, coords[1]*weight];
            emit(this.value.m._id, {
                "coordinates": weighted_coords,
                "c": weight
            })
        },
        function(k, vs) {
            var lat = 0, lon = 0, c= 0;
            vs.forEach(function(v) {
                lat += v.coordinates[0];
                lon += v.coordinates[1];
                c += v.c;
            });
            return {"coordinates":[lat, lon], "c":c};
        },
        { finalize: function(k, v) { return {"_id": k, "coordinates": [v.coordinates[0]/v.c, v.coordinates[1]/v.c]}; },
          out: M
        }
    );

    flatten(M);
    db.alaw_markbest_tyroneh.M.mapReduce(
        function() { emit("hash", {hash: this.coordinates[0] + this.coordinates[1]}); },
        function(k, vs) {
            var hash = 0;
            vs.forEach(function(v) {
                hash += v.hash;
            });
            return {"hash": hash};
        },
        {out: "alaw_markbest_tyroneh.HASHNEW"}
    );
    var o = db.alaw_markbest_tyroneh.HASHOLD.find({}).limit(1).toArray()[0].value.hash;
    var n = db.alaw_markbest_tyroneh.HASHNEW.find({}).limit(1).toArray()[0].value.hash;
    print(o);
    print(n);
    print(iter);
    iter++;
}
while(o != n && iter < 20);

prod(M, R, MR);
dropPerm("alaw_markbest_tyroneh.NewStops");
createPerm("alaw_markbest_tyroneh.NewStops");

db.alaw_markbest_tyroneh.MR.mapReduce(
    function() {
        var m = this.left
        var route = this.right
        if(route._id == rid) {
            route.paths.forEach(function(path) {
                    var min_norm_for_route = findnorm(m.coordinates, path);
                    emit(m._id, {
                        "min_norm": min_norm_for_route,
                        "coordinates":m.coordinates
                    });
            });
        }
    },
    function(k, vs) {
        var min_norm = vs[0].min_norm;
        vs.forEach(function(stop) {
            min_norm = Math.min(stop.min_norm, min_norm);
        });
        return {"min_norm": min_norm, "coordinates":vs[0].coordinates};
    },
    { scope: {rid:route_id, add:_add, findnorm:_findnorm, scale:_scale, dot:_dot, dist:_dist},
      finalize: function(k, v) {
            return {
                "_id":k,
                "coordinates":add(v.coordinates, v.min_norm),
                "norm":v.min_norm
            };},
      out: "alaw_markbest_tyroneh.NewStops" }
);

flatten("alaw_markbest_tyroneh.NewStops");

//Drop all intermediate datasets
var intermediate = [M, P, R, MP, MPD, MPD2, MR]
dropPerm("alaw_markbest_tyroneh.HASHOLD");
dropPerm("alaw_markbest_tyroneh.HASHNEW");
intermediate.forEach(function(v) {
    dropPerm(v);
});
