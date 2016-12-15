/* ------------- Application Setup ------------- */

/**
 * Declare Dependencies
 */
var express = require('express');
var request = require('request');
var bodyParser = require('body-parser');
var mongo = require('mongodb');

// setup mongo client
var MongoClient = mongo.MongoClient;
var url = 'mongodb://asanentz_ldebeasi_mshop_sinichol:asanentz_ldebeasi_mshop_sinichol@localhost:27017/repo';


/**
 * Setup Express server
 */
var app = express();
app.use(bodyParser.json());
app.use(express.static(__dirname + '/assets'));
  app.engine('html', require('ejs').renderFile)
  app.set('view engine', 'ejs');


/* ------------- Application Routing ------------- */


app.get('/lookup', function(req,res) {
   res.render('lookup.html'); 
});

app.get('/data', function(req,res) {
   res.render('data.html'); 
});
/*
 * Given lat and lng coordinates, get all subway, bus, and hubway
 * data within a 1.1km (0.7mi) radius
 * Parameters:
 * coordinates (object): {
 *     lat (string): Latitude of location
 *     lng (string): Longitude of location
 * }   
 */
app.post('/getStops', function(req, res) {
    
    // first get some profile info
    var coordinates = req.body.coordinates;
    
    var lat = parseFloat(coordinates.lat).toFixed(2);
    var lng = parseFloat(coordinates.lng).toFixed(2);
    
    MongoClient.connect(url, function (error, db) {
        if (error) {
            res.setHeader('Content-Type', 'application/json');
            res.send(JSON.stringify({ success: false, message: error}));
            res.end(); 
        } else {
            
            var collection = db.collection('asanentz_ldebeasi_mshop_sinichol.transit');
                    
            // find the users's data
            collection.find().toArray(function(error, result) {
                
                if (error) {
                    res.setHeader('Content-Type', 'application/json');
                    res.send(JSON.stringify({ success: false, message: error}));
                    res.end(); 
                } else {
                    var busStops = {};
                    var tStops = {};
                    var hubStops = {};
                    
                    // this does the same processing as addressValue.py
                    for(var i in result) {
                        var stopLat = (parseFloat(result[i]['LATITUDE'])).toFixed(2);
                        var stopLng = (parseFloat(result[i]['LONGITUDE'])).toFixed(2);
                        
                        var type = result[i]['TYPE'];
                        var coords = [stopLat, stopLng];
                        
                        if (type == 'BUS') {
                            
                            if (busStops[coords]) {
                                busStops[coords] += 1;
                            } else {
                                busStops[coords] = 1;
                            }
                            
                        } else if (type == 'HUBWAY') {
                            
                            if (hubStops[coords]) {
                                hubStops[coords] += 1;
                            } else {
                                hubStops[coords] = 1;
                            }
                            
                        } else if (type == 'MBTA') {
                            
                            if (tStops[coords]) {
                                tStops[coords] += 1;
                            } else {
                                tStops[coords] = 1;
                            }
                            
                        }                 
                    }
                    
                    var coordsArray = [lat, lng];
                    
                    var result = {
                        bus: 0,
                        mbta: 0,
                        hubway: 0,
                        coords: lat + ', ' + lng
                    }
                    
                    if (busStops[coordsArray]) {
                        result.bus = busStops[coordsArray];
                    }
                    
                    if (hubStops[coordsArray]) {
                        result.hubway = hubStops[coordsArray];
                    }
                    
                    if (tStops[coordsArray]) {
                        result.mbta = tStops[coordsArray];
                    }
                                
                    res.setHeader('Content-Type', 'application/json');
                    res.send(JSON.stringify({ success: true, message: result}));
                    res.end(); 
                }
            });
        }
    });
});


console.log('Listening on 8888');
app.listen(8888);
