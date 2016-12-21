var express = require('express');
var router = express.Router();
//mongostuff
var Db = require('mongodb').Db;
var Server = require('mongodb').Server;


//Get data from MongoDB
var schools = [];
var school_coords = [];

var hospitals = [];
var hospital_coords = [];

var crimes = [];
var crime_coords = [];

var property_coords = [];

var neighborhood_scores = [];



var db = new Db('repo', new Server('localhost', 27017));
db.open(function(err, db) {

    if (err) { console.log ('Cannot connect to mongo, error message : ' + error); }

    db.collection('jyaang_robinliu106.school').find().toArray( function(err,result) {

        if (err) { console.log ('Error message : ' + error); }

        for (var i = 0; i < result.length; i++) {
            //schools has [schoolName, coords]
            schools.push( [ result[i]['schoolName'] , result[i]['coord'] ]);
            //coords is just [ coords ]
            school_coords.push( result[i]['coord']);
        }
    });

    db.collection('jyaang_robinliu106.hospital').find().toArray( function(err,result) {

        if (err) { console.log ('Error message : ' + error); }

        for (var i = 0; i < result.length; i++) {
            hospitals.push( [ result[i]['hospitalName'] , result[i]['coord'] ]);
            hospital_coords.push( result[i]['coord']);
        }
    });

    db.collection('jyaang_robinliu106.crime').find().toArray( function(err,result) {

        if (err) { console.log ('Error message : ' + error); }

        for (var i = 0; i < result.length; i++) {
            crimes.push( [ result[i]['crimeName'] , result[i]['coord'] ]);
            crime_coords.push( result[i]['coord']);
        }
    });

    db.collection('jyaang_robinliu106.neighborhood_scores').find().toArray( function(err,result) {

        if (err) { console.log ('Error message : ' + error); }

        for (var i = 0; i < result.length; i++) {
            neighborhood_scores.push( [ result[i]['neighborhood'] , result[i]['score'] , result[i]['property_value'] , result[i]['crime_rate'] , result[i]['hosptial_count'] , result[i]['school_count'] ]);
        }
    });

    db.collection('jyaang_robinliu106.property').find().toArray( function(err,result) {

        if (err) { console.log ('Error message : ' + error); }

        for (var i = 0; i < result.length; i++) {
            property_coords.push( result[i]['coord'] );
        }
    });

});


//console.log(neighborhood_scores.length);



//Render Index Page
router.get('/', function(req, res){
  res.render('index', {
    title: 'CS 591 jyaang_robinliu',
    schools: JSON.stringify(schools),
    school_coords: JSON.stringify(school_coords),
    hospitals: JSON.stringify(hospitals),
    hospital_coords: JSON.stringify(hospital_coords),
    crimes: JSON.stringify(crimes),
    crime_coords: JSON.stringify(crime_coords),
    neighborhood_scores: JSON.stringify(neighborhood_scores),
    property_coords: JSON.stringify(property_coords)
  });
});

//Render About Page
router.get('/about', function(req, res){
  res.render('about', {
    title: 'About Page'
  });
});

module.exports = router;
