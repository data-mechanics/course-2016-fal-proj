var express = require('express');
var router = express.Router();
var models = require('../models/models');
var Lights = models.Lights;
var Crimes = models.Crimes;

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Express' });
});

router.get('/lights/:num', function(req,res,next) {
  Lights.find({}).limit(parseInt(req.params.num)).exec(function(err, lights){
    console.log(lights)
    var lightsMap = {};

    lights.forEach(function(light) {
      lightsMap[light.light_id] = light;
    });

    res.json(lightsMap); 
  });
});

router.get('/crimes/:num', function(req,res,next) {
  Crimes.find({}).limit(parseInt(req.params.num)).exec(function(err, crimes){
    var crimesMap = {};

    crimes.forEach(function(crime) {
      crimesMap[crime._id] = crime;
    });

    res.json(crimesMap); 
  });
});

module.exports = router;
