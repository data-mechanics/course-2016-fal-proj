var mongoose = require('mongoose');

// connect to db
var connect = 'mongodb://admin:example@localhost:27017/repo';
mongoose.connect(connect);

// check mongo connection
var db_connection = mongoose.connection;
db_connection.once('open', function callback () {
  console.log("DB Connected!");
});

// set up schema
var lightsSchema = mongoose.Schema({
  light_id:{
    type: Number,
    required: true
  },
  location: {
    type: {type: String},
    coordinates: {type: Array}
  }
})

var crimesSchema = mongoose.Schema({
  long: {type: Number},
  lat: {type: Number},
  incident_number: {type: String},
  offense_code_group: {type: String}
})

// exports
module.exports = {
  Lights: mongoose.model('alsk_yinghang.lights', lightsSchema),
  Crimes: mongoose.model('alsk_yinghang.crime_lights', crimesSchema)
};