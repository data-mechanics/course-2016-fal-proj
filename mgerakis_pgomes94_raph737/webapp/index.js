var express = require('express')
var app = express()
var path = require('path')
var mongodb = require('mongodb')



app.use(express.static(__dirname + '/'));

const api = express.Router();

api.get('/hospitals', function (req, res) {
	const cb = docs => res.send({'hospitals': docs});
	getCollectionData(cb,"mgerakis_pgomes94_raph737.hospital_locations");
})

api.get('/crimes', function(req,res){
	const cb = docs => res.send({'crimes': docs});
	getCollectionData(cb,"mgerakis_pgomes94_raph737.crime_locations")
})

api.get('/mbtastops', function(req,res){
	const cb = docs => res.send({'mbta_stops': docs});
	getCollectionData(cb,"mgerakis_pgomes94_raph737.mbta_stop_locations")
})

api.get('/policestations', function(req,res){
	const cb = docs => res.send({'policestations': docs});
	getCollectionData(cb,"mgerakis_pgomes94_raph737.police_station_locations")
})

api.get('/optimalcoords', function(req,res){
	const cb = docs => res.send({'optimalcoord': docs});
	getCollectionData(cb,"mgerakis_pgomes94_raph737.optimal_coord")
})

api.get('/trafficlocs', function(req,res){
		const cb = docs => res.send({'trafficlocs': docs});
	getCollectionData(cb,"mgerakis_pgomes94_raph737.traffic_locations")
})

app.use('/api', api);

app.get('/', function(req,res){
	res.sendFile("./index.html")
})


app.listen(3000, function () {
  console.log('Example app listening on port 3000!')
})








function getCollectionData(cb,collectionName){
	var MongoClient = mongodb.MongoClient;
	var url = "mongodb://localhost:27017/repo"

	MongoClient.connect(url,function (err, db) {
	  if (err) {
	  	console.log('Unable to connect to the mongoDB server. Error:', err);
	  	cb(err);
	  }else {
	    console.log('Connection established to', url);
	    var collection = db.collection(collectionName);
	    collection.find({}).toArray((err, docs) => {
	    	cb(docs);
	    });
	  }});
	}
