var express = require('express')
var app = express()
var path = require('path')
var mongodb = require('mongodb')


// mongoose.connect(url);
// var Hospitals = new mongoose.Schema({
//         location: [Number,Number],
//         identifier: String
// });

// var Hospital = mongoose.model('Hospitals', Hospitals,"mgerakis_pgomes94_raph737_hospital_locations");




app.use(express.static(__dirname + '/'));

app.get('/hospitals', function (req, res) {
	const cb = docs => res.send({'hospitals': docs});
	getCollectionData(cb,"mgerakis_pgomes94_raph737.hospital_locations");
});

app.get('/crimes', function(req,res){
	const cb = docs => res.send({'crimes': docs});
	getCollectionData(cb,"mgerakis_pgomes94_raph737.crime_locations")
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
