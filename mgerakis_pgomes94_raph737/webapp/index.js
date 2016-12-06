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

app.get('/test', function (req, res) {
	res.send(getHospitalData(mongodb))
})

app.listen(3000, function () {
  console.log('Example app listening on port 3000!')
})


function getHospitalData(mongodb){
	var MongoClient = mongodb.MongoClient;

	var url = "mongodb://localhost:27017/repo"

	MongoClient.connect(url,function (err, db) {
	  if (err) {
	    console.log('Unable to connect to the mongoDB server. Error:', err);
	  }else {
	    console.log('Connection established to', url);
	    var collection = db.collection("mgerakis_pgomes94_raph737_hospital_locations");
	    console.log(collection);
	    collection.find({}).toArray(function(err,hospital){
	    	console.log(hospital)
	    });
	  }});
}
