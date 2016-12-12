var express = require('express')
var bodyParser = require('body-parser');
var app = express()
var path = require('path')
var mongodb = require('mongodb')
const exec = require('child_process').execSync;



app.use(express.static(__dirname + '/'));

app.use(bodyParser.urlencoded({ extended: false }));
 
// parse application/json
app.use(bodyParser.json());

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

api.get('/clusters', (req, res) => {
  getCollectionData(docs => res.send({'c_clusters': docs.filter(doc => doc.proximity === 'C'), 'f_clusters': docs.filter(doc => doc.proximity === 'F')}), 'mgerakis_pgomes94_raph737.proximity_cluster_centers');
})

api.get('/trafficlocs', function(req,res){
		const cb = docs => res.send({'trafficlocs': docs});
	getCollectionData(cb,"mgerakis_pgomes94_raph737.traffic_locations")
})

app.use('/api', api);

app.get('/', function(req, res){
	res.sendFile("./index.html")
});
app.get('/mikesPark', (req, res) => {
  res.sendFile(__dirname + '/score.html');
});

app.get('/score', (req, res) => {
  const cb = docs => res.json({scores:  docs});
  getCollectionData(cb, 'mgerakis_pgomes94_raph737.hospital_scores');
});

app.post('/score', (req, res) => {
  const data = {
    'identifier': req.body.name,
    'location': [
      parseInt(req.body['latitude']), parseInt(req.body['longitude'])
    ]
  };
	const MongoClient = mongodb.MongoClient;
	const url = "mongodb://localhost:27017/repo"

  MongoClient.connect(url)
  .then(db => {
    const locations = db.collection('mgerakis_pgomes94_raph737.hospital_locations')
    return locations.insertOne(data)
  })
  .then(_ => exec('python3 ./pysparkMapReduceJob.py', (err, stdout, stderr) => {
    if (err) {
      throw err;
    }
  }))
  .then(_ => MongoClient.connect(url))
  .then(db => {
    const scores = db.collection('mgerakis_pgomes94_raph737.hospital_scores')
    scores.find({'identifier': req.body.name}).toArray((err, docs) => {
      res.json({score: docs[0].score});
    })
  })
  .catch(err => res.json({err}));
  // run pyspark code
});


app.listen(3000, function () {
  console.log('Example app listening on port 3000!')
})


function getCollectionData(cb, collectionName){
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
