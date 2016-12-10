const express = require('express');
const app = express();
const bodyParser = require('body-parser')
const MongoClient = require('mongodb').MongoClient








var db

MongoClient.connect('mongodb://localhost:27017/repo', (err, database) => {
  if (err) return console.log(err)
  db = database
  app.listen(process.env.PORT || 3000, () => {
    console.log('listening on 3000')
  })
})

app.set('view engine', 'ejs')
app.use(bodyParser.urlencoded({extended: true}))
app.use(bodyParser.json())
app.use(express.static('public'))



app.get('/', (req, res) => {
//  res.sendFile('/Users/hongyuzhou/Desktop/411/project' + '/goog.html')
  db.collection("aydenbu_huangyh.statistic_data").find().toArray((err, result) => {
    if (err) return console.log(err)

//    console.log(filter(result))
    res.render('demo.ejs', {tdata: result})
  })
})

app.post('/tdata', (req, res) => {

  db.collection("411.foodTruckSchedual").find(req.body).toArray((err, result) => {
    if (err) return console.log(err)
//    console.log('saved to database')
    res.render('index.ejs', {tdata: result})
  })
})

// Note: request and response are usually written as req and res respectively.
const express = require('express');
const app = express();
const bodyParser = require('body-parser')
const MongoClient = require('mongodb').MongoClient








var db

MongoClient.connect('mongodb://localhost:27017/repo', (err, database) => {
  if (err) return console.log(err)
  db = database
  app.listen(process.env.PORT || 3000, () => {
    console.log('listening on 3000')
  })
})

app.set('view engine', 'ejs')
app.use(bodyParser.urlencoded({extended: true}))
app.use(bodyParser.json())
app.use(express.static('public'))



app.get('/', (req, res) => {
//  res.sendFile('/Users/hongyuzhou/Desktop/411/project' + '/goog.html')
  db.collection("aydenbu_huangyh.statistic_data").find().toArray((err, result) => {
    if (err) return console.log(err)

//    console.log(filter(result))
    res.render('demo.ejs', {tdata: result})
  })
})

app.post('/tdata', (req, res) => {

  db.collection("411.foodTruckSchedual").find(req.body).toArray((err, result) => {
    if (err) return console.log(err)
//    console.log('saved to database')
    res.render('index.ejs', {tdata: result})
  })
})

// Note: request and response are usually written as req and res respectively.
