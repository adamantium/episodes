/* Required Modules
    express, mode-mongodb-native
*/
var express = require('express'),
    fs = require('fs'),
    mongodb = require('mongodb'),
    url = require('url');

// Global Objects(Created or Imported or Settings)
var app = express.createServer(),
    dbServer = new mongodb.Server("localhost", 27017, {});

var db = new mongodb.Db('episode', dbServer, {native_parser: true});

// App Configuration
app.configure(function () {
    app.use(express.logger('\x1b[33m:method\x1b[0m \x1b[32m:url\x1b[0m :response-time'));
    //app.use(express.bodyParser());
    //app.use(express.methodOverride());
    app.use(app.router);
    app.use(express.static(__dirname + '/public'));
});

app.configure('development', function () {
    app.use(express.errorHandler({ dumpExceptions: true, showStack: true }));
});

app.configure('production', function () {
    app.use(express.errorHandler());
});

// Routers
app.post('/episode/index', function (req, res) {
    req.on('data', function (chunk) {
        console.log(chunk.toString('utf8'));
        db.open(function (err, db) {
            if (err) throw err;
            var collection = new mongodb.Collection(db, 'us_index');
            collection.find({}, {}, function (err, cursor) {
                cursor.toArray(function(err, docs) {
                    console.dir(docs);
                });
            });
        });
    });
    res.send('okdfasdf');
});

app.get('/index', function (req, res) {
    db.open(function (err, db) {
        if (err) throw err;
        var collection = new mongodb.Collection(db, 'us_index');
        collection.find({'ccn': 'totalindexlist'}, {}, function (err, cursor) {
            cursor.toArray(function(err, docs) {
                // console.dir(docs[0]['list']);
                res.send(docs[0]['list']);
                db.close();
            });
        });
    });
});

app.listen(8000, 'dev.clique.kr');


