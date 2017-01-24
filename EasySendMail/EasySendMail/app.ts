import express = require('express');
import routes = require('./routes/index');
import user = require('./routes/user');
import http = require('http');
import path = require('path');

var app = express();

// all environments
app.set('port', process.env.PORT || 3000);
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');
app.use(express.favicon());
app.use(express.logger('dev'));
app.use(express.json());
app.use(express.urlencoded());
app.use(express.methodOverride());
app.use(app.router);

import stylus = require('stylus');
app.use(stylus.middleware(path.join(__dirname, 'public')));
app.use(express.static(path.join(__dirname, 'public')));

// development only
if ('development' == app.get('env')) {
    app.use(express.errorHandler());
}

app.get('/', routes.index);
app.get('/users', user.list);

http.createServer(app).listen(app.get('port'), function () {
    console.log('Express server listening on port ' + app.get('port'));
});

//https://github.com/Microsoft/TypeScript/wiki/Modules

import odbc = require("odbc");
var db = new odbc.Database({ fetchMode: 3 });
var cn = "Driver=ODBC Driver;Server=nnnn.nnn.nnn.nnn;Database=DB_SOMEDB;UID=login;PWD=password;UseLongVarchar=Yes";

db.open(cn, function (err) {
    db.queryResult('SELECT * FROM TABLE_INTERVENTI WHERE ID = ?', [326],  function (err, result) {
        //now you have to use some async callback management
        //to fetch all your rows. But here we just get the first one:
        //Type = -1 ==> String
        //Type = 8 ==> Number
        //Type = [2..8] => Double
        //Type = [9, 91] => Date
        //Type = [10, 92] => Time
        //Type = [11, 93] => DateTime (Timestamp)
        //console.log(result.getColumnNamesSync());
        //var arrayColumns = result.getColumnNamesSync();
        //console.log(arrayColumns);
        //for (var i = 0; i < arrayColumns.length; i++) {
        //    var row = arrayColumns[i];
        //    var fieldName = row.NAME;
        //    var fieldType = row.TYPE;
        //    console.log("fieldName: " + fieldName);
        //    console.log("fieldType: " + fieldType);
        //}

        var arrayColumns = result.getColumnNamesSync();
        var data = result.fetchSync();
        while (data != null) {
            for (var i = 0; i < arrayColumns.length; i++) {
                var fieldName = arrayColumns[i].NAME;
                var fieldType = arrayColumns[i].TYPE;
                console.log("name: " + fieldName + "<==> type: " + fieldType + "<==> value: " + data[i]);
            }
            data = result.fetchSync();
        }
        //result.fetch(function (err, data) {
        //    //the first record
        //    //console.log(data);
        //    var arrayColumns = result.getColumnNamesSync();
        //    var arrayAll = data;
        //    for (var i = 0; i < arrayAll.length; i++) {
        //        var fieldName = arrayColumns[i].NAME;
        //        var fieldType = arrayColumns[i].TYPE;
        //        var value = arrayAll[i];
        //        console.log("name: " + fieldName + "<==> type: " + fieldType + "<==> value: " + value);
        //    }
        //});
        //result.fetchAll(function (err, rows) {
        //    var arrayColumns = result.getColumnNamesSync();
        //    var arrayAll = rows[0];
        //    for (var i = 0; i < arrayAll.length; i++) {
        //        var fieldName = arrayColumns[i].NAME;
        //        var fieldType = arrayColumns[i].TYPE;
        //        var value = arrayAll[i];
        //        console.log("name: " + fieldName + "<==> type: " + fieldType + "<==> value: " + value);
        //    }
        //    //console.log(rows);
        //    //[ [ 1, 2 ] ]
        //});
    });
});
db.close(function () {
    console.log('done');
});

//db.open(cn, function (err) {
//    if (err)
//        return console.log("start err: " + err);

//    db.query('SELECT * FROM TABLE_INTERVENTI WHERE ID = ?', [326], function (err, data) {
//        //db.query("SELECT * FROM TABLE_PROBLEMI_EVIDENZIATI", function (err, data) {
//        if (err)
//            console.log("err: " + err);

//        console.log("data: " + JSON.stringify(data));

//        db.close(function () {
//            console.log('done');
//        });
//    });
//});
