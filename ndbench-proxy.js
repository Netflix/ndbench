/**
* @Author: Diego Pacheco
* @Since:  Oct/2o16
*
* This is a simple script proxy to provide seeds to dynomite.
* This script simulates dynomite-manager/florida in a very simple way(only for seeds) on the port 8081.
* Using this script you can point to any dynomite cluster whithout having to stop NDBench.
* Basically you just need to change the tokens and point to your dynomite cluster.
*
*
* How to install dependencies:
* 
* $ sudo npm install express -g
*
* In order to run:
*
* $ node ndbench-proxy.js
*
**/

var express = require('express');
var app = express();

var token = '[{"token":"1383429731","hostname":"localhost" ,"dc":"local-dc","ip":"127.0.0.1","zone":"us-east-1c","location":"us-east-1c"}]';

app.get('/REST/v1/admin/cluster_describe', function (req, res) {
  res.send(token);
});

app.listen(8081, function () {
  console.log('Serving: /REST/v1/admin/cluster_describe -> ' + token);
});
