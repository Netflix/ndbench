var express = require('express');
var app = express();

var token = '[{"token":"1383429731","hostname":"localhost" ,"dc":"local-dc","ip":"127.0.0.1","zone":"us-east-1c","location":"us-east-1c"}]';

app.get('/REST/v1/admin/cluster_describe', function (req, res) {
  res.send(token);
});

app.listen(8081, function () {
  console.log('Serving: /REST/v1/admin/cluster_describe -> ' + token);
});

