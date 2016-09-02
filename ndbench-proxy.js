var express = require('express');
var app = express();

var token = '[{"token":"2110532567","hostname":"localhost" ,"dc":"local-dc","ip":"127.0.0.1","zone":"local-dc","location":"local-dc"}]';

app.get('/REST/v1/admin/cluster_describe', function (req, res) {
  res.send(token);
});

app.listen(8081, function () {
  console.log('Serving: /REST/v1/admin/cluster_describe -> ' + token);
});

