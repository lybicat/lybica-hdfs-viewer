var restify = require('restify');
var config = require('./config');

var server = restify.createServer({
    name: 'hdfs-viewer',
    version: '1.0.0'
});

server.use(restify.acceptParser(server.acceptable));
server.use(restify.queryParser());
server.use(restify.bodyParser());

server.use(function(req, res, next) {
    res.header('Access-Control-Allow-Origin', '*');
    return next();
});


server.get('/hdfs/:filePath', function(req, res, next) {
  res.send(404, {err: 'file "' + req.params.filePath + '" not found'});
  return next();
});


server.listen(config.PORT, function() {
    console.log('%s listening at %s', server.name, server.url);
});
