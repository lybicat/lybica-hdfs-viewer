var restify = require('restify');
var path = require('path');
var config = require('./config');
var hdfs = require('./hdfs');

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


// read
// TODO: 1. support normal file on hdfs
// 2. support zip file on fly
// 3. support file in zip file on fly
server.get('/hdfs', function(req, res, next) {
  hdfs.exists(req.params.path, function(fileExist) {
    if (!fileExist) {
      res.send(404, {err: 'file "' + req.params.path + '" not found'});
      return next();
    }
    var remoteStream = hdfs.createReadStream(req.params.path);
    res.setHeader('content-disposition', 'attachment; filename="' + path.basename(req.params.path) +'"');
    remoteStream.pipe(res);
    res.on('end', next);
  });
});


// write
server.post('/hdfs', function(req, res, next) {
  return next();
});


// delete
server.del('/hdfs', function(req, res, next) {
  return next();
});

server.listen(config.PORT, function() {
  console.log('%s listening at %s', server.name, server.url);
});

