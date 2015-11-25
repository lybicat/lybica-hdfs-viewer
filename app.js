var restify = require('restify');
var path = require('path');
var config = require('./config');
var hdfs = require('./hdfs');
var moment = require('moment');
var uuid = require('uuid');
var unzip = require('unzip2');

var server = restify.createServer({
  name: 'hdfs-viewer',
  version: '1.0.0'
});

server.use(restify.acceptParser(server.acceptable));
server.use(restify.queryParser());

server.use(function(req, res, next) {
  res.header('Access-Control-Allow-Origin', '*');
  return next();
});


function _getZipEntries(zipStream, callback) {
  // TODO: entries is not correct
  var entries = [];
  zipStream.pipe(unzip.Parse())
  .on('entry', function(entry) {
    entries.push({path: entry.path, size: entry.size});
    entry.autodrain();
  })
  .on('finish', function() {
    callback(entries);
  });
}


String.prototype.startswith = function(s) {
  return this.substr(0, s.length) === s;
};


String.prototype.endswith = function(s) {
  return this.substr(this.length - s.length, this.length) === s;
};


function _readZipFile(entryPath, srcStream, response) {
  if (entryPath === undefined) {
    entryPath = '/';
  }

  if (entryPath.endswith('/')) {
    // TODO: show sub level of file list
    _getZipEntries(srcStream, function(entries) {
      if (entryPath === '/') {
        response.send(entries);
      } else {
        response.send(entries.filter(function(e) {
          return e.path.startswith(entryPath);
        }));
      }
    });
  } else {
    srcStream.pipe(unzip.Parse())
    .on('entry', function(entry) {
      if (entry.path === entryPath) {
        entry.pipe(response);
        return;
      }
      entry.autodrain();
    });
  }
}


// read
server.get('/hdfs', function(req, res, next) {
  var hdfsPath = req.params.path;
  var fileType = req.params.type;
  var entryPath = req.params.entry;

  hdfs.exists(req.params.path, function(fileExist) {
    if (!fileExist) {
      res.send(404, {err: 'file "' + hdfsPath + '" not found'});
      return next();
    }
    var remoteStream = hdfs.createReadStream(hdfsPath);
    if (fileType === 'zip') {
      _readZipFile(entryPath, remoteStream, res);
    } else {
      res.setHeader('content-disposition', 'attachment; filename="' + path.basename(req.params.path) +'"');
      remoteStream.pipe(res);
    }
    res.on('end', next);
  });
});


// write
server.post('/hdfs', function(req, res, next) {
  var now = moment();
  var dirPath = '/lybica/' + now.format('YYYY/MM/DD');
  var fileName = now.format('HHmmss') + '_' + uuid.v1().substr(0, 6);
  var remoteStream = hdfs.createWriteStream(dirPath + '/' + fileName);
  req.pipe(remoteStream);
  remoteStream.on('error', function(err) {
    res.send(400, {err: err});
    return next();
  });
  remoteStream.on('finish', function() {
    res.setHeader('hdfsurl', dirPath + '/' + fileName);
    res.send(200, {path: dirPath + '/' + fileName});
    return next();
  });
});


// delete
// TODO
server.del('/hdfs', function(req, res, next) {
  return next();
});

server.listen(config.PORT, function() {
  console.log('%s listening at %s', server.name, server.url);
});

