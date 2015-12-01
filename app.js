var restify = require('restify');
var path = require('path');
var config = require('./config');
var hdfs = require('./hdfs');
var moment = require('moment');
var uuid = require('uuid');
var fs = require('fs');
var yauzl = require('yauzl');
var jade = require('jade');
var cache = require('node-cache');
var md5 = require('./hash').md5;

var entryCache = new cache({stdTTL: config.CACHE_TTL});

var cachedDir = __dirname + '/cache'; // cached zip file

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


String.prototype.startswith = function(s) {
  return this.substr(0, s.length) === s;
};


String.prototype.endswith = function(s) {
  return this.substr(this.length - s.length, this.length) === s;
};


String.prototype.count = function(lit) {
  var m = this.toString().match(new RegExp(lit, 'g'));
  return (m !== null) ? m.length : 0;
};


String.prototype.strip = function(s) {
  if (!this.endswith(s)) {
    return this;
  }
  return this.substr(0, this.length - s.length);
};

function _openZip(filePath, callback) {
  yauzl.open(filePath, function(err, zipfile) {
    if (err) return callback(err);
    return callback(null, zipfile);
  });
}

function _readZip(hdfsPath, callback) {
  var filePath = cachedDir + '/' + md5(hdfsPath);
  fs.exists(filePath, function(fileExist) {
    if (!fileExist) {
      var remoteStream = hdfs.createReadStream(hdfsPath);
      var localStream = fs.createWriteStream(filePath);
      localStream
        .on('error', function(err) {
          return callback(err);
        })
        .on('finish', function() {
          console.log('write hdfs file %s to %s', hdfsPath, filePath);
          _openZip(filePath, callback);
        });
      remoteStream.pipe(localStream);
    }
    _openZip(filePath, callback);
  });
}


function _getZipEntries(hdfsPath, callback) {
  var cachedEntries = entryCache.get(hdfsPath);
  if (cachedEntries !== undefined) {
    return callback(null, cachedEntries);
  }

  var entries = [];
  _readZip(hdfsPath, function(err, zipfile) {
    if (err) return callback(err);
    zipfile
      .on('entry', function(entry) {
        entries.push({path: entry.fileName, size: entry.uncompressedSize, lastmod: entry.getLastModDate()});
      })
      .on('error', function(err) {
        callback(err);
      })
      .on('close', function() {
        callback(null, entries);
        entryCache.set(hdfsPath, entries);
      });
  });
}


function _getHtml(entries) {
  return jade.compileFile('./index.jade')({entries: entries});
}


function _renderDirectory(entryPath, hdfsPath, response) {
  _getZipEntries(hdfsPath, function(err, entries) {
    if (err) return response.send(400, err);

    var renderedEntries;
    if (entryPath === '/') {
      renderedEntries = entries.filter(function(e) {
        return e.path.strip('/').count('/') === 0;
      });
      renderedEntries.forEach(function(e) {
        e.url = e.path;
      });
    } else {
      renderedEntries = entries.filter(function(e) {
        return e.path !== entryPath &&
          e.path.startswith(entryPath) &&
          e.path.strip('/').count('/') === entryPath.count('/');
      });
      renderedEntries.forEach(function(e) {
        e.url = e.path.substr(entryPath.length, e.path.length);
      });
    }
    if (process.env.IN_UNIT_TEST === 'y') {
      response.send(renderedEntries);
    } else {
      response.writeHead(200, {'Content-Type': 'text/html'});
      response.write(_getHtml(renderedEntries));
      response.end();
    }
  });
}


function _renderFile(entryPath, hdfsPath, res) {
  _readZip(hdfsPath, function(err, zipfile) {
    if (err) return res.send(400, err);
    zipfile.on('entry', function(entry) {
      if (entry.fileName === entryPath) {
        zipfile.openReadStream(entry, function(err, readStream) {
          if (err) return res.send(400, err);
          readStream.pipe(res);
        });
      }
    });
  });
}


function _readZipFile(entryPath, hdfsPath, res) {
  if (entryPath.endswith('/')) {
    _renderDirectory(entryPath, hdfsPath, res);
  } else {
    _renderFile(entryPath, hdfsPath, res);
  }
}


// read
server.get(/hdfs\/(\S+)!\/(.*)/, function(req, res, next) {
  var hdfsPath = decodeURI(req.params[0]);
  if (!hdfsPath.startswith('/')) {
    hdfsPath = '/' + hdfsPath;
  }
  var entryPath = decodeURI(req.params[1]) || '/';

  hdfs.exists(hdfsPath, function(fileExist) {
    if (!fileExist) {
      res.send(404, {err: 'file "' + hdfsPath + '" not found'});
      return next();
    }
    if (hdfsPath.endswith('.zip')) {
      _readZipFile(entryPath, hdfsPath, res);
    } else {
      res.setHeader('content-disposition', 'attachment; filename="' + path.basename(hdfsPath) +'"');
      hdfs.createReadStream(hdfsPath).pipe(res);
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
server.del(/hdfs\/(\S+)/, function(req, res, next) {
  var hdfsPath = req.params[0];
  if (!hdfsPath.startswith('/')) {
    hdfsPath = '/' + hdfsPath;
  }
  hdfs.rmdir(hdfsPath, true, function(err) {
    if (err) {
      return next(err);
    } else {
      res.send(200);
      return next();
    }
  });
});

server.listen(config.PORT, function() {
  console.log('%s listening at %s', server.name, server.url);
});

