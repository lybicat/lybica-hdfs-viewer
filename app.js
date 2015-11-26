var restify = require('restify');
var path = require('path');
var config = require('./config');
var hdfs = require('./hdfs');
var moment = require('moment');
var uuid = require('uuid');
var unzip = require('unzip');
var jade = require('jade');

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


function _getZipEntries(zipStream, callback) {
  var entries = [];
  zipStream.pipe(unzip.Parse())
  .on('entry', function(entry) {
    entries.push({path: entry.path, type: entry.type, size: entry.size});
    entry.autodrain();
  })
  .on('close', function() {
    callback(entries);
  });
}


function _getHtml(entries) {
  return jade.compileFile('./index.jade')({entries: entries});
}


function _renderDirectory(entryPath, zipStream, response) {
  _getZipEntries(zipStream, function(entries) {
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


function _renderFile(entryPath, zipStream, response) {
  zipStream.pipe(unzip.Parse())
  .on('entry', function(entry) {
    if (entry.path === entryPath) {
      entry.pipe(response);
    } else {
      entry.autodrain();
    }
  });
}


function _readZipFile(entryPath, zipStream, response) {
  if (entryPath.endswith('/')) {
    _renderDirectory(entryPath, zipStream, response);
  } else {
    _renderFile(entryPath, zipStream, response);
  }
}


// read
server.get(/hdfs\/(\S+)!\/(.*)/, function(req, res, next) {
  var hdfsPath = decodeURI(req.params[0]);
  if (!hdfsPath.startswith('/')) {
    hdfsPath = '/' + hdfsPath;
  }
  var entryPath = req.params[1] || '/';
  entryPath = decodeURI(entryPath);
  var fileType = req.params.type || 'zip';

  hdfs.exists(hdfsPath, function(fileExist) {
    if (!fileExist) {
      res.send(404, {err: 'file "' + hdfsPath + '" not found'});
      return next();
    }
    var remoteStream = hdfs.createReadStream(hdfsPath);
    if (fileType === 'zip') {
      _readZipFile(entryPath, remoteStream, res);
    } else {
      res.setHeader('content-disposition', 'attachment; filename="' + path.basename(hdfsPath) +'"');
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

