var restify = require('restify');
var expect = require('expect.js');
var fs = require('fs');
var request = require('request');

describe('/hdfs', function() {
  var client;

  before(function(done) {
    client = restify.createStringClient({
      url: process.env.HDFS_VIEWER_URL
    });
    var hdfs = require('../hdfs');
    hdfs.mkdir('/unittest', function() {
      var localStream = fs.createReadStream(__dirname + '/data/zipfile1.zip');
      var remoteStream = hdfs.createWriteStream('/unittest/zipfile1.zip');
      localStream.pipe(remoteStream);
      remoteStream.on('end', done);
    });
  });

  after(function(done) {
    //var hdfs = require('../hdfs');
    //hdfs.rmdir('/unittest', true, done);
    done();
  });

  it('GET /hdfs/ return 404 when file not found', function(done) {
    client.get('/hdfs?path=/unexist', function(err, req, res, obj) {
      expect(err).not.to.eql(null);
      expect(res.statusCode).to.eql(404);
      done();
    });
  });

  it('GET /hdfs/ return 200 when file exist', function(done) {
    client.get('/hdfs?path=/unittest/zipfile1.zip', function(err, req, res, obj) {
      expect(err).to.eql(null);
      expect(res.statusCode).to.eql(200);
      done();
    });
  });

  it('GET /hdfs/ return file content in zip', function(done) {
    client.get('/hdfs?path=/unittest/zipfile1.zip&type=zip&entry=file1.txt', function(err, req, res, obj) {
      expect(err).to.eql(null);
      expect(res.statusCode).to.eql(200);
      expect(obj).to.eql('["file1", "file2"]');
      done();
    });
  });

  it('GET /hdfs/ return file list in directory of zip', function(done) {
    client.get('/hdfs?path=/unittest/zipfile1.zip&type=zip', function(err, req, res, obj) {
      expect(err).to.eql(null);
      expect(res.statusCode).to.eql(200);
      expect(obj).to.eql('file1\n');
      done();
    });
  });

  it('POST /hdfs/ return 200', function(done) {
    var req = request.post(process.env.HDFS_VIEWER_URL + '/hdfs');
    fs.createReadStream(__dirname + '/data/zipfile1.zip').pipe(req);
    req.on('response', function(res) {
      expect(res.statusCode).to.eql(200);
      var hdfs = require('../hdfs');
      hdfs.exists(res.headers.hdfsurl, function(fileExist) {
        expect(fileExist).to.eql(true);
        done();
      });
    });
  });
});

