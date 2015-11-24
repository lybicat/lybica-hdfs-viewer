var restify = require('restify');
var expect = require('expect.js');
var fs = require('fs');

describe('/hdfs', function() {
  var client;

  before(function(done) {
    client = restify.createJsonClient({
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
    var hdfs = require('../hdfs');
    hdfs.rmdir('/unittest', true, done);
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
});

