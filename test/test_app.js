/* jshint node: true */
'use strict';

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
    var hdfs = require('../hdfs');
    hdfs.rmdir('/unittest', true, done);
  });

  it('GET /hdfs/ return 404 when file not found', function(done) {
    client.get('/hdfs/unexist!/', function(err, req, res, obj) {
      expect(err).not.to.eql(null);
      expect(res.statusCode).to.eql(404);
      done();
    });
  });

  it('GET /hdfs/ return file content in zip', function(done) {
    client.get('/hdfs/unittest/zipfile1.zip!/file1.txt', function(err, req, res, obj) {
      expect(err).to.eql(null);
      expect(res.statusCode).to.eql(200);
      expect(obj).to.eql('file1\n');
      done();
    });
  });

  it('GET /hdfs/ return file list in top directory of zip', function(done) {
    client.get('/hdfs/unittest/zipfile1.zip!/', function(err, req, res, obj) {
      expect(err).to.eql(null);
      expect(res.statusCode).to.eql(200);
      expect(obj).to.eql('[{"path":"sub/","size":0,"lastmod":"2015-11-26T01:14:26.000Z","url":"sub/"},{"path":"file1.txt","size":6,"lastmod":"2015-11-24T02:16:34.000Z","url":"file1.txt"},{"path":"file2.txt","size":6,"lastmod":"2015-11-24T02:16:46.000Z","url":"file2.txt"}]');
      done();
    });
  });

  it('GET /hdfs/ return file list in sub directory of zip', function(done) {
    client.get('/hdfs/unittest/zipfile1.zip!/sub/', function(err, req, res, obj) {
      expect(err).to.eql(null);
      expect(res.statusCode).to.eql(200);
      expect(obj).to.eql('[{"path":"sub/empty","size":0,"lastmod":"2015-11-26T01:14:24.000Z","url":"empty"},{"path":"sub/subsub/","size":0,"lastmod":"2015-11-26T01:14:44.000Z","url":"subsub/"}]');
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

  it('POST /hdfs/just/a/test.zip return 200', function(done) {
    var req = request.post(process.env.HDFS_VIEWER_URL + '/hdfs/just/a/test.zip');
    fs.createReadStream(__dirname + '/data/zipfile1.zip').pipe(req);
    req.on('response', function(res) {
      expect(res.statusCode).to.eql(200);
      expect(res.headers.hdfsurl).to.eql('/just/a/test.zip');
      var hdfs = require('../hdfs');
      hdfs.exists(res.headers.hdfsurl, function(fileExist) {
        expect(fileExist).to.eql(true);
        done();
      });
    });
  });
});

