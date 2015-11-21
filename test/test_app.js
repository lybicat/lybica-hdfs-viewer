var restify = require('restify');
var expect = require('expect.js');

describe('/hdfs/(action)', function() {
  var client;

  before(function(done) {
    client = restify.createJsonClient({
      url: process.env.HDFS_VIEWER_URL
    });
    done();
  });

  afterEach(function(done) {
    done();
  });

  it('GET /hdfs/ return 404 when file not found', function(done) {
    client.get('/hdfs/?path=/unexist', function(err, req, res, obj) {
      expect(err).not.to.eql(null);
      expect(res.statusCode).to.eql(404);
      done();
    });
  });
});

