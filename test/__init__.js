before(function(done) {
    process.env.HDFS_VIEWER_PORT = Math.round(Math.random() * 10000) + 10000;
    delete process.env.http_proxy;
    delete process.env.https_proxy;
    process.env.HDFS_VIEWER_URL = 'http://127.0.0.1:' + process.env.HDFS_VIEWER_PORT;
    process.env.HDFS_USER = process.env.USER;
    var app = require('../app');
    done();
});


after(function(done) {
  done();
});
