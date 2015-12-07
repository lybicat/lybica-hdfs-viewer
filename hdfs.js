/* jshint node: true */
'use strict';

var WebHDFS = require('webhdfs');
var config = require('./config');

module.exports = WebHDFS.createClient({
  user: config.HDFS_USER,
  host: config.HDFS_HOST,
  port: config.HDFS_PORT,
  path: config.HDFS_PATH
});
