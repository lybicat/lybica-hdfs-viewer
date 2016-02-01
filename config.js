/* jshint node: true */
'use strict';

exports.PORT = process.env.HDFS_VIEWER_PORT || 3001;
exports.HDFS_USER = process.env.HDFS_USER || process.env.USER;
exports.HDFS_PORT = process.env.HDFS_PORT || 50070;
exports.HDFS_HOST = process.env.HDFS_HOST || 'localhost';
exports.HDFS_PATH = process.env.HDFS_PATH || '/webhdfs/v1';
exports.HDFS_PREFIX = process.env.HDFS_PREFIX || '/lybica/';
exports.CACHE_TTL = process.env.CACHE_TTL || 86400;
if (process.env.IN_UNIT_TEST === 'y') {
  module.exports.LOG4JS_SETTINGS = {
    appenders: [
      {type: 'console'}
    ],
    replaceConsole: true
  }
} else {
  module.exports.LOG4JS_SETTINGS = {
    appenders: [
      {
        type: 'file',
        filename: __dirname + '/logs/access.log',
        maxLogSize: 50 * 1024 * 1024,
        backups: 4
      },
      {
        type: 'logLevelFilter',
        level: 'ERROR',
        appender: {
          type: 'file',
          filename: __dirname + '/logs/error.log',
          maxLogSize: 50 * 1024 * 1024,
          backups: 4
        }
      }
    ],
    replaceConsole: true
  }
}
