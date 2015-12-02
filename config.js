exports.PORT = process.env.HDFS_VIEWER_PORT || 3001;
exports.HDFS_USER = process.env.HDFS_USER || process.env.USER;
exports.HDFS_PORT = process.env.HDFS_PORT || 50070;
exports.HDFS_HOST = process.env.HDFS_HOST || 'localhost';
exports.HDFS_PATH = process.env.HDFS_PATH || '/webhdfs/v1';
exports.HDFS_PREFIX = process.env.HDFS_PREFIX || '/lybica/';
exports.CACHE_TTL = process.env.CACHE_TTL || 86400;
