nconf = require('nconf')

readConfig = () ->
  nconf.argv()
    .env()
    .file { file: __dirname + '/../../../configs/config.' + process.env.NODE_ENV + '.json' }
  return nconf

module.exports = readConfig()
