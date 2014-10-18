Firebase = require 'firebase'
config = require '../config'

module.exports =
  firebase: new Firebase('https://' + config.FIREBASE_NAME + '.firebaseIO.com')
