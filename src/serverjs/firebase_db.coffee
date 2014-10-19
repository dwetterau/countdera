Firebase = require 'firebase'
config = require '../config'
constants = require '../constants'

ROOT_REF = new Firebase('https://' + config.FIREBASE_NAME + '.firebaseIO.com')

WORKER_STATUS_REF = ROOT_REF.child(constants.FIREBASE_WORKER_STATUS)

WORKER_MESSAGE_REF = ROOT_REF.child(constants.FIREBASE_WORKER_MESSAGES)

WORKER_ID_REF = ROOT_REF.child(constants.FIREBASE_WORKER_IDS)

CLIENT_MESSAGE_REF = ROOT_REF.child(constants.FIREBASE_CLIENT_MESSAGES)

IO_SERVER_MESSAGE_REF = ROOT_REF.child(constants.FIREBASE_IO_SERVER_MESSAGES)

JOB_STATUS_REF = ROOT_REF.child(constants.FIREBASE_JOB_STATUS)

module.exports = {
  ROOT_REF,
  WORKER_STATUS_REF,
  WORKER_MESSAGE_REF,
  WORKER_ID_REF,
  CLIENT_MESSAGE_REF,
  IO_SERVER_MESSAGE_REF,
  JOB_STATUS_REF
}
