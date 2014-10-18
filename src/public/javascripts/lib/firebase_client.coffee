config = require '../../../config.coffee'
constants = require '../../../constants.coffee'

ROOT_REF = new Firebase('https://' + config.FIREBASE_NAME + '.firebaseIO.com')

CLIENT_STATUS_REF = ROOT_REF.child(constants.FIREBASE_CLIENT_STATUS)

CLIENT_MESSAGE_REF = ROOT_REF.child(constants.FIREBASE_CLIENT_MESSAGES)

CLIENT_ID_REF = ROOT_REF.child(constants.FIREBASE_CLIENT_IDS)

MASTER_MESSAGE_REF = ROOT_REF.child(constants.FIREBASE_MASTER_MESSAGES)

module.exports = {
  ROOT_REF,
  CLIENT_STATUS_REF,
  CLIENT_MESSAGE_REF,
  CLIENT_ID_REF,
  MASTER_MESSAGE_REF
}
