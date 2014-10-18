{firebase} = require './firebase_db'
constants = require '../constants'
Q = require 'q'

module.exports =
  get_room_id: () ->
    room_name = firebase.child(constants.FIREBASE_ROOM_IDS).push {'in_use': false}
    console.log room_name.name()
    return room_name.name()
