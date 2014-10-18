express = require 'express'
router = express.Router()
room_manager = require '../serverjs/room_manager'

# GET home page
router.get '/', (req, res) ->
  # This should generate a room_id that other users will use to connect.
  room_id = room_manager.get_room_id()
  res.redirect room_id + '/master'

router.get '/:room_id/master', (req, res) ->
  # If the master is the first to connect, no other connections should be
  # allowed (there should only be one master).
  room_id = req.params.room_id
  res.render 'master',
    {room_id}

router.get '/client', (req, res) ->
  res.render 'client'

module.exports = router