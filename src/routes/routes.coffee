express = require 'express'
router = express.Router()
room_manager = require '../serverjs/room_manager'

# GET home page
router.get '/', (req, res) ->
  # This should generate a room_id that other users will use to connect.
  room_id = room_manager.get_room_id()
  res.redirect room_id + '/client'

router.get '/:room_id/client', (req, res) ->
  # If the master is the first to connect, no other connections should be
  # allowed (there should only be one master).
  room_id = req.params.room_id
  res.render 'client',
    {room_id}

router.get '/worker', (req, res) ->
  res.render 'worker'

module.exports = router