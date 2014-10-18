{Client} = require './lib/models/client.coffee'

module.exports =
  run: () ->
    room_id = get_room_id_from_url()
    client = new Client(room_id)
    client.init()

    setTimeout(add_handlers, 100)

add_handlers = (client) ->

get_room_id_from_url = () ->
  path = window.location.pathname
  return path.slice(1, path.lastIndexOf('/'))
