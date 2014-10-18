{Master} = require './lib/models/master.coffee'

module.exports =
  run: () ->
    room_id = get_room_id_from_url()
    master = new Master(room_id)
    master.init()

    setTimeout(add_handlers, 100)

add_handlers = (master) ->

get_room_id_from_url = () ->
  path = window.location.pathname
  return path.slice(1, path.lastIndexOf('/'))
