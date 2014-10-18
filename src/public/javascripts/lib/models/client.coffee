firebase = require '../firebase_client.coffee'
config = require '../../../../config.coffee'
constants = require '../../../../constants.coffee'

class Client
  constructor: () ->
    @_last_update = new Date().getTime()
    @_connections = {}
    @_status = {
      state: 'IDLE'
    }

  init: () ->
    @get_id()
    @listen()
    setInterval () =>
      @heartbeat()
    , constants.HEARTBEAT_INTERVAL

  heartbeat: () ->
    @_update_time()
    @save_to_firebase()

  get_id: () ->
    id_ref = firebase.CLIENT_ID_REF.push "new_client"
    @_id = id_ref.name()

  _update_time: () ->
    @_last_update = new Date().getTime()

  to_json: () ->
    object =
      id: @_id
      last_update: @_last_update
      status: @_status
    return object

  id: () ->
    return @_id

  save_to_firebase: (callback) ->
    update_object = {}
    update_object[@_id] = @to_json()
    firebase.CLIENT_STATUS_REF.update update_object, callback

  process_message: (message) ->
    console.log "Got a message!", message

  listen: () ->
    message_ref = firebase.CLIENT_MESSAGE_REF.child(@_id)
    message_ref.on 'child_added', (new_child) =>
      @process_message(new_child.val())
      message_ref.child(new_child.name()).remove()

  send_to: (client, message, callback) ->
    other = firebase.CLIENT_MESSAGE_REF.child(client)
    other.push message, callback

module.exports = {Client}