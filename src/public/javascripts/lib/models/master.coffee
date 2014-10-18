firebase = require '../firebase_client.coffee'
constants = require '../../../../constants.coffee'

class Master
  constructor: (id) ->
    @_id = id
    @_clients = {}

  init: () ->
    @listen()

  listen: () ->
    inbound_message_ref = firebase.MASTER_MESSAGE_REF.child(@_id)
    inbound_message_ref.on 'child_added', (new_child) =>
      @process_message(new_child.val())
      inbound_message_ref.child(new_child.name()).remove()

  process_message: (message) ->
    # TODO
    console.log @_id, "got a message!"
    console.log message

  send_to: (client_id, message) ->
    other = firebase.CLIENT_MESSAGE_REF.child(client)
    other.push message, callback

module.exports = {Master}