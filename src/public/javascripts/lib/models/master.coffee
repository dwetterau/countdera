firebase = require '../firebase_client.coffee'
constants = require '../../../../constants.coffee'

class Master
  constructor: (id) ->
    @_id = id
    @_clients = {}

  init: () ->
    @listen()

    # TODO: Remove this
    setInterval () =>
      clients = @get_active_clients()
      to_display = '<ul>'
      for client_id, client of clients
        to_display += '<li>'
        to_display += client_id
        to_display += ' - last heartbeat: '
        to_display += client.last_update
        to_display += '</li>'
      to_display += '</ul>'
      $('#connected_clients').html to_display

  listen: () ->
    inbound_message_ref = firebase.MASTER_MESSAGE_REF.child(@_id)
    inbound_message_ref.on 'child_added', (new_child) =>
      @process_message(new_child.val())
      inbound_message_ref.child(new_child.name()).remove()

    client_status_ref = firebase.CLIENT_STATUS_REF
    client_status_ref.on 'child_changed', (snapshot) =>
      @_clients[snapshot.name()] = snapshot.val()
    client_status_ref.on 'child_removed', (snapshot) =>
      delete @_clients[snapshot.name()]

  get_active_clients: () ->
    now = new Date().getTime()
    active_clients = {}
    for child_id, child of @_clients
      # Allow some time for RTT + delay
      if now - child.last_update < 4 * constants.HEARTBEAT_INTERVAL
        active_clients[child_id] = child
    return active_clients


  process_message: (message) ->
    # TODO
    console.log @_id, "got a message!"
    console.log message

  send_to: (client_id, message) ->
    other = firebase.CLIENT_MESSAGE_REF.child(client)
    other.push message, callback

module.exports = {Master}