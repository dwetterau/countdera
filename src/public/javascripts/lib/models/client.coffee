firebase = require '../firebase_client.coffee'
constants = require '../../../../constants.coffee'

class Client
  constructor: (id) ->
    @_id = id
    @_clients = {}

  init: () ->
    @listen()

  listen: () ->
    inbound_message_ref = firebase.WORKER_MESSAGE_REF.child(@_id)
    inbound_message_ref.on 'child_added', (new_child) =>
      @process_message(new_child.val())
      inbound_message_ref.child(new_child.name()).remove()

  process_message: (message) ->
    # TODO
    console.log @_id, "got a message!"
    console.log message

  send_to: (worker_id, message) ->
    other = firebase.WORKER_MESSAGE_REF.child(worker)
    other.push message, callback

  save_attribute: (attribute, object) ->

  save_map_code: (map_code) ->

  save_reduce_code: (reduce_code) ->

  save_urls: (urls) ->

  start_job: () ->

  finish_job: () ->


module.exports = {Client}