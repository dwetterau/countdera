firebase = require '../firebase_client.coffee'
constants = require '../../../../constants.coffee'

class Client
  constructor: (id) ->
    @_id = id
    @_clients = {}

  init: () ->
    @listen()

  listen: () ->
    inbound_message_ref = firebase.CLIENT_MESSAGE_REF.child(@_id)
    inbound_message_ref.on 'child_added', (new_child) =>
      @process_message(new_child.val())
      inbound_message_ref.child(new_child.name()).remove()

  process_message: (message) ->
    console.log @_id, "Client got a message?"
    console.log message

  send_to_server: (message, callback) ->
    firebase.SERVER_MESSAGE_REF.push message, callback

  save_attribute: (attribute, object, callback) ->
    firebase.JOB_STATUS_REF.child(@_id).child(attribute).set(object, callback)

  save_map_code: (map_code) ->
    @save_attribute('map_code', map_code)

  save_reduce_code: (reduce_code) ->
    @save_attribute('reduce_code', reduce_code)

  save_urls: (urls) ->
    @save_attribute 'urls', urls, () =>
      @start_job()

  start_job: () ->
    @send_to_server {name: "START_JOB", job_id: @_id}

  finish_job: () ->
    @send_to_server {name: "FINISH_JOB", job_id: @_id}

module.exports = {Client}