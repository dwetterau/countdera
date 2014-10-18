firebase = require '../firebase_client.coffee'
config = require '../../../../config.coffee'
constants = require '../../../../constants.coffee'
http = require 'http'
q = require 'q'

class Worker
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
    id_ref = firebase.WORKER_ID_REF.push "new_worker"
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
    firebase.WORKER_STATUS_REF.update update_object, callback

  process_message: (message) ->
    console.log "Got a message!", message
    message_type = message.name
    switch(message_type)
      when "MAP_START" then @start_map(message)
      when "JOB_DONE" then @finish_job(message)
      when "REDUCE_NODES" then @send_map_data(message)

  listen: () ->
    message_ref = firebase.WORKER_MESSAGE_REF.child(@_id)
    message_ref.on 'child_added', (new_child) =>
      @process_message(new_child.val())
      message_ref.child(new_child.name()).remove()

  send_to_server: (message, callback) ->
    other = firebase.SERVER_MESSAGE_REF
    other.push message, callback

  send_to_friend: (client, message, callback) ->
    other = firebase.WORKER_MESSAGE_REF.child(client)
    other.push message, callback

  finish_job: () ->
    @data = null
    @mappings = null
    @map_code  = null
    #todo invalidate everything
    @status.state = 'IDLE'

  start_map: (map_start_message) ->
    @status.state = 'MAPPER'
    @job_id = map_start_message.job_id

    get_data(map_start_message.url).then(() ->
      return get_mapping_code()
    ).then () ->
      run_map_job()

  get_data: (url) ->
    deferred = q.defer()
    @data = ''
    http.get { host: url }, (res) ->
      res.on 'data', (chunk) ->
        @data += chunk.toString()
      res.on 'end', () ->
        deferred.resolve()

    return deferred.promise

  get_mapping_code: () ->
    #TODO figure out where this is

    @map_code = null

  run_map_job: () ->
    @mappings = []

    emit = (key, object) =>
      @mappings.push [key, object]

    map = (lines) =>
      eval(@map_code)

    map(@data.split('\n'))

  map_done: () ->
    msg = {name: "MAPPER_DONE", id: @_id}
    send_to_server msg, () ->
      @status.state = 'MAPPER_DONE'

  send_map_data: (reduce_node_list) ->
    num_nodes = reduce_node_list.nodes.length
    for clients in reduce_node_list.nodes
      send_to_friend to_send_client {name : 'START_MAP_OUTPUT ', id: @_id} null

    for [key, object] in @mappings
      hash = hashval(key)
      to_send_client = reduce_node_list[hash % num_nodes]
      send_to_friend to_send_client {name : 'MAP_OUTPUT ', id: @_id, key : object} null

    for clients in reduce_node_list.nodes
      send_to_friend to_send_client {name : 'END_MAP_OUTPUT ', id: @_id} null


  start_reduce: (start_reduce_message) ->

  add_map_output: (map_output_message) ->

  finish_reduce: () ->

  hashval (s) =>
    hash = 0
    chr = null

    if s.length == 0
        return hash;
    for i in s.length
      chr   = this.charCodeAt(i)
      hash  = ((hash << 5) - hash) + chr;
      hash |= 0

    return hash;


module.exports = {Worker}