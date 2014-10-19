firebase = require '../firebase_client.coffee'
config = require '../../../../config.coffee'
constants = require '../../../../constants.coffee'
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
    message_type = message.name
    #todo we know the states so check if receiving a message is sane
    switch message_type
      when "MAP_START" then @start_map(message)
      when "REDUCE_NODES" then @send_map_data(message)
      when "JOB_DONE" then @finish_job(message)
      when "START_REDUCE" then @start_reduce(message)
      when "START_MAP_OUTPUT" then @add_data_src(message) #todo
      when "MAP_OUTPUT" then @add_map_output(message)
      when "END_MAP_OUTPUT" then @close_data_src(message) #todo
      when "REDUCE_DONE" then @finish_reduce(message)
      else
        throw new Error("Unknown message!");


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
    @_status.state = 'IDLE'

  start_map: (map_start_message) ->
    @_status.state = 'MAPPER'
    @job_id = map_start_message.job_id
    @index = map_start_message.index

    @get_data(map_start_message.url).then(() =>
      return @get_mapping_code()
    ).then () =>
      @run_map_job()
      @map_done()

  get_data: (url) ->
    deferred = q.defer()
    @data = ''
    $.get url, (data) =>
      @data = data
      deferred.resolve()

    return deferred.promise

  get_mapping_code: () ->
    deferred = q.defer()
    firebase.JOB_STATUS_REF.child(@job_id).child('map_code').once 'value', (snapshot) =>
      @map_code = snapshot.val()
      deferred.resolve()
    return deferred.promise

  run_map_job: () ->
    @mappings = []

    emit = (key, object) =>
      @mappings.push [key, object]

    lines = @data.split('\n')
    lines = (line for line in lines when line.length > 0)

    map = (lines) =>
      eval(@map_code)

    map lines

  map_done: () ->
    msg = {name: "MAPPER_DONE", id: @_id}
    send_to_server msg, () =>
      @_status.state = 'MAPPER_DONE'

  send_map_data: (reduce_node_list) ->
    num_nodes = reduce_node_list.nodes.length
    for client in reduce_node_list.nodes
      @send_to_friend client,
        name: 'START_MAP_OUTPUT'
        index: @index

    for tuple in @mappings
      hash = @hashval(tuple[0])
      to_send_client = reduce_node_list[hash % num_nodes]
      @send_to_friend to_send_client,
        name: 'MAP_OUTPUT'
        index: @index
        key: tuple

    for client in reduce_node_list.nodes
      @send_to_friend client,
        name: 'END_MAP_OUTPUT'
        index: @index


  start_reduce: (start_reduce_message) ->
    @_status.state = 'REDUCER'
    @job_id = start_reduce_message.job_id
    @number_of_mappers = start_reduce_message.number_of_mappers
    @reduce_data = {}
    @num_done = 0
    @mapper_done = (false for _ in @number_of_mappers)

    @get_reduce_code()

  get_reduce_code: () ->
    deferred = q.defer()
    firebase.JOB_STATUS_REF.child(@job_id).child('reduce_code').once 'value', (snapshot) =>
      @reduce_code = snapshot.val()
      deferred.resolve()
    return deferred.promise

  add_data_src: (map_data_msg) ->
    if @mapper_done[map_data_msg.index]
      return
    @reduce_data[map_data_msg.index] = []

  close_data_src: (map_data_msg) ->
    if not @mapper_done[map_data_msg.index]
      @num_done++
    @mapper_done[map_data_msg.index] = true

    if @num_done == @number_of_mappers
      @do_reduce()

  add_map_output: (map_data_msg) ->
    if @mapper_done[map_data_msg.index]
      return
    @reduce_data[map_data_msg.index].push(map_data_msg.key)

  do_reduce: () ->
    collected_data = {}
    for index, list of @reduce_data
      for item in list
        if item[0] not of collected_data
          collected_data[item[0]] = []
        collected_data[item[0]].push(item[1])

    data_for_jimmy = {}
    reduce = (key, map_output_list) =>
      emit = (line) ->
        if key not of data_for_jimmy
          data_for_jimmy[key] = []
        data_for_jimmy[key].push(line)
      eval(@reduce_code)

    console.log "running reduce code:"
    console.log @reduce_code
    for key, list of collected_data
      reduce(key, list)

    @finish_reduce(data_for_jimmy)


  finish_reduce: (data_for_jimmy) ->
    #todo hi-5 the backend with some reduce judo
    console.log "Reduce finished!"
    console.log data_for_jimmy

  hashval: (s) ->
    hash = 0
    chr = null

    if s.length == 0
      return hash;
    for i in s.length
      chr   = this.charCodeAt(i)
      hash  = ((hash << 5) - hash) + chr
      hash |= 0

    return hash;

module.exports = {Worker}