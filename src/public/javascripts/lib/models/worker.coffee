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
    #todo we know the states so check if receiving a message is sane
    switch(message_type)
      when "MAP_START" then @start_map(message)
      when "REDUCE_NODES" then @send_map_data(message)
      when "JOB_DONE" then @finish_job(message)
      when "START_REDUCE" then @start_reduce(message)
      when "START_MAP_OUTPUT" then @add_data_src(message) #todo
      when "MAP_OUTPUT" then @add_map_output(message)
      when "END_MAP_OUTPUT" then @close_data_src(message) #todo
      when "REDUCE_DONE" then @finish_reduce(message)


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
    @index = map_start_message.index

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

    @map_code = "pass"

  run_map_job: () ->
    @mappings = []

    emit = (key, object) =>
      @mappings.push [key, object]

    map = (lines) =>
      eval(@map_code)

    map(@data.split('\n'))
    #todo will new lines work? jagnew says hrm

  map_done: () ->
    msg = {name: "MAPPER_DONE", id: @_id}
    send_to_server msg, () ->
      @status.state = 'MAPPER_DONE'

  send_map_data: (reduce_node_list) ->
    num_nodes = reduce_node_list.nodes.length
    for client in reduce_node_list.nodes
      @send_to_friend client,
        name: 'START_MAP_OUTPUT'
        index: @index

    for tuple in @mappings
      hash = hashval(tuple[0])
      to_send_client = reduce_node_list[hash % num_nodes]
      @send_to_friend to_send_client,
        name: 'MAP_OUTPUT'
        index: @index
        key: object

    for client in reduce_node_list.nodes
      @send_to_friend client,
        name: 'END_MAP_OUTPUT'
        index: @index


  start_reduce: (start_reduce_message) ->
    @status.state = 'REDUCER'
    @job_id = start_reduce_message.job_id
    @number_of_mappers = start_reduce_message.number_of_mappers
    @reduce_data = {}
    @mapper_done = (false for i in @number_of_mappers)

    @get_reduce_code()

  get_reduce_code: () ->
    #todo something with job ids?

    @reduce_code = "pass"


  add_data_src: (map_data_msg) ->
    if(@mapper_done[map_data_msg.index])
      return
    else
      @reduce_data[map_data_msg.index] = []

  close_data_src: (map_data_msg) ->
    @mapper_done[map_data_msg.index] = true

  add_map_output: (map_data_msg) ->
    @reduce_data[map_data_msg.index].append(map_data_msg.key)

  do_reduce: (themsg_by_grandmaster_flash_and_the_furious_five) ->
    collected_data = {}
    for list in reduce_data
      for item in list
        if item[0] not in collected_data_data.keys
          collected_data[item[0]] = []
        for value in item[1]
          collected_data[item[0]].append(value)

    reduce = (key, list_of_objects) =>
      eval(@reduce_code)

    data_for_jimmy = {}
    for key in collected_data.keyset
      data_for_jimmy[key] = []
      emit = (line) =>
        data_for_jimmy[key].push(line)
      reduce(key, collected_data[key])

    finish_reduce


  finish_reduce: () ->
    #todo hi-5 the backend with some reduce judo



  hashval (s) =>
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