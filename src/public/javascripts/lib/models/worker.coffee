firebase = require '../firebase_client.coffee'
config = require '../../../../config.coffee'
constants = require '../../../../constants.coffee'
q = require 'q'

class Worker
  constructor: (statecallback) ->
    @_last_update = new Date().getTime()
    @_connections = {}
    @_status = {
      state: 'IDLE'
    }
    @statecallback = statecallback

  init: () ->
    @get_id()
    @listen()
    setInterval () =>
      before = @_last_update
      @heartbeat () ->
        #console.log "Heartbeated... diff=", (new Date().getTime() - before)
    , constants.HEARTBEAT_INTERVAL
    @statecallback @_status.state

  heartbeat: (callback) ->
    @_update_time()
    @save_to_firebase(callback)

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
    if message_type == "MAP_START"
      @start_map(message)
    else if message_type == "REDUCE_NODES"
      @send_map_data(message)
    else if message_type == "JOB_DONE"
      @finish_job(message)
    else if message_type == "START_REDUCE"
      @start_reduce(message)
    else if message_type == "START_MAP_OUTPUT"
      @add_data_src(message)
    else if message_type == "MAP_OUTPUT"
      @add_map_output(message)
    else if message_type == "END_MAP_OUTPUT"
      @close_data_src(message)
    else if message_type == "REDUCE_DONE"
      @finish_reduce(message)
    else
      throw new Error("Unknown message!")


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
    @clean_reduce()

  start_map: (map_start_message) ->
    @_status.state = 'MAPPER'
    @statecallback(@_status.state)
    @job_id = map_start_message.job_id
    @index = map_start_message.index

    @get_data(map_start_message.url).then(() =>
      return @get_mapping_code()
    ).then () =>
      @run_map_job()
      @map_done()

  get_data: (url) ->
    @_status.state = "DOWNLOADING_MAPPING_DATA"
    @statecallback(@_status.state)
    deferred = q.defer()
    @data = ''
    $.get url, (data) =>
      @data = data
      deferred.resolve()

    return deferred.promise

  get_mapping_code: () ->
    @_status.state = "DOWNLOADING_MAPPING_CODE"
    @statecallback(@_status.state)
    deferred = q.defer()
    firebase.JOB_STATUS_REF.child(@job_id).child('map_code').once 'value', (snapshot) =>
      @map_code = snapshot.val()
      deferred.resolve()
    return deferred.promise

  run_map_job: () ->
    @_status.state = "MAPPING"
    @statecallback(@_status.state)
    @mappings = []
    emit = (key, object) =>
      @mappings.push [key, object]

    lines = @data.split('\n')
    lines = (line for line in lines when line.length > 0)

    map = (lines) =>
      eval(@map_code)

    map lines

  map_done: () ->
    msg = {name: "MAPPER_DONE", job_id: @job_id, id: @_id}
    @send_to_server msg, () =>
      @_status.state = 'MAPPER_DONE'
      @statecallback(@_status.state)

  send_map_data: (reduce_node_list) ->
    @_status.state = "SENDING_MAPPED_RESULTS"
    @statecallback(@_status.state)

    num_nodes = reduce_node_list.nodes.length
    # Group all of the messages together
    reducer_queues = []
    for client in reduce_node_list.nodes
      reducer_queues.push []
    for tuple in @mappings
      hash = @hashval(tuple[0])
      reducer_index = hash % num_nodes
      reducer_queues[reducer_index].push tuple

    for queue, queue_index in reducer_queues
      batched_queue = []
      # Batch the individual tuples
      current_batch = []
      for tuple, index in queue
        if index > 0 and index % constants.BATCH_SIZE == 0
          batched_queue.push {tuples: current_batch}
          current_batch = []
        current_batch.push tuple
      if current_batch.length
        batched_queue.push {tuples: current_batch}
      reducer_queues[queue_index] = batched_queue

    for client in reduce_node_list.nodes
      @send_to_friend client,
        name: 'START_MAP_OUTPUT'
        index: @index

    total_sent = 0
    total_to_send = 0
    for queue in reducer_queues
      for batched_tuples in queue
        total_to_send += batched_tuples.tuples.length

    for queue, queue_index in reducer_queues
      for batched_tuples, index in queue
        to_send_client = reduce_node_list.nodes[queue_index]
        @send_to_friend to_send_client,
          name: 'MAP_OUTPUT'
          index: @index
          key: batched_tuples
        total_sent += batched_tuples.tuples.length
        @statecallback(@_status.state, "Sent " + total_sent + " / " + total_to_send)

    for client in reduce_node_list.nodes
      @send_to_friend client,
        name: 'END_MAP_OUTPUT'
        index: @index

    @_status.state = 'MAPPER_DONE'
    @statecallback(@_status.state)


  start_reduce: (start_reduce_message) ->
    @_status.state = 'REDUCER'
    @statecallback(@_status.state)
    @job_id = start_reduce_message.job_id
    @index = start_reduce_message.index
    @number_of_mappers = start_reduce_message.number_of_mappers
    @reduce_data = {}
    @num_done = 0
    @mapper_done = (false for _ in @number_of_mappers)

  get_reduce_code: () ->
    @_status.state = "DOWNLOADING_REDUCE_CODE"
    @statecallback(@_status.state)
    deferred = q.defer()
    firebase.JOB_STATUS_REF.child(@job_id).child('reduce_code').once 'value', (snapshot) =>
      @reduce_code = snapshot.val()
      deferred.resolve()
    return deferred.promise

  add_data_src: (map_data_msg) ->
    @_status.state = "RECEIVING_REDUCE_DATA"
    @statecallback(@_status.state)
    if @mapper_done[map_data_msg.index]
      return
    @reduce_data[map_data_msg.index] = []

  close_data_src: (map_data_msg) ->
    if not @mapper_done[map_data_msg.index]
      @num_done++
    @mapper_done[map_data_msg.index] = true

    if @num_done == @number_of_mappers
      @get_reduce_code().then () =>
        @do_reduce()

  add_map_output: (map_data_msg) ->
    if @mapper_done[map_data_msg.index]
      return

    # Unpack the batched message
    for tuple in map_data_msg.key.tuples
      @reduce_data[map_data_msg.index].push(tuple)

  do_reduce: () ->
    @_status.state = "REDUCING"
    @statecallback(@_status.state)
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

    num_reduced = 0
    for key, list of collected_data
      reduce(key, list)
      num_reduced++

    @finish_reduce(data_for_jimmy)


  finish_reduce: (data_for_jimmy) ->
    @_status.state = "SENDING_TO_IO_SERVER"
    @statecallback(@_status.state)
    # We want to collect the key -> [output_lines] and send a certain number of lines at a time
    messages = []
    current_batch = []
    current_batch_length = 0
    for key, list of data_for_jimmy
      if list.length + current_batch_length > constants.BATCH_SIZE
        messages.push current_batch
        current_batch = []
        current_batch_length = 0
      current_batch.push [key, if list.length == 1 then list[0] else list]
      current_batch_length += list.length
    if current_batch.length
      messages.push current_batch

    # Start message to Jimmy
    firebase.IO_SERVER_MESSAGE_REF.push
      name: "START_REDUCER_OUTPUT",
      reducer: @index,
      job: @job_id

    for message in messages
      firebase.IO_SERVER_MESSAGE_REF.push {
        name: "REDUCER_OUTPUT"
        reducer: @index
        message
        job: @job_id
      }

    firebase.IO_SERVER_MESSAGE_REF.push
      name: "STOP_REDUCER_OUTPUT",
      reducer: @index,
      job: @job_id

    @send_to_server
      name: "REDUCE_DONE"
      index: @index
      job_id: @job_id

    @clean_reduce()

  clean_reduce: () ->
    @reduce_code = null
    @reduce_data = null
    @job_id = null
    @number_of_mappers = null
    @reduce_data = {}
    @num_done = 0
    @mapper_done = null
    @_status.state = 'IDLE'
    @statecallback(@_status.state)

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

  getState: () ->
    return @_status.state

module.exports = {Worker}