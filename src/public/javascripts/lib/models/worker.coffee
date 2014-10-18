firebase = require '../firebase_client.coffee'
config = require '../../../../config.coffee'
constants = require '../../../../constants.coffee'

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
      when "MAP_START" then start_map(message)
      when "JOB_DONE" then finish_job(message)



  listen: () ->
    message_ref = firebase.WORKER_MESSAGE_REF.child(@_id)
    message_ref.on 'child_added', (new_child) =>
      @process_message(new_child.val())
      message_ref.child(new_child.name()).remove()

  send_to: (client, message, callback) ->
    other = firebase.SERVER_MESSAGE_REF.child(client)
    other.push message, callback


  finish_job: () ->
    @status.state = 'IDLE'


  start_map: (map_start_message) ->
    job_id = map_start_message.job_id
    url = map_start_message.url
    @status.state = 'MAPPER'

    get_data(url)
    #TODO: retreive mapping code from firebase

    get_mapping_code(job_id)

    run_map_job()


  get_data: (url) ->

    @data = ''
    http.get { host: 'url' }, (res) ->

    res.on 'data', (chunk) ->
      @data += chunk.toString()

  get_mapping_code: (job_id) ->
    #TODO figure out where this is

    @map_code = null


  run_map_job: () ->
    context = {data: @data}
    @mappings = eval(context, @map_code)

  map_done: () ->
    #TODO figure out if this is correct
    firebase.git


  send_map_data: (reduce_node_list) ->

  start_reduce: (start_reduce_message) ->

  add_map_output: (map_output_message) ->

  start_reduce: () ->

  finish_reduce: () ->


module.exports = {Worker}