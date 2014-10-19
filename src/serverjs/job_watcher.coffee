firebase = require './firebase_db'
constants = require '../constants'
Q = require 'q'

class JobWatcher
  constructor: (jobid) ->
    @jobid = jobid
    @urls = []
    @status = "STARTING"
    @queue = {}
    @mapStatus = {}
    @reduceStatus = {}
    @initFromFirebase()

    @doneMappers = []


  initFromFirebase: () ->
    # keep it ALL in memory because reasons
    @clientMap = {}
    firebase.WORKER_STATUS_REF.once 'value', (snapshot) ->
      snapshot.forEach (child) ->
        @clientMap[child.name()] = child.val()
    firebase.WORKER_STATUS_REF.on 'child_changed', (snapshot) =>
      @clientMap[snapshot.name()] = snapshot.val()
    firebase.WORKER_STATUS_REF.on 'child_removed', (snapshot) =>
      delete @clientMap[snapshot.name()]

    # get urls from firebase
    firebase.JOB_STATUS_REF.child(@jobid).child('urls').once 'value', (snapshot) ->
      snapshot.forEach (child) ->
        @urls.push child.name()

  get_active_clients: (num) ->
    now = new Date().getTime()
    active_clients = {}
    for child_id, child of @clientMap
      # Allow some time for RTT + delay
      if now - child.last_update < 4 * constants.HEARTBEAT_INTERVAL
        active_clients[child_id] = child
      if active_clients.size == num
        break
    return active_clients

  addMessage: (messageid, message) ->
    @queue[messageid] = message

  run: () ->
    @loop()

  loop: () ->

    switch @status
      when "STARTING"
        @allocateMappers()
        @startMap()
      when "MAPPING_STARTED"
        @checkMappers()
      when "MAPPING_ENDING_FIRST"
        @allocateReducers()
        @finishMappers()
        @status = "MAPPING_ENDING"
      when "MAPPING_ENDING"
        @checkMappers()
        @finishMappers()
      when "REDUCE_START"
        @startReduce()
        @status = "REDUCE_STARTED"
      when "REDUCE_STARTED"
        @checkMappers()  # so that if a reducer fails we have the data to send it?
        @checkReducers()
      else
        # done

    if @status is not "DONE"
      setTimeout () =>
        @loop()
      , 1
    else
      @finish()

  allocateMappers: () ->
    numMappers = @urls.length
    @mappers = @get_active_clients numMappers
    index = 0
    for child_id, child in @mappers
      @mapStatus[child_id] = { done: false, index: index }
      index++
    @numMappersLeft = numMappers

  startMap: () ->
    index = 0
    for child_id of @mappers
      startMapper child_id, index++

  startMapper: (mapper, index) ->
    @send mapper, { name: 'MAP_START', index: index, job_id: @jobid, url: @urls[index]}

  checkMappers: () ->
    # read through messages to see if anyone finished, if all finished, or if anyone is dead

    for message_id, message of @queue
      if message.name is 'MAPPER_DONE'
        node = message.worker_id
        @mapStatus[node].done = true
        @doneMappers.push node
        @numMappersLeft--
        delete @queue[message_id]
        if @status is "MAPPING_STARTED"
          @status = "MAPPING_ENDING_FIRST"

    toRetry = []
    for worker_id of @mappers
      now = new Date().now()
      if now - @clientMap[worker_id].last_update > 4 * constants.HEARTBEAT_INTERVAL
        # he's dead jim
        toRetry.push worker_id

    for failed_worker in toRetry
      @retryMap failed_worker

    if @numMappersLeft == 0 and @status is not "REDUCE_STARTED"
      @status = "REDUCE_START"

  allocateReducers: () ->
    @numReducers = @urls.length
    @reducers = @get_active_clients numReducers
    index = 0
    for child_id of @reducers
      @reduceStatus[child_id] = { done: false, index: index++ }
    @numReducersLeft = numReducers


  finishMappers: () ->
    for mapper in @doneMappers
      @sendReduceNodes mapper
    @doneMappers = []

  startReduce: () ->
    for reducer of @reducers
      @startReducer reducer

  startReducer: (reducer) ->
    @send reducer { name: 'START_REDUCE', number_of_mappers: @mappers.length }

  checkReducers: () ->
    # see if anyone finished
    for message_id, message of @queue
      if message.name is 'REDUCE_DONE'
        node = message.worker_id
        @reduceStatus[node].done = true
        @numReducersLeft--
        delete @queue[message_id]

    # see if anyone failed
    failed_reducers = []
    now = new Date().now()
    for reducer in @reducers
      if now - @clientMap[reducer].last_update > 4 * constants.HEARTBEAT_INTERVAL and not @reduceStatus[reducer].done
        #dis fucker dead
        failed_reducers.push reducer

    if reducer.length > 0
      @retryReducers failed_reducers
    else if @numReducersLeft == 0
      state = "DONE"


  sendReduceNodes: (mapper) ->
    send mapper { name: 'REDUCE_NODES', nodes: @reducers }

  # whether mapper fails before or after it finishes, it needs to be alive the whole time,
  # so that it can send the data to failed reducers
  retryMap: (failed_id) ->
    new_node = get_active_clients(1)[0]
    index = @mapStatus[failed_id].index
    startMapper new_node, index
    @mappers[index] = new_node
    @mapStatus[new_node] = { done: false, index: index}
    if @mapStatus[failed_id].done
      @numMappersLeft++

    delete @mappers[failed_id]
    delete @mapStatus[failed_id]


  retryReducers: (failed_reducers) ->
    for failed_reducer in failed_reducers
      @retryReduce failed_reducer

    # all done mappers need to resend
    for mapper_id, mapper_status of @mapStatus
      if mapper_status.done
        @doneMappers.push mapper_id

    # have all done mappers re-send the data
    @finishMappers()


  #don't call if reducer has already finished, data is written to firebase
  retryReduce: (failed_id) ->
    new_node = get_active_clients(1)[0]
    index = @reduceStatus[failed_id].index

    @reducers[index] = new_node
    @reduceStatus[new_node] = { done: false, index: index }

  # abstraction
  send: (node, jsonMessage) ->
    #todo add to node's firebase message queue


  finish: () ->
    # TODO? does this need to exist?

module.exports = {JobWatcher}