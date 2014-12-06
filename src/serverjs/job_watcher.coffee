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
    @doneMappers = []
    @clientMap = {}

  initFromFirebase: (callback) ->
    firebase.WORKER_STATUS_REF.on 'value', (snapshot) =>
      for name, value of snapshot.val()
        @clientMap[name] = value

    # keep it ALL in memory because reasons
    @clientMap = {}
    firebase.WORKER_STATUS_REF.once 'value', (snapshot) =>
      for name, value of snapshot.val()
        @clientMap[name] = value

      # get urls from firebase
      firebase.JOB_STATUS_REF.child(@jobid).child('urls').once 'value', (snapshot) =>
        @urls = snapshot.val()
        @startIOServer()
        callback()

  startIOServer: () ->
    firebase.IO_SERVER_MESSAGE_REF.push
      name: 'START_JOB',
      numReducers: @urls.length,
      job: @jobid

  get_active_clients: (num) ->
    now = new Date().getTime()
    active_clients = []
    for child_id, child of @clientMap
      # Allow some time for RTT + delay
      if now - child.last_update < 10 * constants.HEARTBEAT_INTERVAL and (
        child.status.state == 'IDLE')
        child.status.state == 'NOT_IDLE'
        active_clients.push child_id
      if active_clients.length == num
        break
    return active_clients

  addMessage: (messageid, message) ->
    @queue[messageid] = message
    if message.name == 'FINISH_JOB'
      @finish()

  run: () ->
    @initFromFirebase () =>
      @loop()

  loop: () ->
    switch @status
      when "STARTING"
        @allocateMappers()
        @startMap()
        @status = 'MAPPING_STARTED'
      when "MAPPING_STARTED"
        @checkMappers()
      when "MAPPING_ENDING_FIRST"
        @allocateReducers()
        @startReduce()
        @finishMappers()
        @status = "MAPPING_ENDING"
      when "MAPPING_ENDING"
        @checkMappers()
        @finishMappers()
      when "REDUCE_START"
        @status = "REDUCE_STARTED"
        @checkMappers()
        @finishMappers()
      when "REDUCE_STARTED"
        @checkMappers()  # so that if a reducer fails we have the data to send it?
        @finishMappers()
        @checkReducers()
      else
        # done
        console.log 'other state', @status


    if @status != "DONE"
      setTimeout () =>
        @loop()
      , 1


  allocateMappers: () ->
    numMappers = @urls.length
    @mappers = @get_active_clients numMappers
    index = 0
    for child_id in @mappers
      @mapStatus[child_id] = { done: false, index: index }
      index++
    @numMappersLeft = numMappers

  startMap: () ->
    index = 0
    for child_id in @mappers
      @startMapper child_id, index++

  startMapper: (mapper, index) ->
    @send mapper, { name: 'MAP_START', index: index, job_id: @jobid, url: @urls[index]}

  checkMappers: () ->
    # read through messages to see if anyone finished, if all finished, or if anyone is dead

    for message_id, message of @queue
      if message.name == 'MAPPER_DONE'

        node = message.id
        if @mapStatus[node].done
          continue
        @numMappersLeft--
        @mapStatus[node].done = true
        @doneMappers.push node
        delete @queue[message_id]
        if @status == "MAPPING_STARTED"
          @status = "MAPPING_ENDING_FIRST"


    toRetry = []
    for worker_id in @mappers
      now = new Date().getTime()
      if now - @clientMap[worker_id].last_update > 15 * constants.HEARTBEAT_INTERVAL
        # he's dead jim
        toRetry.push worker_id

    if toRetry.length > 0
      for failed_worker in toRetry
        @retryMap failed_worker

    if @numMappersLeft == 0 and @status == "MAPPING_ENDING"
      @status = "REDUCE_START"

  allocateReducers: () ->
    @reduceStatus = {}
    @numReducers = @urls.length
    @reducers = @get_active_clients @numReducers
    index = 0
    for child_id in @reducers
      @reduceStatus[child_id] = { done: false, index: index++ }
    @numReducersLeft = @numReducers


  finishMappers: () ->
    if @doneMappers.length == 0
      return
    for mapper in @doneMappers
      @sendReduceNodes mapper
    @doneMappers = []

  startReduce: () ->
    index = 0
    for reducer in @reducers
      @startReducer reducer, index++

  startReducer: (reducer, index) ->
    @send reducer,
      name: 'START_REDUCE',
      job_id: @jobid,
      index: index,
      number_of_mappers: @mappers.length

  checkReducers: () ->
    # see if anyone finished
    for message_id, message of @queue
      if message.name == 'REDUCE_DONE'
        node = @reducers[message.index]
        @reduceStatus[node].done = true
        @numReducersLeft--
        delete @queue[message_id]
    # see if anyone failed
    failed_reducers = []
    now = new Date().getTime()
    for reducer in @reducers
      if now - @clientMap[reducer].last_update > 15 * constants.HEARTBEAT_INTERVAL and (
        not @reduceStatus[reducer].done)
        #dis fucker dead
        failed_reducers.push reducer

    if failed_reducers.length > 0
      @retryReducers failed_reducers
    else if @numReducersLeft == 0
      @status = "DONE"


  sendReduceNodes: (mapper) ->
    @send mapper, { name: 'REDUCE_NODES', nodes: @reducers }

  # whether mapper fails before or after it finishes, it needs to be alive the whole time,
  # so that it can send the data to failed reducers
  retryMap: (failed_id) ->
    new_node = @get_active_clients(1)[0]
    if not new_node?
      return
    index = @mapStatus[failed_id].index
    @startMapper new_node, index
    @mappers[index] = new_node

    if @mapStatus[failed_id].done
      @numMappersLeft++

    delete @mapStatus[failed_id]
    @mapStatus[new_node] = { done: false, index: index }


  retryReducers: (failed_reducers) ->
    for failed_reducer in failed_reducers
      @retryReduce failed_reducer

    # all done mappers need to resend
    for mapper_id, mapper_status of @mapStatus
      if mapper_status.done
        @doneMappers.push mapper_id

    # have all done mappers re-send the data
    if @doneMappers.length > 0
      @finishMappers()


  #don't call if reducer has already finished, data is written to firebase
  retryReduce: (failed_id) ->
    new_node = @get_active_clients(1)[0]
    if not new_node?
      return
    index = @reduceStatus[failed_id].index

    @reducers[index] = new_node
    @reduceStatus[new_node] = { done: false, index: index }

    @startReducer new_node, index

  # abstraction
  send: (node, jsonMessage) ->
    worker = firebase.WORKER_MESSAGE_REF.child(node)
    worker.push jsonMessage, ((error) ->
      if error?
        console.log 'error sending message'
        console.log error)


  finish: () ->
    # tell the mappers they are done (if they're still around)
    if not @mappers?
      return
    for mapper in @mappers
      @send mapper, { name: 'JOB_DONE' }

module.exports = {JobWatcher}