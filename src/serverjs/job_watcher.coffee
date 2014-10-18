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
        @start()
      when "MAPPING_STARTED"
        @checkMappers()
      when "MAPPING_ENDING_FIRST"
        @allocateReducers()
        @status = "MAPPING_ENDING"
      when "MAPPING_ENDING"
        @checkMappers
      when "REDUCE_START"
        # TODO init stuff
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


  start: () ->
    @allocateMappers()
    for child_id of @mappers
      @send child_id, { name: 'MAP_START', job_id: @jobid, url: @mapStatus[child_id].url }

  checkMappers: () ->
    # read through messages to see if anyone finished, if all finished, or if anyone is dead

    for message_id, message of @queue
      if message.name is 'MAP_DONE'
        node = message.worker_id
        @mapStatus[node].done = true
        @numMappersLeft--
        delete @queue[message_id]
        if @status is "MAPPING_STARTED"
          @status = "MAPPING_ENDING_FIRST"

    toRetry = []
    for worker_id of @mappers
      now = new Date().now()
      if (now - @clientMap[worker_id].last_update > 4 * constants.HEARTBEAT_INTERVAL)
        # he's dead jim
        toRetry.push worker_id

    for failed_worker in toRetry
      @retryMap failed_worker

    if @numMappersLeft == 0 and @status is not "REDUCE_STARTED"
      @status = "REDUCE_START"

  checkReducers: () ->

  allocateMappers: () ->
    numMappers = @urls.length
    @mappers = @get_active_clients numMappers
    index = 0
    for child_id, child in @mappers
      @mapStatus[child_id] = { done: false, url: @urls[index++] }
    @numMappersLeft = numMappers

  allocateReducers: () ->
    numReducers = @urls.length
    @reducers = @get_active_clients numReducers
    for child_id of @reducers
      @reduceStatus[child_id] = { done: false }
    @numReducersLeft = numReducers

  retryMap: (failed_id) ->
    new_node = get_active_clients(1)[0]
    url = @mapStatus[failed_id].url
    @send new_node, { name: 'MAP_START', job_id: @jobid, url: url}
    @mappers[new_node] = { done: false, url: url}
    if @mapStatus[failed_id].done
      @numMappersLeft++

    delete @mappers[failed_id]


  retryReduce: (failed_id) ->

  # abstraction
  send: (node, message) ->
    #todo add to node's firebase message queue


module.exports = {JobWatcher}