firebase = require './firebase_db'
constants = require '../constants'
Q = require 'q'

module.exports =
  get_job_id: () ->
    job_name = firebase.ROOT_REF.child(constants.FIREBASE_JOB_IDS).push {'in_use': false}
    console.log job_name.name()
    return job_name.name()

  get_active_clients: () ->
    now = new Date().getTime()
    active_clients = {}
    for child_id, child of @_clients
      # Allow some time for RTT + delay
      if now - child.last_update < 4 * constants.HEARTBEAT_INTERVAL
        active_clients[child_id] = child
    return active_clients

  listen: () ->
    client_status_ref = firebase.WORKER_STATUS_REF
    client_status_ref.on 'child_changed', (snapshot) =>
      @_clients[snapshot.name()] = snapshot.val()
    client_status_ref.on 'child_removed', (snapshot) =>
      delete @_clients[snapshot.name()]
