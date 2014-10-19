{Worker} = require './lib/models/worker.coffee'

module.exports =
  run: () ->
    temporary_worker = new Worker()
    temporary_worker.init()


