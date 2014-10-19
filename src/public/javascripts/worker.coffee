{Worker} = require './lib/models/worker.coffee'

module.exports =
  run: () ->
    worker = new Worker()

    setTimeout () ->
      $('#a').click () ->
        $('#a').hide()
        worker.init()
    , 100
 