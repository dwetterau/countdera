{Worker} = require './lib/models/worker.coffee'

module.exports =
  run: () ->
    temporary_worker = new Worker()
    temporary_worker.init()

    setTimeout () ->
      id1 = temporary_client.id()
      html_string = '<div>Worker id=' + id1 + '</div>'
      $('#id_div').html html_string
    , 1000

