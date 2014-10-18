{Client} = require './lib/models/client.coffee'

module.exports =
  run: () ->
    temporary_client = new Client()
    temporary_client.init()

    setTimeout () ->
      id1 = temporary_client.id()
      html_string = '<div>Client id=' + id1 + '</div>'
      $('#id_div').html html_string
    , 1000

