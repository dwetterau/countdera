{Client} = require './lib/models/client.coffee'
firebase = require './lib/firebase_client.coffee'

module.exports =
  run: () ->
    job_id = get_job_id_from_url()
    client = new Client(job_id)
    client.init()

    setTimeout () ->
      add_handlers(client)
    , 100

add_handlers = (client) ->
  $('#save_map').click () ->
    map_code = trim_code($('#map_code_area').val())
    client.save_map_code(map_code)

    # Switch to the reduce code view.
    $("#header_text").html("Enter your reduce code:")
    $("#map_div").slideUp 300, () ->
      $("#reduce_div").slideDown(300)

  $('#save_reduce').click () ->
    reduce_code = trim_code($('#reduce_code_area').val())
    client.save_reduce_code(reduce_code)

    # Switch to the URL input view.
    $("#header_text").html("Enter the URLs for your data:")
    $("#reduce_div").slideUp 300, () ->
      $("#url_div").slideDown(300)

  $('#save_urls').click () ->
    urls = (s.trim() for s in $("#url_area").val().split(','))
    client.save_urls(urls)

    $("#header_text").html("Job Running!")
    $("#url_div").slideUp 300

  firebase.JOB_STATUS_REF.child(client._id).on 'child_added', (snapshot) ->
    if snapshot.name() == 'output_url'
      url = 'http://' + window.location.hostname + '/' + snapshot.val()
      $('#output_url_div').html('<a href="' + url + '">Click to download output!</a>')
      client.finish_job()

trim_code = (text) ->
  lines = text.trim().split "\n"

  # TODO: Verify the input? Nah.
  return lines.slice(1, lines.length - 1).join('\n')


get_job_id_from_url = () ->
  path = window.location.pathname
  return path.substring(path.lastIndexOf('/') + 1)
