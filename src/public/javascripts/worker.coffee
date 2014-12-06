{Worker} = require './lib/models/worker.coffee'

module.exports =
  run: () ->
    stateMapper = (status, other) ->
      switch status
        when "IDLE"
          str = "Waiting to be scheduled."
          percentage = "0"
        when "MAPPER"
          str = "Scheduled as a mapper."
          percentage = "14"
        when "DOWNLOADING_MAPPING_DATA"
          str = "Downloading map input data."
          percentage = "29"
        when "DOWNLOADING_MAPPING_CODE"
          str = "Downloading map job code."
          percentage = "43"
        when "MAPPING"
          str = "Running the map job."
          percentage = "57"
        when "MAPPER_DONE"
          str = "Finished running the map job."
          percentage = "71"
        when "SENDING_MAPPED_RESULTS"
          str = "Sending map output to reducers."
          percentage = "86"
        when "REDUCER"
          str = "Scheduled as a reducer."
          percentage = "17"
        when "RECEIVING_REDUCE_DATA"
          str = "Receiving map output data."
          percentage = "33"
        when "DOWNLOADING_REDUCE_CODE"
          str = "Downloading reduce job code."
          percentage = "50"
        when "REDUCING"
          str = "Running the reduce job."
          percentage = "67"
        when "SENDING_TO_IO_SERVER"
          str = "Sending reduce output to server."
          percentage = "83"
        else
          return

      # If the percentage != 0, show the progress bar with that percentage
      if percentage != "0"
        $("#progress_bar").removeClass("progress-bar-success")
          .attr("aria-valuenow", percentage)
          .css("width", percentage + "%")
          .show()
      else
        # If the progress bar is visible and the percentage is 0, then make it green
        if $("#progress_bar").is(":visible")
          $("#progress_bar").addClass("progress-bar-success")
            .css("width", "100%")
            .attr("aria-valuenow", "100")

      $("#status_text").text(str)
      $("#extra_status_text").text(if other? then other else '')

    worker = new Worker(stateMapper)
    setTimeout () ->
      $('#a').click () ->
        $('#a').slideUp()
        worker.init()
        $("#worker_status").show()
    , 100
