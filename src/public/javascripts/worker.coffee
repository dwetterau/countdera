{Worker} = require './lib/models/worker.coffee'

module.exports =
  run: () ->
    stateMapper = (status) ->
      console.log "I am a strong independent callback who don't need no context"
      switch status
        when "IDLE"
          str = "BigData Corporation-Senpai has not yet noticed you"
        when "MAPPER"
          str = "Yayyy. I am a mapper, proud to MapTx. (It sounds like HackTX)"
        when "DOWNLOADING_MAPPING_DATA"
          str = "Downloading Mapping Data, To Make Some Checkbooks Fatta"
        when "DOWNLOADING_MAPPING_CODE"
          str = "Downloading Mapping Code, This Won't take long to Load"
        when "MAPPING"
          str = "I am done downloading, so I can map. Hopefully after, I'll take a nap"
        when "MAPPER_DONE"
          str = "I've finished my map, my job's almost done."
        when "SENDING_MAPPED_RESULTS"
          str = "Sending to reducer to finish the run."
        when "REDUCER"
          str = "Yes. I am the reducer. Good day to you, sir"
        when "RECEIVING_REDUCE_DATA"
          str = "Getting processed mapped data. Reduce, Reuse, Your Cycles"
        when "DOWNLOADING_REDUCE_CODE"
          str = "Getting reduce code. I hate node."
        when "REDUCING"
          str = "We've got our data, it's time to reduce. TODO: jagnew make joke"
        when "SENDING_TO_IO_SERVER"
          str = "ERROR WE CRASHED. Just Kidding. I couldn't resist. IOserver write."
        else
          return
      currhtml = $("#worker_status").html()
      str = currhtml + "<br>" + str
      $("#worker_status").html(str)

    worker = new Worker(stateMapper)
    setTimeout () ->
      $('#a').click () ->
        $('#a').slideUp()
        worker.init()
    , 100
