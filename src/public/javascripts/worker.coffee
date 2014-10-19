{Worker} = require './lib/models/worker.coffee'

module.exports =
  run: () ->
    temporary_worker = new Worker()
    temporary_worker.init()

    temporary_worker.start_map
      job_id: "-JZa4Sxsw0kFe6Jt5dOt"
      index: 0
      url: "http://dwett.com/data/file1.txt"
