{Worker} = require './lib/models/worker.coffee'

module.exports =
  run: () ->
    temporary_worker = new Worker()
    temporary_worker.init()

    temporary_worker.start_reduce
      job_id: "-JZa4Sxsw0kFe6Jt5dOt"
      index: 0
      number_of_mappers: 1

    temporary_worker.add_data_src
      index: 0

    url = "http://dwett.com/data/file1.txt"
    array = [['Alpha', 1], ['Delta', 1], ['Charlie', 1], ['Bravo', 1], ['Echo', 1],
             ['Alpha', 1], ['Charlie', 1], ['Bravo', 1], ['Bravo', 1], ['bravo', 1]]
    for tuple in array
      temporary_worker.add_map_output
        index: 0
        key: tuple

    setTimeout () ->
      temporary_worker.close_data_src
        index: 0
    , 1000

