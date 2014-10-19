
firebase_db = require("../serverjs/firebase_db")
constants = require("../constants")
fs = require("fs")

class ReducerSet
  constructor: (reducerid) ->
    @reducerid = reducerid
    @output = {}
    @finished = false

  addLinesFromKey: (key, lines) ->
    if (key not of @output)
      @output[key] = lines


class JobSet
  constructor: (jobid, totalReducers) ->
    @job = jobid
    @reducers = {}
    @totalReducers = totalReducers
    @numReducers = 0

  addReducer: (reducerid) ->
    @reducers[reducerid] = new ReducerSet(reducerid)

  addOutput: (reducerid, key, lines) ->
    @reducers[reducerid].addLinesFromKey(key, lines)

  finishReducer: (reducerid) ->
    @reducers[reducerid].finished = true
    @numReducers++
    if (@numReducers == @totalReducers)
      jobMap = @combineToJobMap()
      str = @serializeJobMap(jobMap)
      @saveToFile(str)
      firebase_db.JOB_STATUS_REF.child(@job).child('output_url')
      .set(constants.OUTPUT_DIR + @job)

  combineToJobMap: () ->
    jobMap = {}
    for _,reducer of @reducers
      for key, lines of reducer.output
        jobMap[key] = lines
    return jobMap

  serializeJobMap: (jobMap) ->
    str = ""
    keylist = []
    for key,_ of jobMap
      keylist.push(key)
    keylist.sort()
    for key in keylist
      str += @keyToString(jobMap, key)
    return str

  keyToString: (map, key) ->
    str = ""
    for line in map[key]
      str += key + ":" + line + "\n"
    return str

  saveToFile: (str) ->
    console.log("Finished Job " + @job)
    fs.writeFileSync(constants.TOP_DIR + constants.OUTPUT_DIR + @job, str)

test = (fb) ->
  fb.push({name: "START_JOB", job: 7, numReducers: 1})
  fb.push({name: "START_REDUCER_OUTPUT", job: 7, reducer: 1})
  fb.push({
    name: "REDUCER_OUTPUT",
    job: 7,
    reducer: 1,
    key: "Josh",
    lines: ["d", "e", "f"]})
  fb.push({name: "START_REDUCER_OUTPUT", job: 7, reducer: 2})
  fb.push({
    name: "REDUCER_OUTPUT",
    job: 7,
    reducer: 2,
    key: "Dan",
    lines: ["f", "g", "h"]})
  fb.push({
    name: "REDUCER_OUTPUT",
    job: 7,
    reducer: 2,
    key: "John",
    lines: ["x", "y", "z"]})
  fb.push({
    name: "REDUCER_OUTPUT",
    job: 7,
    reducer: 1,
    key: "David",
    lines: ["a", "b", "c"]})
  fb.push({name: "STOP_REDUCER_OUTPUT", job: 7, reducer: 2})
  fb.push({name: "START_REDUCER_OUTPUT", job: 7, reducer: 2})
  fb.push({name: "STOP_REDUCER_OUTPUT", job: 7, reducer: 1})

main = () ->
  fb = firebase_db.IO_SERVER_MESSAGE_REF
  currentJobs = {}
  currentJobs[7] = new JobSet(7, 2)

  fb.on("child_added", newMessage = (snapshot) ->
    message = snapshot.val()
    switch message.name
      when "START_JOB" then (
        if (not (currentJobs[message.job] == -1))
          currentJobs[message.job] =
          new JobSet(message.job, message.numReducers)
      )
      when "START_REDUCER_OUTPUT" then (
        if (not((currentJobs[message.job] == null or currentJobs[message.job] == -1)))
          currentJobs[message.job].addReducer(message.reducer)
      )
      when "REDUCER_OUTPUT" then (
        if (not((currentJobs[message.job] == null or currentJobs[message.job] == -1)))
          currentJobs[message.job]
          .addOutput(message.reducer, message.key, message.lines)
      )
      when "STOP_REDUCER_OUTPUT" then (
        if (not((currentJobs[message.job] == null or currentJobs[message.job] == -1)))
          currentJobs[message.job].finishReducer(message.reducer)
          currentJobs[message.job] = -1
      )
    fb.child(snapshot.name()).remove()
  )
  #test(fb)


main()
