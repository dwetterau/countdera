express          = require('express')
path             = require('path')
favicon          = require('serve-favicon')
logger           = require('morgan')
cookieParser     = require('cookie-parser')
bodyParser       = require('body-parser')

{ config }       = require('./lib/common')

indexRoute       = require('./routes/routes')

firebase = require('./serverjs/firebase_db')
{JobWatcher} = require('./serverjs/job_watcher')

app = express()

# view engine setup
app.set 'views', path.join(__dirname, 'views')
app.set 'view engine', 'jade'

app.use favicon(__dirname + '/public/favicon.ico')
app.use logger('dev')
app.use bodyParser.json()
app.use bodyParser.urlencoded {extended: true}
app.use cookieParser()
app.use express.static(path.join(__dirname, '/public'))
app.use express.static('/countdera')

app.use '/', indexRoute

# catch 404 and forward to error handler
app.use (req, res, next) ->
  err = new Error('Not Found')
  err.status = 404
  next(err)

if app.get('env') == 'local'
  # development error handler
  # will print stacktrace in local mode
  app.use (err, req, res, next) ->
    res.status(err.status || 500)
    errorObj =
      message: err.message
      error: err
    res.render('error', errorObj)
else
  # production error handler
  # no stacktraces leaked to user
  app.use (err, req, res, next) ->
    res.status(err.status || 500)
    errorObj =
      message: err.message
      error: {}
    res.render('error', errorObj)

watchers = {}
process_message = (message_id, message) ->
  jobid = message.job_id
  if jobid not of watchers
    watchers[jobid] = new JobWatcher(jobid)
    watchers[jobid].run()
  watchers[jobid].addMessage(message_id, message)
  if message.name == 'FINISH_JOB'
    delete watchers[jobid]


message_ref = firebase.SERVER_MESSAGE_REF
message_ref.on 'child_added', (new_child) ->
  process_message(new_child.name(), new_child.val())
  message_ref.child(new_child.name()).remove()

app.listen(config.get('PORT'))

module.exports = app
