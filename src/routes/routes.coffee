express = require 'express'
router = express.Router()
job_manager = require '../serverjs/job_manager'

# GET home page
router.get '/client', (req, res) ->
  # This should generate a room_id that other users will use to connect.
  job_id = job_manager.get_job_id()
  res.redirect '/client/' + job_id

router.get '/', (req, res) ->
  res.render 'welcome'

router.get '/client/:job_id', (req, res) ->
  # If the master is the first to connect, no other connections should be
  # allowed (there should only be one master).
  job_id = req.params.job_id
  res.render 'client',
    {job_id}

router.get '/worker', (req, res) ->
  res.render 'worker'

router.get '/demo', (req, res) ->
  res.render 'iframed.jade'

module.exports = router