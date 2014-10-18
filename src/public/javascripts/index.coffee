client = require('./client.coffee')
worker = require('./worker.coffee')

# If the path ends with master, run the master code. Otherwise run the client code.
path = window.location.pathname
if path.indexOf('/client') == 0
  client.run()
else
  worker.run()