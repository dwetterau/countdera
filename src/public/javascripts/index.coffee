master = require('./master.coffee')
client = require('./client.coffee')

# If the path ends with master, run the master code. Otherwise run the client code.
path = window.location.pathname
if path.slice(path.length - 'master'.length) == 'master'
  master.run()
else
  client.run()