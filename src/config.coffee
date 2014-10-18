hostname = 'localhost'
if window?
  if window.location.host.indexOf(':3000') == -1
    hostname = window.location.host

module.exports =
  FIREBASE_NAME: 'FIREBASE_NAME_HERE'

