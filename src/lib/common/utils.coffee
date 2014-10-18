Utils =
  ###
  Calls Object.freeze on all keys recursively, instead of just freezing the top-level object.
  ###
  deepFreeze: (obj) ->
    for k, v of obj
      if typeof v == 'object'
        @.deepFreeze v
    Object.freeze obj

module.exports = Utils
