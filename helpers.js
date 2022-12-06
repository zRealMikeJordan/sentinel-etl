module.exports.sleep = function (ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms)
  })
}

module.exports.isValidJson = function (data) {
  try {
    const str = JSON.stringify(data)
    return JSON.parse(str) && !!str
  } catch (e) {
    return false
  }
}
