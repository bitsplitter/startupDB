const get = async function () {
    return { "key": "value" }
}
const startupDB = {
    "checkPoint": 0,
    "nextOplogId": 1,
    "options": options,
    "data": {},
    "get": get
}
module exports = function(options) {

    return startupDB
}