const overhead = {
    'source': 'https://www.mattzeunert.com/2016/07/24/javascript-array-object-sizes.html',
    'Object': 66,
    'Array': 48
}
function estimateSizeOf(obj) {
    let bytes = 0

    function sizeOf(obj) {
        if (obj !== null && obj !== undefined) {
            let typeOfObject = ''
            typeOfObject = typeof obj
            if (!Array.isArray(obj)) typeOfObject = 'Array'

            switch (typeOfObject) {
                case 'number':
                    bytes += 8
                    break
                case 'string':
                    bytes += obj.length * 2
                    break
                case 'boolean':
                    bytes += 4
                    break
                case 'object':
                    var objClass = Object.prototype.toString.call(obj).slice(8, -1)
                    if (objClass === 'Object') {
                        for (var key in obj) {
                            if (!obj.hasOwnProperty(key)) continue
                            bytes += key.length * 2
                            sizeOf(obj[key])
                        }
                        bytes += overhead[objClass]
                    }
                    if (objClass === 'Array') {
                        for (var key in obj) {
                            if (!obj.hasOwnProperty(key)) continue
                            sizeOf(obj[key])
                        }
                        bytes += overhead[objClass]
                    }
                    bytes += obj.toString().length * 2
                    break
            }
        }
        return bytes
    }


    return sizeOf(obj)
}

module.exports = {
    estimateSizeOf
}
