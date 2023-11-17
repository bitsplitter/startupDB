const { Readable } = require('stream')
class ObjectStream extends Readable {
    constructor(obj) {
        super({ objectMode: true })
        this.keys = Object.keys(obj)
        this.index = 0
        this.obj = obj
    }

    _read() {
        if (this.index == 0) this.push('{')
        if (this.index < this.keys.length) {
            const key = this.keys[this.index]
            const value = JSON.stringify(this.obj[key])
            if (value) this.push((this.index != 0 ? ',' : '') + `"${key}":${value}`)
            else this.push('')
            this.index++
        } else {
            this.push('}')
            this.push(null) // No more data
        }
    }
}

class ArrayStream extends Readable {
    constructor(obj) {
        super({ objectMode: true })
        this.index = 0
        this.obj = obj
        this.length = this.obj.length
    }

    _read() {
        if (this.index == 0) this.push('[')
        if (this.index < this.length) {
            const value = JSON.stringify(this.obj[this.index])
            if (value) this.push((this.index != 0 ? ',' : '') + value)
            else this.push('')
            this.index++
        } else {
            this.push(']')
            this.push(null) // No more data
        }
    }
}
export default {
    ObjectStream,
    ArrayStream,
}
