const { Readable } = require('stream')

const MAX_BUFFER_SIZE = 65536

class jsonObjectStream extends Readable {
    constructor(obj) {
        super({ objectMode: true })
        this.keys = Object.keys(obj)
        this.index = 0
        this.obj = obj
    }

    _read() {
        let buf = ''
        if (this.index == 0) this.push('{')
        while (this.index < this.keys.length && buf.length < MAX_BUFFER_SIZE) {
            const key = this.keys[this.index]
            const value = JSON.stringify(this.obj[key])
            if (value) buf = buf + (this.index != 0 ? ',' : '') + `"${key}":${value}`
            this.index++
        }
        if (buf.length > 0) this.push(buf)
        if (this.index >= this.keys.length) {
            this.push('}')
            this.push(null) // No more data
        }
    }
}

class jsonArrayStream extends Readable {
    constructor(obj) {
        super({ objectMode: true })
        this.index = 0
        this.obj = obj
        this.length = this.obj.length
    }

    _read() {
        let buf = ''
        if (this.index == 0) this.push('[')
        while (this.index < this.length && buf.length < MAX_BUFFER_SIZE) {
            const value = JSON.stringify(this.obj[this.index])
            if (value) buf = buf + (this.index != 0 ? ',' : '') + value
            this.index++
        }
        if (buf.length > 0) this.push(buf)
        if (this.index >= this.length) {
            this.push(']')
            this.push(null) // No more data
        }
    }
}

class ndJsonObjectStream extends Readable {
    constructor(obj) {
        super({ objectMode: true })
        this.keys = Object.keys(obj)
        this.index = 0
        this.obj = obj
    }

    _read() {
        let buf = ''
        while (this.index < this.keys.length && buf.length < MAX_BUFFER_SIZE) {
            const key = this.keys[this.index]
            const value = JSON.stringify(this.obj[key])
            if (value) buf = buf + value + '\n'
            this.index++
        }
        if (buf.length > 0) this.push(buf)
        if (this.index >= this.keys.length) {
            this.push(null) // No more data
        }
    }
}

class ndJsonArrayStream extends Readable {
    constructor(obj) {
        super({ objectMode: true })
        this.index = 0
        this.obj = obj
        this.length = this.obj.length
    }

    _read() {
        let buf = ''
        while (this.index < this.length && buf.length < MAX_BUFFER_SIZE) {
            const value = JSON.stringify(this.obj[this.index])
            if (value) buf = buf + value + '\n'
            this.index++
        }
        if (buf.length > 0) this.push(buf)
        if (this.index >= this.length) {
            this.push(null) // No more data
        }
    }
}

export default {
    jsonObjectStream,
    jsonArrayStream,
    ndJsonObjectStream,
    ndJsonArrayStream,
}
