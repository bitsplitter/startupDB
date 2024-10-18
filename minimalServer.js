const fs = require('fs')
const express = require('express')
const app = express()
const startupDB = require('./dist/server')

app.use(express.json({ inflate: true, limit: '1mb' }))

app.use(
    '/test',
    startupDB.db({
        testing: true,
        dataFiles: './testDB',
        addTimeStamps: function (operation, object, oldObject) {
            if (operation == 'created') object.__created = new Date().getTime()
            if (operation == 'modified') {
                object.__modified = new Date().getTime()
                if (oldObject) object.__created = oldObject.__created
            }
        },
        streamObjects: true,
        serveRawCheckpoint: true,
    })
)

app.listen(3456)
console.log('Server listening on port 3456')
