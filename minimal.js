const express = require('express')
const startupDB = require('./dist/server.js')

const consoleLogger = function (req, res, next) {
    console.log(req.url)
    next()
}

const app = express()
app.use(express.json())
app.use(consoleLogger)
app.use('/myDB', [startupDB.db({ dataFiles: './resources' })])
app.listen(3000)
