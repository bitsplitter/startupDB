const express = require('express')
const startupDB = require('./dist/server.js')

const app = express()
app.use(express.json())
app.use('/myDB', [startupDB.db({ dataFiles: './resources' })])
app.listen(3000)
