/*
This test suite tests the implementation of a few edge cases that require specifice db configuration.
*/
const fs = require('fs')
const path = require('path')
const request = require('supertest')
const assert = require('assert')
const express = require('express')
const startupDB = require('../dist/server.js')

try {
    fs.rmSync('./archive', { recursive: true })
} catch (e) {}
try {
    fs.rmSync('./leesplank', { recursive: true })
} catch (e) {}
try {
    fs.rmSync('./backup1', { recursive: true })
} catch (e) {}
try {
    fs.rmSync('./backup2', { recursive: true })
} catch (e) {}

const app = express()
app.use(express.json({ inflate: true, limit: '100mb' }))

app.use('/leesplank', startupDB.db({ readOnly: true }))

fs.mkdirSync(path.join(__dirname, '..', 'tmp'), { recursive: true })
const pullDir = fs.mkdtempSync(path.join(__dirname, '..', 'tmp', 'pullOplog-'))
const pullApp = express()
pullApp.use(express.json())
pullApp.use(
    '/db',
    startupDB.beforeGet(async function (req, res, next, collection) {
        await req.startupDB.pullOplog(collection)
        return { statusCode: 0 }
    })
)
pullApp.use('/db', startupDB.db({ dataFiles: pullDir }))

describe('Behaviour: PUT command', function () {
    it('executing a PUT command on a readonly database should return an error', function (done) {
        request(app)
            .put('/leesplank')
            .set('Content-type', 'application/json')
            .send([
                { id: 'Wim', description: 'Een broer.' },
                { id: 'Zus', description: 'Een baby.' },
                { id: 'Jet', description: 'Een zus.' },
            ])
            .expect(403)
            .end(done)
    })
})

describe('Behaviour: pullOplog on a writable (secondary) server', function () {
    const collection = 'sync'
    after(function () {
        fs.rmSync(pullDir, { recursive: true, force: true })
    })

    it('applies new oplog files from disk without aborting the GET with 403', async function () {
        await request(pullApp).post(`/db/${collection}`).set('Content-type', 'application/json').send({ id: 'seed', v: 1 }).expect(200)

        // Collection is warm with nextOpLogId 2. Another writer drops op 2 on the shared oplog folder.
        const op2 = {
            operation: 'create',
            collection,
            data: [{ id: 'fromPrimary', v: 2 }],
            opLogId: 2,
            timestamp: { unixTimeStamp: 1, timeStamp: 'test' },
        }
        fs.mkdirSync(path.join(pullDir, 'oplog', collection), { recursive: true })
        fs.writeFileSync(path.join(pullDir, 'oplog', collection, '2.json'), JSON.stringify(op2))

        const res = await request(pullApp).get(`/db/${collection}`).expect(200)
        const ids = res.body.map((d) => d.id).sort()
        assert.deepStrictEqual(ids, ['fromPrimary', 'seed'])
    })
})
