/*
This test suite tests the implementation of a few edge cases that require specifice db configuration. 
*/
const request = require('supertest')
const assert = require('assert')
const express = require('express')
const fs = require('fs-extra')
const app = express()
const startupDB = require('../dist/server.js')

// try {
//     fs.rmSync('./archive', { recursive: true })
// } catch (e) {}
// try {
//     fs.rmSync('./leesplank', { recursive: true })
// } catch (e) {}
// try {
//     fs.rmSync('./backup1', { recursive: true })
// } catch (e) {}
// try {
//     fs.rmSync('./backup2', { recursive: true })
// } catch (e) {}

const pullOplog = startupDB.beforeGet(async (req, res, next, collection) => {
    await req.startupDB.pullOplog('origineel')
    return { statusCode: 0 }
})

app.use(express.json({ inflate: true, limit: '100mb' }))
app.use('/leesplank/origineel', [pullOplog])

app.use('/leesplank', startupDB.db({ readOnly: true }))
const server = app.listen(3456)

describe('Behaviour: PUT command', function () {
    it('executing a PUT command on a readonly database should return an error', function (done) {
        request(app)
            .put('/leesplank/origineel')
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

describe('Behaviour: GET command', function () {
    it('executing a GET command on a readonly database should return all data in collection', function (done) {
        request(app)
            .get('/leesplank/origineel')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body[0].id, 'Aap')
                assert.strictEqual(res.body[1].id, 'Noot')
                assert.strictEqual(res.body[2].id, 'Mies')
                assert.strictEqual(res.body[3].id, 'Wim')
                assert.strictEqual(res.body[4].id, 'Zus')
                assert.strictEqual(res.body[5].id, 'Jet')
                assert.strictEqual(res.body.length, 6)
            })
            .end(done)
    })

    it('executing a GET command on a readonly database should return all data in collection, including new transactions pulled in by a pullOplog hook', function (done) {
        fs.ensureDirSync('./leesplank/oplog/origineel')
        fs.writeFileSync('./leesplank/oplog/origineel/latest.ndjson', `{"operation":"create","collection":"origineel","data":[{"id":"pullOplog"}]}\n`)
        request(app)
            .get('/leesplank/origineel')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body[0].id, 'Aap')
                assert.strictEqual(res.body[1].id, 'Noot')
                assert.strictEqual(res.body[2].id, 'Mies')
                assert.strictEqual(res.body[3].id, 'Wim')
                assert.strictEqual(res.body[4].id, 'Zus')
                assert.strictEqual(res.body[5].id, 'Jet')
                assert.strictEqual(res.body[6].id, 'pullOplog')
                assert.strictEqual(res.body.length, 7)
            })
            .end(done)
    })

    it('executing a GET command on a readonly database should return all data in collection, including newest transactions pulled in by a pullOplog hook', function (done) {
        fs.ensureDirSync('./leesplank/oplog/origineel')
        fs.appendFileSync('./leesplank/oplog/origineel/latest.ndjson', `{"operation":"create","collection":"origineel","data":[{"id":"pullOplog2"},{"id":"pullOplog3"}]}\n`)
        request(app)
            .get('/leesplank/origineel')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body[0].id, 'Aap')
                assert.strictEqual(res.body[1].id, 'Noot')
                assert.strictEqual(res.body[2].id, 'Mies')
                assert.strictEqual(res.body[3].id, 'Wim')
                assert.strictEqual(res.body[4].id, 'Zus')
                assert.strictEqual(res.body[5].id, 'Jet')
                assert.strictEqual(res.body[7].id, 'pullOplog2')
                assert.strictEqual(res.body[8].id, 'pullOplog3')
                assert.strictEqual(res.body[6].id, 'pullOplog')
                assert.strictEqual(res.body.length, 9)
            })
            .end(done)
    })
})

server.close()
