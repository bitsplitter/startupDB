/*
This test suite tests the implementation. 
It checks how data is stored to disk after performing test operations.
*/
for (var key in Object.keys(require.cache)) {
    delete require.cache[key]
}
const fs = require('fs')
const request = require('supertest')
const { assert } = require('chai')
const express = require('express')
const app = express()
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

app.use(express.json({ inflate: true, limit: '1mb' }))
app.use(
    '/leesplank',
    startupDB.db({
        secondaryDataDirs: ['./backup1', './backup2'],
        opLogArchive: './archive',
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

const server = app.listen(3456)

describe('Implementation: POST /leesplank/origineel', function () {
    it('should create an oplog file', function (done) {
        request(app)
            .post('/leesplank/origineel')
            .set('Content-type', 'application/json')
            .send([
                { id: 'Aap', description: 'Een dier met een staart.' },
                { id: 'Noot', description: 'Een harde vrucht.' },
                { id: 'Mies', description: 'De poes.' },
            ])
            .expect(200)
            .expect(function (res) {
                let operation = fs.readFileSync('./leesplank/oplog/origineel/latest.ndjson')
                operation = JSON.parse(operation)
                assert.strictEqual(operation.operation, 'create')
                assert.strictEqual(operation.data[0].id, 'Aap')
                assert.strictEqual(operation.data[1].id, 'Noot')
                assert.strictEqual(operation.data[2].id, 'Mies')
            })
            .end(done)
    })
})

describe('Implementation: POST /leesplank/nieuw', function () {
    it('should create an oplog file', function (done) {
        request(app)
            .post('/leesplank/nieuw')
            .set('Content-type', 'application/json')
            .send([
                { id: 'Maan', description: 'Een hemellichaam.' },
                { id: 'Roos', description: 'Een bloem.' },
                { id: 'Vis', description: 'Een dier.' },
            ])
            .expect(function (res) {
                let operation = fs.readFileSync('./leesplank/oplog/nieuw/latest.ndjson')
                operation = JSON.parse(operation)
                assert.strictEqual(operation.operation, 'create')
                assert.strictEqual(operation.data[0].id, 'Maan')
                assert.strictEqual(operation.data[1].id, 'Roos')
                assert.strictEqual(operation.data[2].id, 'Vis')
            })
            .end(done)
    })
})

describe('Implementation: POST /leesplank (flush origineel )', function () {
    it('should create a checkpoint file when there are unflushed documents', function (done) {
        assert.ok(!fs.existsSync('./leesplank/checkpoint/origineel/latest.ndjson'))
        request(app)
            .post('/leesplank')
            .set('Content-type', 'application/json')
            .send({ command: 'flush', collection: 'origineel', options: { archive: true } })
            .expect(200)
            .expect(function (res) {
                assert.ok(fs.existsSync('./leesplank/checkpoint/origineel/latest.ndjson'))
            })
            .expect(function (res) {
                const dir = fs.readdirSync('./archive/oplog/origineel')
                const fileName = dir[0]
                let operation = fs.readFileSync('./archive/oplog/origineel/' + fileName)
                operation = JSON.parse(operation)
                assert.strictEqual(operation.operation, 'create')
                assert.strictEqual(operation.data[0].id, 'Aap')
                assert.strictEqual(operation.data[1].id, 'Noot')
                assert.strictEqual(operation.data[2].id, 'Mies')
            })
            .end(done)
    })
})

describe('Implementation: POST /leesplank/ndjson)', function () {
    it('should should prepare a collection for the next tests', function (done) {
        request(app)
            .post('/leesplank/ndjson')
            .set('Content-type', 'application/json')
            .send([{ id: 'type', description: 'Newline Delimited JSON.' }])
            .end(done)
    })
})
describe('Implementation: HEAD /leesplank/ndjson', function () {
    it('should return proper HEAD of an ndjson opLog', function (done) {
        request(app)
            .head('/leesplank/ndjson')
            .set('Content-type', 'application/ndjson')
            .expect(200)
            .expect(function (res) {
                assert.deepEqual(res.headers['x-last-checkpoint-time'], '0')
                assert.deepEqual(res.headers['x-last-oplog-id'], '112')
            })
            .end(done)
    })
})
describe('Implementation: POST /leesplank (flush ndjson )', function () {
    it('should create an ndjson checkpoint file', function (done) {
        request(app)
            .post('/leesplank')
            .set('Content-type', 'application/json')
            .send({ command: 'flush', collection: 'ndjson', options: { contentType: 'ndjson', force: true, archive: true } })
            .expect(200)
            .expect(function (res) {
                assert.ok(fs.existsSync('./leesplank/checkpoint/ndjson/latest.ndjson'))
            })
            .end(done)
    })
})
describe('Implementation: HEAD /leesplank/ndjson', function () {
    it('should return proper HEAD of an ndjson checkpoint', function (done) {
        request(app)
            .head('/leesplank/ndjson')
            .set('Content-type', 'application/ndjson')
            .expect(200)
            .expect(function (res) {
                assert.ok(parseFloat(res.headers['x-last-checkpoint-time']) > 1710577775446)
            })
            .end(done)
    })
})
describe('Implementation: POST /leesplank (drop ndjson )', function () {
    it('should drop the ndjson checkpoint file', function (done) {
        request(app)
            .post('/leesplank')
            .set('Content-type', 'application/json')
            .send({ command: 'drop', collection: 'ndjson' })
            .expect(200)
            .expect(function (res) {
                assert.ok(!fs.existsSync('./leesplank/checkpoint/ndjson/latest.ndjson'))
            })
            .end(done)
    })
})

describe('Implementation: POST /leesplank (flush nieuw)', function () {
    it('should create a checkpoint file when there are unflushed documents', function (done) {
        request(app)
            .post('/leesplank')
            .set('Content-type', 'application/json')
            .send({ command: 'flush', collection: 'nieuw', options: { archive: true } })
            .expect(200)
            .expect(function (res) {
                assert.ok(fs.existsSync('./leesplank/checkpoint/nieuw/latest.ndjson'))
            })
            .end(done)
    })
})

describe('Implementation: POST /leesplank/origineel', function () {
    it('should return the POSTed body 1', function (done) {
        request(app)
            .post('/leesplank/origineel')
            .set('Content-type', 'application/json')
            .send([
                { id: 'Wim', description: 'Een broer.' },
                { id: 'Zus', description: 'Een baby.' },
                { id: 'Jet', description: 'Een zus.' },
            ])
            .expect(function (res) {
                let operation = fs.readFileSync('./leesplank/oplog/origineel/latest.ndjson')
                operation = JSON.parse(operation)
                assert.strictEqual(operation.operation, 'create')
                assert.strictEqual(operation.data[0].id, 'Wim')
                assert.strictEqual(operation.data[1].id, 'Zus')
                assert.strictEqual(operation.data[2].id, 'Jet')
            })
            .end(done)
    })
})

describe('Implementation: POST /leesplank', function () {
    it('executing a flush command with an existing checkpoint should create a new checkpoint file when there are unflushed documents', function (done) {
        request(app)
            .post('/leesplank')
            .set('Content-type', 'application/json')
            .send({ command: 'flush', collection: 'origineel', options: { archive: true } })
            .expect(function (res) {
                const dir = fs.readdirSync('./leesplank/checkpoint/origineel')
                assert.deepEqual(fs.existsSync('./leesplank/checkpoint/origineel/latest.ndjson'), true)
                assert.deepEqual(dir.length, 2)
            })
            .end(done)
    })
})

describe('Implementation: DELETE /leesplank/origineel', function () {
    it('should store the DELETEd body in the opLog', function (done) {
        request(app)
            .delete('/leesplank/origineel?id=Wim')
            .set('Content-type', 'application/json')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                let operation = fs.readFileSync('./leesplank/oplog/origineel/latest.ndjson')
                operation = JSON.parse(operation)
                assert.strictEqual(operation.operation, 'delete')
                assert.strictEqual(operation.oldData[0].id, 'Wim')
            })
            .end(done)
    })
})

describe('Implementation: PUT /leesplank/origineel', function () {
    it('should store the PUT body in the opLog', function (done) {
        request(app)
            .put('/leesplank/origineel')
            .set('Content-type', 'application/json')
            .send([
                { id: 'Wim', description: 'Een broer.' },
                { id: 'Zus', description: 'Een baby.' },
                { id: 'Jet', description: 'Een zus.' },
            ])
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                let operations = fs.readFileSync('./leesplank/oplog/origineel/latest.ndjson')
                const operation = JSON.parse(operations.toString().split('\n')[1])
                assert.strictEqual(operation.operation, 'update')
                assert.strictEqual(operation.data[0].id, 'Wim')
                assert.strictEqual(operation.data[1].id, 'Zus')
                assert.strictEqual(operation.data[2].id, 'Jet')
            })
            .end(done)
    })
})

describe('Implementation PATCH /leesplank/origineel', function () {
    it('should store the PATCH body in the opLog', function (done) {
        request(app)
            .patch('/leesplank/origineel')
            .set('Content-type', 'application/json')
            .send([
                {
                    id: 'Zus',
                    patch: [
                        { op: 'replace', path: '/description', value: 'Baby zusje.' },
                        { op: 'add', path: '/english', value: 'Sister' },
                    ],
                },
                { id: 'Jet', patch: [] },
            ])
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                let operations = fs.readFileSync('./leesplank/oplog/origineel/latest.ndjson')
                const operation = JSON.parse(operations.toString().split('\n')[2])
                assert.strictEqual(operation.operation, 'patch')
                assert.strictEqual(operation.data[0].id, 'Zus')
                assert.strictEqual(operation.data[0].patch[0].op, 'replace')
                assert.strictEqual(operation.data[0].patch[0].path, '/description')
                assert.strictEqual(operation.data[0].patch[0].value, 'Baby zusje.')
                assert.strictEqual(operation.data[0].patch[1].op, 'add')
                assert.strictEqual(operation.data[0].patch[1].path, '/english')
                assert.strictEqual(operation.data[0].patch[1].value, 'Sister')
                assert.strictEqual(operation.data[1].id, 'Jet')
                assert.strictEqual(operation.data[1].patch.length, 0)
            })
            .end(done)
    })
})

describe('Implementation: POST /leesplank', function () {
    it('executing a purgeOplog command to revert back to the last flushed state of the collection', function (done) {
        request(app)
            .post('/leesplank')
            .set('Content-type', 'application/json')
            .send({ command: 'purgeOplog', collection: 'origineel' })
            .expect(200)
            .expect(function (res) {
                assert.strictEqual(fs.existsSync('./leesplank/oplog/origineel/1.json'), false)
            })
            .end(done)
    })
})

describe('Implementation POST /leesplank/parent/child', function () {
    it('should return the POSTed body 2', function (done) {
        request(app)
            .post('/leesplank/parent/child')
            .set('Content-type', 'application/json')
            .send([
                { id: 'Aap', description: 'Een dier met een staart.' },
                { id: 'Noot', description: 'Een harde vrucht.' },
                { id: 'Mies', description: 'De poes.' },
            ])
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body[0].id, 'Aap')
                assert.strictEqual(res.body[1].id, 'Noot')
                assert.strictEqual(res.body[2].id, 'Mies')
            })
            .end(done)
    })
})

describe('Implementation POST /leesplank/dropThisCollection', function () {
    it('should return the POSTed body', function (done) {
        request(app)
            .post('/leesplank/dropThisCollection')
            .set('Content-type', 'application/json')
            .send([{ id: 'Aap', description: 'Een dier met een staart.' }])
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body[0].id, 'Aap')
            })
            .end(done)
    })
})

describe('Implementation: flush command ', function () {
    it('Flush command should return a 200, create checkpoint and remove oplog ', function (done) {
        request(app)
            .post('/leesplank')
            .set('Content-type', 'application/json')
            .send({ command: 'flush', collection: 'dropThisCollection', options: { archive: false } })
            .expect(200)
            .expect(function (res) {
                assert.ok(!fs.existsSync('./archive/oplog/dropThisCollection'))
                assert.ok(fs.existsSync('./leesplank/checkpoint/dropThisCollection/latest.ndjson'))
                assert.ok(!fs.existsSync('./leesplank/oplog/dropThisCollection/latest.ndjson'))
            })
            .end(done)
    })
})
describe('Implementation: flush command ', function () {
    it('Flush command should return a 400 when archive options is missing, ', function (done) {
        request(app).post('/leesplank').set('Content-type', 'application/json').send({ command: 'flush', collection: 'dropThisCollection' }).expect(400).end(done)
    })
})
describe('Implementation POST /leesplank/dropThisCollection', function () {
    it('should return the POSTed body', function (done) {
        request(app)
            .post('/leesplank/dropThisCollection')
            .set('Content-type', 'application/json')
            .send([{ id: 'Noot', description: 'Een harde vrucht.' }])
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body[0].id, 'Noot')
            })
            .end(done)
    })
})

describe('Implementation: drop command ', function () {
    it('Drop command should return a 200 ', function (done) {
        request(app)
            .post('/leesplank')
            .set('Content-type', 'application/json')
            .send({ command: 'drop', collection: 'dropThisCollection' })
            .expect(200)
            .expect(function (res) {
                assert.ok(!fs.existsSync('./leesplank/checkpoint/dropThisCollection/latest.ndjson'))
                assert.ok(!fs.existsSync('./leesplank/oplog/dropThisCollection/1.json'))
                assert.ok(!fs.existsSync('./leesplank/oplog/dropThisCollection/2.json'))
            })
            .end(done)
    })
})

server.close()
