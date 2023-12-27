/*
This test suite tests the implementation. 
It checks how data is stored to disk after performing test operations.
*/
for (var key in Object.keys(require.cache)) {
    delete require.cache[key]
}
const fs = require('fs')
const request = require('supertest')
const assert = require('assert')
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
                let operation = fs.readFileSync('./leesplank/oplog/origineel/1.json')
                operation = JSON.parse(operation)
                assert.strictEqual(operation.opLogId, 1)
                assert.strictEqual(operation.operation, 'create')
                assert.strictEqual(operation.collection, 'origineel')
                assert.strictEqual(operation.data[0].id, 'Aap')
                assert.strictEqual(operation.data[0].description, 'Een dier met een staart.')
                assert.strictEqual(operation.data[1].id, 'Noot')
                assert.strictEqual(operation.data[1].description, 'Een harde vrucht.')
                assert.strictEqual(operation.data[2].id, 'Mies')
                assert.strictEqual(operation.data[2].description, 'De poes.')
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
                let operation = fs.readFileSync('./leesplank/oplog/nieuw/1.json')
                operation = JSON.parse(operation)
                assert.strictEqual(operation.opLogId, 1)
                assert.strictEqual(operation.operation, 'create')
                assert.strictEqual(operation.collection, 'nieuw')
                assert.strictEqual(operation.data[0].id, 'Maan')
                assert.strictEqual(operation.data[0].description, 'Een hemellichaam.')
                assert.strictEqual(operation.data[1].id, 'Roos')
                assert.strictEqual(operation.data[1].description, 'Een bloem.')
                assert.strictEqual(operation.data[2].id, 'Vis')
                assert.strictEqual(operation.data[2].description, 'Een dier.')
            })
            .end(done)
    })
})

describe('Implementation: POST /leesplank (flush origineel )', function () {
    it('should create a checkpoint file when there are unflushed documents', function (done) {
        request(app)
            .post('/leesplank')
            .set('Content-type', 'application/json')
            .send({ command: 'flush', collection: 'origineel' })
            .expect(200)
            .expect(function (res) {
                assert.ok(fs.existsSync('./leesplank/checkpoint/origineel/latest.json'))
                assert.ok(fs.existsSync('./archive/oplog/origineel/1.json'))
            })
            .expect(function (res) {
                let operation = fs.readFileSync('./archive/oplog/origineel/1.json')
                operation = JSON.parse(operation)
                assert.strictEqual(operation.opLogId, 1)
                assert.strictEqual(operation.operation, 'create')
                assert.strictEqual(operation.collection, 'origineel')
                assert.strictEqual(operation.data[0].id, 'Aap')
                assert.strictEqual(operation.data[0].description, 'Een dier met een staart.')
                assert.strictEqual(operation.data[1].id, 'Noot')
                assert.strictEqual(operation.data[1].description, 'Een harde vrucht.')
                assert.strictEqual(operation.data[2].id, 'Mies')
                assert.strictEqual(operation.data[2].description, 'De poes.')
            })
            .end(done)
    })
})

describe('Implementation: POST /leesplank/ndjson)', function () {
    it('should should prepare a collection for the next test', function (done) {
        request(app)
            .post('/leesplank/ndjson')
            .set('Content-type', 'application/json')
            .send([{ id: 'type', description: 'Newline Delimited JSON.' }])
            .end(done)
    })
})
describe('Implementation: POST /leesplank (flush ndjson )', function () {
    it('should create an ndjson checkpoint file', function (done) {
        request(app)
            .post('/leesplank')
            .set('Content-type', 'application/json')
            .send({ command: 'flush', collection: 'ndjson', options: { contentType: 'ndjson', force: true } })
            .expect(200)
            .expect(function (res) {
                assert.ok(fs.existsSync('./leesplank/checkpoint/ndjson/latest.ndjson'))
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
            .send({ command: 'flush', collection: 'nieuw' })
            .expect(200)
            .expect(function (res) {
                assert.ok(fs.existsSync('./leesplank/checkpoint/nieuw/latest.json'))
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
                let operation = fs.readFileSync('./leesplank/oplog/origineel/2.json')
                operation = JSON.parse(operation)
                assert.strictEqual(operation.opLogId, 2)
                assert.strictEqual(operation.operation, 'create')
                assert.strictEqual(operation.collection, 'origineel')
                assert.strictEqual(operation.data[0].id, 'Wim')
                assert.strictEqual(operation.data[0].description, 'Een broer.')
                assert.strictEqual(operation.data[1].id, 'Zus')
                assert.strictEqual(operation.data[1].description, 'Een baby.')
                assert.strictEqual(operation.data[2].id, 'Jet')
                assert.strictEqual(operation.data[2].description, 'Een zus.')
            })
            .end(done)
    })
})

describe('Implementation: POST /leesplank', function () {
    it('executing a flush command with an existing checkpoint should create a new checkpoint file when there are unflushed documents', function (done) {
        request(app)
            .post('/leesplank')
            .set('Content-type', 'application/json')
            .send({ command: 'flush', collection: 'origineel' })
            .expect(function (res) {
                assert.ok(fs.existsSync('./leesplank/checkpoint/origineel/latest.json'))
            })
            .expect(function (res) {
                assert.ok(fs.existsSync('./leesplank/checkpoint/origineel/1.json'))
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
                let operation = fs.readFileSync('./leesplank/oplog/origineel/3.json')
                operation = JSON.parse(operation)
                assert.strictEqual(operation.opLogId, 3)
                assert.strictEqual(operation.operation, 'delete')
                assert.strictEqual(operation.collection, 'origineel')
                assert.strictEqual(operation.oldData[0].id, 'Wim')
                assert.strictEqual(operation.oldData[0].description, 'Een broer.')
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
                let operation = fs.readFileSync('./leesplank/oplog/origineel/4.json')
                operation = JSON.parse(operation)
                assert.strictEqual(operation.opLogId, 4)
                assert.strictEqual(operation.operation, 'update')
                assert.strictEqual(operation.collection, 'origineel')
                assert.strictEqual(operation.data[0].id, 'Wim')
                assert.strictEqual(operation.data[0].description, 'Een broer.')
                assert.strictEqual(operation.data[1].id, 'Zus')
                assert.strictEqual(operation.data[1].description, 'Een baby.')
                assert.strictEqual(operation.data[2].id, 'Jet')
                assert.strictEqual(operation.data[2].description, 'Een zus.')
                assert.strictEqual(operation.oldData[0].id, 'Zus')
                assert.strictEqual(operation.oldData[0].description, 'Een baby.')
                assert.strictEqual(operation.oldData[1].id, 'Jet')
                assert.strictEqual(operation.oldData[1].description, 'Een zus.')
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
                let operation = fs.readFileSync('./leesplank/oplog/origineel/5.json')
                operation = JSON.parse(operation)
                assert.strictEqual(operation.opLogId, 5)
                assert.strictEqual(operation.operation, 'patch')
                assert.strictEqual(operation.collection, 'origineel')
                assert.strictEqual(operation.data[0].id, 'Zus')
                assert.strictEqual(operation.data[0].patch[0].op, 'replace')
                assert.strictEqual(operation.data[0].patch[0].path, '/description')
                assert.strictEqual(operation.data[0].patch[0].value, 'Baby zusje.')
                assert.strictEqual(operation.data[0].patch[1].op, 'add')
                assert.strictEqual(operation.data[0].patch[1].path, '/english')
                assert.strictEqual(operation.data[0].patch[1].value, 'Sister')
                assert.strictEqual(operation.data[1].id, 'Jet')
                assert.strictEqual(operation.data[1].patch.length, 0)
                assert.strictEqual(operation.oldData[0].id, 'Zus')
                assert.strictEqual(operation.oldData[0].description, 'Een baby.')
                assert.strictEqual(operation.oldData[1].id, 'Jet')
                assert.strictEqual(operation.oldData[1].description, 'Een zus.')
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
    it('Flush command should return a 200, ', function (done) {
        request(app)
            .post('/leesplank')
            .set('Content-type', 'application/json')
            .send({ command: 'flush', collection: 'dropThisCollection' })
            .expect(200)
            .expect(function (res) {
                assert.ok(fs.existsSync('./leesplank/checkpoint/dropThisCollection/latest.json'))
                assert.ok(!fs.existsSync('./leesplank/oplog/dropThisCollection/1.json'))
                assert.ok(fs.existsSync('./archive/oplog/dropThisCollection/1.json'))
            })
            .end(done)
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
                assert.ok(!fs.existsSync('./leesplank/checkpoint/dropThisCollection/latest.json'))
                assert.ok(!fs.existsSync('./leesplank/oplog/dropThisCollection/1.json'))
                assert.ok(!fs.existsSync('./leesplank/oplog/dropThisCollection/2.json'))
            })
            .end(done)
    })
})

// TODO: Write implementation tests for using zipped and unzipped JSON arrays from external sources as checkpoint
server.close()
