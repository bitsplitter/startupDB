/*
This test suite specifically tests for behaviour.
It doesn't care about the implementation details.
Although its build using a unit test library, the tests are not independent, in fact, the build on each other deliberately.
*/
for (var key in Object.keys(require.cache)) {
    delete require.cache[key]
}
const fs = require('fs')
const request = require('supertest')
const { assert } = require('chai')
const should = require('chai').should()
const express = require('express')
const app = express()
const startupDB = require('../dist/server')

try {
    fs.rmSync('./archive', { recursive: true })
} catch (e) {}
try {
    fs.rmSync('./leesplank', { recursive: true })
} catch (e) {}
app.use(express.json({ inflate: true, limit: '1mb' }))

describe('Behaviour: setting up a non-function hook ', function () {
    it('should fail validation', function () {
        should.throw(() => {
            app.use('/leesplank', startupDB.beforePost(3.14))
        })
    })
})

app.use(
    '/leesplank',
    startupDB.beforePut(async function (req, res, next, collection) {
        if (req.query['letPutHookFail']) throw Error
        return { statusCode: 0 }
    })
)
app.use(
    '/leesplank',
    startupDB.beforePatch(async function (req, res, next, collection) {
        return { statusCode: 0 }
    })
)
app.use(
    '/leesplank',
    startupDB.beforeDelete(async function (req, res, next, collection) {
        return { statusCode: 0 }
    })
)
app.use(
    '/leesplank',
    startupDB.beforeAll(async function (req, res, next, collection) {
        if (req.query['letAllHookFail']) throw Error
        return { statusCode: 0 }
    })
)

app.use(
    '/leesplank',
    startupDB.beforeGet(async function (req, res, next, collection) {
        if (req.query['letGetHookFail']) return { statusCode: req.query['letGetHookFail'] }
        return { statusCode: 0 }
    })
)
app.use(
    '/leesplank',
    startupDB.afterGet(async function (req, response) {
        return response
    })
)
app.use(
    '/leesplank',
    startupDB.afterPost(async function (req, response) {
        return response
    })
)
app.use(
    '/leesplank',
    startupDB.afterPatch(async function (req, response) {
        return response
    })
)
app.use(
    '/leesplank',
    startupDB.afterPut(async function (req, response) {
        return response
    })
)
app.use(
    '/leesplank',
    startupDB.afterDelete(async function (req, response) {
        return response
    })
)
app.use(
    '/leesplank',
    startupDB.afterAll(async function (req, response) {
        return response
    })
)

app.use(
    '/leesplank',
    startupDB.db({
        testing: true,
        dataFiles: './leesplank',
        addTimeStamps: function (operation, object, oldObject) {
            if (operation == 'created') object.__created = new Date().getTime()
            if (operation == 'modified') {
                object.__modified = new Date().getTime()
                if (oldObject) object.__created = oldObject.__created
            }
        },
        validator: function (collection, documents) {
            const serializePayload = JSON.stringify(documents)
            if (serializePayload.includes('Throw me an error')) throw 'Error'
            if (collection == 'noTimeStamps')
                documents.forEach((d) => {
                    delete d.__created
                    delete d.__modified
                })
            return serializePayload.includes('reject this document')
        },
        streamObjects: true,
        serveRawCheckpoint: true,
    })
)

// app.use("/leesplank", startupDB.db())
const server = app.listen(3456)

describe('Behaviour: GET /leesplank/origineel', function () {
    it('should return a 200 even when the collection is not initialized.', function (done) {
        request(app).get('/leesplank/origineel').expect(200).end(done)
    })
})

describe('Behaviour: POST /leesplank/origineel', function () {
    it('should return the POSTed body', function (done) {
        request(app)
            .post('/leesplank/origineel')
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
                assert.notEqual(res.body[0].__created, undefined)
                assert.strictEqual(res.body[0].__modified, undefined)
            })
            .end(done)
    })
})

describe('Behaviour after POSTn', function () {
    it('totalBytesInMemory should match stringified length of POSTed items (including metadata like timestamps)', function (done) {
        request(app)
            .post('/leesplank')
            .set('Content-type', 'application/json')
            .send({ command: 'inspect' })
            .expect(200)
            .expect(function (res) {
                assert.strictEqual(res.body.usedBytesInMemory, 142)
            })
            .end(done)
    })
})

describe('Behaviour after POSTn', function () {
    it('Should catch an error in a failing hook during a dba command', function (done) {
        request(app).post('/leesplank?letAllHookFail=true').set('Content-type', 'application/json').send({ command: 'inspect' }).expect(500).end(done)
    })
})

describe('Behaviour during DBA commands', function () {
    it('Should return result from prehook if one acts on a dba command', function (done) {
        request(app)
            .get('/leesplank?letGetHookFail=209')
            .set('Content-type', 'application/json')
            .expect(209)
            .expect(function (res) {
                assert.strictEqual(res.body.collections, undefined)
            })
            .end(done)
    })
})

describe('Behaviour: POST /leesplank/origineel', function () {
    it('should reject non-json payloads', function (done) {
        request(app).post('/leesplank/origineel').set('Content-type', 'application/text').send('test').expect(400).end(done)
    })
})

describe('Behaviour: POST /leesplank/reject', function () {
    it('should return a 400 when posting a non-validating document', function (done) {
        request(app)
            .post('/leesplank/reject')
            .set('Content-type', 'application/json')
            .send([{ id: 'reject', reject: 'reject this document' }])
            .expect(400)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .end(done)
    })
})

describe('Behaviour: POST /leesplank/reject', function () {
    it('should return a 500 when posting a non-validating document that throws the validation function', function (done) {
        request(app)
            .post('/leesplank/reject')
            .set('Content-type', 'application/json')
            .send([{ id: 'reject', reject: 'Throw me an error' }])
            .expect(500)
            .end(done)
    })
})

describe('Behaviour: ??? /leesplank/origineel', function () {
    it('should pass through for non-implemented methods', function (done) {
        request(app).trace('/leesplank/origineel').set('Content-type', 'application/json').expect(404).end(done)
    })
})

describe('Behaviour: PUT /leesplank/origineel', function () {
    it('should return a 400 when putting a non-validating document', function (done) {
        request(app)
            .put('/leesplank/reject')
            .set('Content-type', 'application/json')
            .send([{ id: 'aap', reject: 'reject this document' }])
            .expect(400)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .end(done)
    })
})
describe('Behaviour: GET /leesplank/origineel', function () {
    it('should return a 404 when trying to retrieve an noexisting document.', function (done) {
        request(app).get('/leesplank/origineel?id=Wim').expect(404).end(done)
    })
})
describe('Behaviour: GET /leesplank/origineel', function () {
    it('should return the error from a hook when it fails.', function (done) {
        request(app).get('/leesplank/origineel?letGetHookFail=409').expect(409).end(done)
    })
})
describe('Behaviour: GET /leesplank/origineel', function () {
    it('should return the error from a hook when i fails.', function (done) {
        request(app).get('/leesplank/origineel?letGetHookFail=404').expect(404).end(done)
    })
})

describe('Behaviour: POST /leesplank/origineel', function () {
    it('should return the POSTed body', function (done) {
        request(app)
            .post('/leesplank/origineel')
            .set('Content-type', 'application/json')
            .send([
                { id: 'Wim', description: 'Een broer.' },
                { id: 'Zus', description: 'Een baby.' },
                { id: 'Jet', description: 'Een zus.' },
            ])
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body[0].id, 'Wim')
                assert.strictEqual(res.body[1].id, 'Zus')
                assert.strictEqual(res.body[2].id, 'Jet')
                assert.notEqual(res.body[0].__created, undefined)
                assert.strictEqual(res.body[0].__modified, undefined)
            })
            .end(done)
    })
})

describe('Behaviour: PUT /leesplank/origineel', function () {
    it('should return the PUT body', function (done) {
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
                assert.strictEqual(res.body[0].id, 'Wim')
                assert.strictEqual(res.body[1].id, 'Zus')
                assert.strictEqual(res.body[2].id, 'Jet')
                assert.notEqual(res.body[0].__created, undefined)
                assert.notEqual(res.body[0].__modified, undefined)
            })
            .end(done)
    })
})

describe('Behaviour: GET /leesplank/origineel', function () {
    it('should return the POSTed bodys from the previous tests.', function (done) {
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
            })
            .end(done)
    })
})

describe('Behaviour: GET /leesplank/origineel (filtered)', function () {
    it('should return objects matching the filter.', function (done) {
        request(app)
            .get('/leesplank/origineel?filter=id in ("Mies", "Jet")')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body[0].id, 'Mies')
                assert.strictEqual(res.body[1].id, 'Jet')
            })
            .end(done)
    })
})

describe('Behaviour: GET /leesplank/origineel (filtered)', function () {
    it('should return no objects when none match the filter.', function (done) {
        request(app)
            .get('/leesplank/origineel?filter=id in ("Schapen")')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body.length, 0)
            })
            .end(done)
    })
})
describe('Behaviour: GET /leesplank/origineel (filtered)', function () {
    it('should return objects matching the filter.', function (done) {
        request(app)
            .get('/leesplank/origineel?filter=id~=".*i.*"')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body[0].id, 'Mies')
                assert.strictEqual(res.body[1].id, 'Wim')
            })
            .end(done)
    })
})

describe('Behaviour: GET /leesplank/origineel (filtered)', function () {
    it('should return an error on an malformed filter.', function (done) {
        request(app).get('/leesplank/origineel?filter=id~=.*i.*"').expect(400).expect('Content-Type', 'application/json; charset=utf-8').end(done)
    })
})

describe('Behaviour: POST /leesplank/origineel', function () {
    it('should return a 409 on a duplicate key', function (done) {
        request(app)
            .post('/leesplank/origineel')
            .set('Content-type', 'application/json')
            .send([{ id: 'Aap', description: 'Een dier met een staart.' }])
            .expect(409)
            .end(done)
    })
})

describe('Behaviour: POST /leesplank/origineel', function () {
    it('should return a 409 on a duplicate key', function (done) {
        request(app)
            .post('/leesplank/origineel')
            .set('Content-type', 'application/json')
            .send([
                { id: 'Aap', description: 'Een dier met een staart.' },
                { id: 'Does', description: 'Een hond.' },
            ])
            .expect(409)
            .end(done)
    })
})
describe('Behaviour: GET /leesplank/origineel', function () {
    it('should return a 404 when trying to retrieve an noexisting document.', function (done) {
        request(app).get('/leesplank/origineel?id=Does').expect(404).end(done)
    })
})

describe('Behaviour: GET /leesplank/origineel?limit=2', function () {
    it('should return an array with operations when requesting a proper oplogId', function (done) {
        request(app)
            .get('/leesplank/origineel?limit=2')
            .set('Content-type', 'application/json')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.ok(Array.isArray(res.body))
                assert.strictEqual(res.body[0].id, 'Aap')
                assert.strictEqual(res.body[1].id, 'Noot')
                assert.strictEqual(res.body.length, 2)
            })
            .end(done)
    })
})

describe('Behaviour: GET /leesplank/origineel?limit=2&offset=1', function () {
    it('should return an array with operations when requesting a proper oplogId', function (done) {
        request(app)
            .get('/leesplank/origineel?limit=2&offset=1')
            .set('Content-type', 'application/json')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.ok(Array.isArray(res.body))
                assert.strictEqual(res.body[0].id, 'Noot')
                assert.strictEqual(res.body[1].id, 'Mies')
                assert.strictEqual(res.body.length, 2)
            })
            .end(done)
    })
})

describe('Behaviour: GET /leesplank/origineel?returnType=tally', function () {
    it('should return the number of documents', function (done) {
        request(app)
            .get('/leesplank/origineel?returnType=tally')
            .set('Content-type', 'application/json')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body.tally, 6)
            })
            .end(done)
    })
})
describe('Behaviour: DELETE one document from /leesplank/origineel', function () {
    it('should return the original document', function (done) {
        request(app)
            .delete('/leesplank/origineel?id=Aap')
            .set('Content-type', 'application/json')
            .expect(200)
            .expect(function (res) {
                assert.strictEqual(res.body[0].id, 'Aap')
                assert.strictEqual(res.body[0].description, 'Een dier met een staart.')
            })
            .end(done)
    })
})

describe('Behaviour: DELETE nonexisting document from /leesplank/origineel', function () {
    it('should return 404', function (done) {
        request(app).delete('/leesplank/origineel?id=Aap').set('Content-type', 'application/json').expect(404).end(done)
    })
})

describe('Behaviour: DELETE without parameters from /leesplank/origineel', function () {
    it('should return 400', function (done) {
        request(app).delete('/leesplank/origineel').set('Content-type', 'application/json').expect(400).end(done)
    })
})

describe('Behaviour: DELETE multiple documents from /leesplank/origineel', function () {
    it('should return the original documents', function (done) {
        request(app)
            .delete('/leesplank/origineel?filter=id in ("Wim","Zus")')
            .set('Content-type', 'application/json')
            .expect(200)
            .expect(function (res) {
                assert.strictEqual(res.body[0].id, 'Wim')
                assert.strictEqual(res.body[0].description, 'Een broer.')
                assert.strictEqual(res.body[1].id, 'Zus')
                assert.strictEqual(res.body[1].description, 'Een baby.')
            })
            .end(done)
    })
})

describe('Behaviour: PUT /leesplank/origineel', function () {
    it('should return the PUT body', function (done) {
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
                assert.strictEqual(res.body[0].id, 'Wim')
                assert.strictEqual(res.body[1].id, 'Zus')
                assert.strictEqual(res.body[2].id, 'Jet')
                assert.notEqual(res.body[0].__created, undefined)
                assert.strictEqual(res.body[0].__modified, undefined)
                assert.notEqual(res.body[2].__created, undefined)
                assert.notEqual(res.body[2].__modified, undefined)
            })
            .end(done)
    })
})

describe('Behaviour: PATCH /leesplank/origineel', function () {
    it('should return the PATCH body using the jsonpatch schema', function (done) {
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
                assert.strictEqual(res.body[0].id, 'Zus')
                assert.strictEqual(res.body[0].patch[0].value, 'Baby zusje.')
                assert.strictEqual(res.body[0].patch[1].value, 'Sister')
                assert.strictEqual(res.body[1].id, 'Jet')
            })
            .end(done)
    })
})

describe('Behaviour: PATCH /leesplank/origineel', function () {
    it('should return the PATCH body using any schema', function (done) {
        request(app)
            .patch('/leesplank/origineel')
            .set('Content-type', 'application/json')
            .send([
                {
                    id: 'Jet',
                    description: 'Grote zus.',
                    english: 'Big sister',
                },
            ])
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body[0].id, 'Jet')
                assert.strictEqual(res.body[0].description, 'Grote zus.')
                assert.strictEqual(res.body[0].english, 'Big sister')
            })
            .end(done)
    })
})

describe('Behaviour: PATCH /leesplank/origineel', function () {
    it('should return the  body a non-existing PATCHed document', function (done) {
        request(app)
            .patch('/leesplank/origineel')
            .set('Content-type', 'application/json')
            .send([
                {
                    id: 'Teun',
                    description: 'Man met baard.',
                    english: 'Man with beard',
                },
            ])
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body[0].id, 'Teun')
                assert.strictEqual(res.body[0].description, 'Man met baard.')
                assert.strictEqual(res.body[0].english, 'Man with beard')
            })
            .end(done)
    })
})
describe('Behaviour: DELETE one document by ID from /leesplank/origineel', function () {
    it('should return the original document', function (done) {
        request(app)
            .delete('/leesplank/origineel?id=Teun')
            .expect(200)
            .expect(function (res) {
                assert.strictEqual(res.body[0].id, 'Teun')
            })
            .end(done)
    })
})
describe('Behaviour: PATCH /leesplank/origineel', function () {
    it('should return an error on a non-validation PATCH', function (done) {
        request(app)
            .patch('/leesplank/origineel')
            .set('Content-type', 'application/json')
            .send([
                {
                    id: 'Zus',
                    patch: [{ op: 'replace', path: '/description', value: 'reject this document' }],
                },
            ])
            .expect(400)
            .end(done)
    })
})

describe('Behaviour: PATCH /leesplank/origineel', function () {
    it('should return OK when trying to patch a non-existing object', function (done) {
        request(app)
            .patch('/leesplank/origineel')
            .set('Content-type', 'application/json')
            .send([
                {
                    id: 'NotThere',
                    patch: [
                        { op: 'replace', path: '/description', value: 'Baby zusje.' },
                        { op: 'add', path: '/english', value: 'Sister' },
                    ],
                },
                { id: 'Jet', patch: [] },
            ])
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .end(done)
    })
})

describe('Behaviour: GET /leesplank/origineel', function () {
    it('should return the PATCHed object from the previous test', function (done) {
        request(app)
            .get('/leesplank/origineel?id=Zus')
            .set('Content-type', 'application/json')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body.id, 'Zus')
                assert.strictEqual(res.body.description, 'Baby zusje.')
                assert.strictEqual(res.body.english, 'Sister')
            })
            .end(done)
    })
})

describe('Behaviour: PATCH /leesplank/origineel', function () {
    it('should return 400 on a malformed PATCH request', function (done) {
        request(app)
            .patch('/leesplank/origineel?id=Zus')
            .set('Content-type', 'application/json')
            .send([
                {
                    id: 'Zus',
                    patch: [
                        { 'not avalidop': 'replace', path: '/description', value: 'Baby zusje.' },
                        { op: 'add', path: '/english', value: 'Sister' },
                    ],
                },
            ])
            .expect(400)
            .end(done)
    })
})

describe('Behaviour: PATCH /leesplank/origineel', function () {
    it('should ignore a PATCH request that does not change anythin', function (done) {
        request(app)
            .patch('/leesplank/origineel?id=Zus')
            .set('Content-type', 'application/json')
            .send([
                {
                    id: 'Zus',
                    patch: [{ op: 'remove', path: '/nonexistingproperty' }],
                },
            ])
            .expect(200)
            .end(done)
    })
})

describe('Behaviour: GET /leesplank/origineel?returnType=array', function () {
    it('should return an array', function (done) {
        request(app)
            .get('/leesplank/origineel?returnType=array')
            .set('Content-type', 'application/json')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.ok(Array.isArray(res.body))
            })
            .end(done)
    })
})

describe('Behaviour: GET /leesplank/origineel?returnType=object', function () {
    it('should not return an array', function (done) {
        request(app)
            .get('/leesplank/origineel?returnType=object')
            .set('Content-type', 'application/json')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.ok(!Array.isArray(res.body))
            })
            .end(done)
    })
})

describe('Behaviour: GET /leesplank/origineel?returnType=checkpoint', function () {
    it('should return a checkpoint object', function (done) {
        request(app)
            .get('/leesplank/origineel?returnType=checkpoint')
            .set('Content-type', 'application/json')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.ok('checkPoint' in res.body)
                assert.ok('nextOpLogId' in res.body)
            })
            .end(done)
    })
})

describe('Behaviour: GET /leesplank/origineel?returnType=checkpoint&id=Noot', function () {
    it('should return a checkpoint object', function (done) {
        request(app)
            .get('/leesplank/origineel?returnType=checkpoint&id=Noot')
            .set('Content-type', 'application/json')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body.data.Noot.id, 'Noot')
            })
            .end(done)
    })
})

describe('Behaviour: GET /leesplank/origineel?returnType=checkpoint&filter=id=="Noot"', function () {
    it('should return a checkpoint object', function (done) {
        request(app)
            .get('/leesplank/origineel?returnType=checkpoint&filter=id=="Noot"')
            .set('Content-type', 'application/json')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body.data.Noot.id, 'Noot')
            })
            .end(done)
    })
})

describe('Behaviour: GET /leesplank/origineel?fromOpLogId=100', function () {
    it('should return a 400 when requesting an nonexisting oplogId', function (done) {
        request(app).get('/leesplank/origineel?fromOpLogId=aap').set('Content-type', 'application/json').expect(400).end(done)
    })
})

describe('Behaviour: GET /leesplank/origineel?fromOpLogId=1', function () {
    it('should return an array with operations when requesting a proper oplogId', function (done) {
        request(app)
            .get('/leesplank/origineel?fromOpLogId=1')
            .set('Content-type', 'application/json')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.ok(Array.isArray(res.body))
                assert.strictEqual(res.body[0].operation, 'create')
                assert.strictEqual(res.body[0].collection, 'origineel')
                assert.strictEqual(res.body.length, 11)
            })
            .end(done)
    })
})

describe('Behaviour empty command', function () {
    it('executing an empty command should return a 400', function (done) {
        request(app).post('/leesplank').set('Content-type', 'application/json').send({}).expect(400).end(done)
    })
})

describe('Behaviour GET command', function () {
    it('executing a GET command should return a list of collections', function (done) {
        request(app)
            .get('/leesplank')
            .expect(200)
            .expect({
                collections: [
                    { name: 'origineel', inCache: true, count: 6, checkPoint: 0, lastOplogId: 0 },
                    { name: 'reject', inCache: true, count: 0, checkPoint: 0, lastOplogId: 0 },
                ],
            })
            .end(done)
    })
})

describe('Behaviour unknown POST command', function () {
    it('executing an unknown POST command should return a 400', function (done) {
        request(app).post('/leesplank').set('Content-type', 'application/json').send({ command: 'knurft' }).expect(400).end(done)
    })
})

describe('Behaviour flush command without collection', function () {
    it('executing a flush command without a collection should return a 400', function (done) {
        request(app).post('/leesplank').set('Content-type', 'application/json').send({ command: 'flush' }).expect(400).end(done)
    })
})

describe('Behaviour flush command without prior checkpoint', function () {
    it('executing a flush command without a prior checkpoint should return a 200', function (done) {
        request(app).post('/leesplank').set('Content-type', 'application/json').send({ command: 'flush', collection: 'origineel' }).expect(200).end(done)
    })
})

describe('Behaviour garbageCollector command', function () {
    it('should return a list of deleted collections', function (done) {
        request(app)
            .post('/leesplank')
            .set('Content-type', 'application/json')
            .send({ command: 'garbageCollector' })
            .expect(200)
            .expect(function (res) {
                assert.ok('deletedCollections' in res.body)
            })
            .end(done)
    })
})

describe('Behaviour: POST /leesplank/silent?returnType=tally', function () {
    it('should return number of objects in payload', function (done) {
        request(app)
            .post('/leesplank/silent?returnType=tally')
            .set('Content-type', 'application/json')
            .send([
                { id: 'Wim', description: 'Een broer.' },
                { id: 'Zus', description: 'Een baby.' },
                { id: 'Jet', description: 'Een zus.' },
            ])
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body.tally, 3)
            })
            .end(done)
    })
})
describe('Behaviour: PUT /leesplank/silent?returnType=tally', function () {
    it('should return number of objects in payload', function (done) {
        request(app)
            .put('/leesplank/silent?returnType=tally')
            .set('Content-type', 'application/json')
            .send([
                { id: 'Wim', description: 'Een broer!' },
                { id: 'Zus', description: 'Een baby!' },
                { id: 'Jet', description: 'Een zus!' },
            ])
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body.tally, 3)
            })
            .end(done)
    })
})
describe('Behaviour: DELETE /leesplank/silent?returnType=tally', function () {
    it('should return number of deleted objects', function (done) {
        request(app)
            .delete('/leesplank/silent?returnType=tally&filter=id in ("Wim","Zus")')
            .set('Content-type', 'application/json')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body.tally, 2)
            })
            .end(done)
    })
})

describe('Behaviour: PATCH /leesplank/silent', function () {
    it('should return the number of PATCHed objects', function (done) {
        request(app)
            .patch('/leesplank/silent?returnType=tally')
            .set('Content-type', 'application/json')
            .send([
                {
                    id: 'Jet',
                    patch: [
                        { op: 'replace', path: '/description', value: 'Rerteketet.' },
                        { op: 'add', path: '/english', value: 'Sister' },
                    ],
                },
            ])
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body.tally, 1)
            })
            .end(done)
    })
})
describe('Implementation POST /leesplank', function () {
    it('executing a create command without a collection should return a 400', function (done) {
        request(app)
            .post('/leesplank')
            .set('Content-type', 'application/json')
            .send({ command: 'create', options: { storageType: 'array' } })
            .expect(400)
            .end(done)
    })
})

describe('Implementation POST /leesplank', function () {
    it("executing a create command with storageType='array' creates a collection without id's", function (done) {
        request(app)
            .post('/leesplank')
            .set('Content-type', 'application/json')
            .send({ command: 'create', collection: 'array2', options: { storageType: 'array' } })
            .expect(200)
            .end(done)
    })
})

describe('Implementation POST /leesplank', function () {
    it("executing an ensureCollection command with storageType='array' creates a collection without id's", function (done) {
        request(app)
            .post('/leesplank')
            .set('Content-type', 'application/json')
            .send({ command: 'ensureCollection', collection: 'array', options: { storageType: 'array' } })
            .expect(200)
            .end(done)
    })
})

describe('Implementation POST /leesplank', function () {
    it('executing an ensureCollection command with without a collection returns an error', function (done) {
        request(app)
            .post('/leesplank')
            .set('Content-type', 'application/json')
            .send({ command: 'ensureCollection', options: { storageType: 'array' } })
            .expect(400)
            .end(done)
    })
})

describe('Implementation POST /leesplank', function () {
    it('executing an DROP command with without a collection returns an error', function (done) {
        request(app).post('/leesplank').set('Content-type', 'application/json').send({ command: 'drop' }).expect(400).end(done)
    })
})

describe('Implementation POST /leesplank', function () {
    it('executing a create command on an existing collection should return a 409', function (done) {
        request(app)
            .post('/leesplank')
            .set('Content-type', 'application/json')
            .send({ command: 'create', collection: 'array', options: { storageType: 'array' } })
            .expect(409)
            .end(done)
    })
})

describe('Behaviour: POST /leesplank/array?returnType=tally', function () {
    it('should return numbers of objects in payload', function (done) {
        request(app)
            .post('/leesplank/array?returnType=tally')
            .set('Content-type', 'application/json')
            .send([{ description: 'Een dier met een staart.' }, { description: 'Een harde vrucht.' }, { description: 'De poes.' }])
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body.tally, 3)
            })
            .end(done)
    })
})
describe('Behaviour: POST /leesplank/array?returnType=tally', function () {
    it('should return numbers of objects in payload', function (done) {
        request(app)
            .post('/leesplank/array?returnType=tally')
            .set('Content-type', 'application/json')
            .send([
                { id: 'Wim', description: 'Een broer.' },
                { id: 'Zus', description: 'Een baby.' },
                { id: 'Jet', description: 'Een zus.' },
            ])
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body.tally, 3)
            })
            .end(done)
    })
})
describe('Behaviour: POST /leesplank/array?returnType=tally', function () {
    it('should return numbers of objects in payload, ignoring duplicates', function (done) {
        request(app)
            .post('/leesplank/array?returnType=tally')
            .set('Content-type', 'application/json')
            .send([
                { id: 'Wim', description: 'Een broer.' },
                { id: 'Zus', description: 'Een baby.' },
                { id: 'Jet', description: 'Een zus.' },
            ])
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body.tally, 3)
            })
            .end(done)
    })
})
describe('Behaviour: GET /leesplank/array', function () {
    it('should return objects in array', function (done) {
        request(app)
            .get('/leesplank/array')
            .set('Content-type', 'application/json')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body.length, 9)
                assert.strictEqual(res.body[8].id, 'Jet')
            })
            .end(done)
    })
})
describe('Behaviour: GET /leesplank/array?returnType=checkpoint', function () {
    it('should return a checkpoint object with storageType array', function (done) {
        request(app)
            .get('/leesplank/array?returnType=checkpoint')
            .set('Content-type', 'application/json')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.ok('checkPoint' in res.body)
                assert.ok('nextOpLogId' in res.body)
                assert.strictEqual(res.body.options.storageType, 'array')
            })
            .end(done)
    })
})
describe('Behaviour: HEAD /leesplank/array?fromOpLogId=1', function () {
    it('should return 200 even on a non-existing collection', function (done) {
        request(app).head('/leesplank/notthere?fromOpLogId=1').set('Content-type', 'application/json').expect(200).end(done)
    })
})

describe('Behaviour: HEAD /leesplank/array?fromOpLogId=1', function () {
    it('should return predictable x-headers', function (done) {
        request(app)
            .head('/leesplank/array?fromOpLogId=1')
            .set('Content-type', 'application/json')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(JSON.stringify(res.body), '{}')
                assert.ok(res.headers['x-last-checkpoint-time'] > 0)
            })
            .end(done)
    })
})
describe('Behaviour: GET /leesplank/array?fromOpLogId=1', function () {
    it('should return an array with operations when requesting a proper oplogId', function (done) {
        request(app)
            .get('/leesplank/array?fromOpLogId=1')
            .set('Content-type', 'application/json')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.ok(Array.isArray(res.body))
            })
            .end(done)
    })
})
describe('Behaviour POST /leesplank', function () {
    it("executing a flush command with storageType='array' creates a checkpoint", function (done) {
        request(app).post('/leesplank').set('Content-type', 'application/json').send({ command: 'flush', collection: 'array' }).expect(200).end(done)
    })
})
describe('Behaviour: purgeOplog', function () {
    it("executing a purgeOplog command with storageType='array' reloads the checkpoint", function (done) {
        request(app).post('/leesplank').set('Content-type', 'application/json').send({ command: 'purgeOplog', collection: 'array' }).expect(200).end(done)
    })
})

describe('Behaviour: purgeOplog', function () {
    it('executing a purgeOplog command without a collection should return a 400', function (done) {
        request(app).post('/leesplank').set('Content-type', 'application/json').send({ command: 'purgeOplog' }).expect(400).end(done)
    })
})

describe('Behaviour: GET /leesplank/array', function () {
    it('should return objects in array', function (done) {
        request(app)
            .get('/leesplank/array')
            .set('Content-type', 'application/json')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body.length, 9)
                assert.strictEqual(res.body[8].id, 'Jet')
            })
            .end(done)
    })
})
describe('Behaviour: DELETE /leesplank/array', function () {
    it('should return a 409', function (done) {
        request(app).delete('/leesplank/array?id=409').set('Content-type', 'application/json').expect(409).expect('Content-Type', 'application/json; charset=utf-8').end(done)
    })
})
describe('Behaviour: UPDATE /leesplank/array', function () {
    it('should return a 409', function (done) {
        request(app)
            .put('/leesplank/array')
            .set('Content-type', 'application/json')
            .send({ id: 'Aap', description: 'Een dier met een staart.' })
            .expect(409)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .end(done)
    })
})
describe('Behaviour: PATCH /leesplank/array', function () {
    it('should return an error on array collections', function (done) {
        request(app)
            .patch('/leesplank/array')
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
            .expect(409)
            .end(done)
    })
})
describe('Behaviour: POST /leesplank/genids', function () {
    it('should return a generated ID', function (done) {
        request(app)
            .post('/leesplank/genids')
            .set('Content-type', 'application/json')
            .send([{ description: 'New ID.' }])
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body[0].description, 'New ID.')
                assert.isDefined(res.body[0].id)
            })
            .end(done)
    })
})
describe('Behaviour: GET /leesplank/notthere', function () {
    it('should return a 200 when trying to retrieve from noexisting collection.', function (done) {
        request(app).get('/leesplank/notthere').expect(200).end(done)
    })
})

describe('Behaviour after POST', function () {
    it('leastRecentlyUsed collection should be /Users/jeroen/startupDB/leesplank/silent)', function (done) {
        request(app)
            .post('/leesplank')
            .set('Content-type', 'application/json')
            .send({ command: 'inspect' })
            .expect(200)
            .expect(function (res) {
                assert.include(res.body.leastRecentlyUsed.collection, '/startupDB/leesplank/silent')
                assert.equal(res.body.nrCollectionsInCache, 5)
            })
            .end(done)
    })
})
describe('Behaviour after POSTing a complex object', function () {
    it('shour return the original object)', function (done) {
        request(app)
            .post('/leesplank/complex')
            .set('Content-type', 'application/json')
            .send({ id: 'complexObject', nrTiles: 17, tiles: { aap: 'Een dier met een staart.', noot: 'Een harde vrucht.' } })
            .expect(200)
            .expect(function (res) {
                assert.strictEqual(res.body[0].id, 'complexObject')
            })
            .end(done)
    })
})

describe('Behaviour: GET /leesplank/complex (filtered by nonexisting property)', function () {
    it('should return nothing.', function (done) {
        request(app)
            .get('/leesplank/complex?filter=tiles.mies=="De poes"')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body.length, 0)
            })
            .end(done)
    })
})

describe('Behaviour: GET /leesplank/complex (filtered by property)', function () {
    it('should return nothing.', function (done) {
        request(app)
            .get('/leesplank/complex?filter=tiles.aap=="Een dier met een staart."')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body[0].id, 'complexObject')
            })
            .end(done)
    })
})
describe('Behaviour: PUT /leesplank/origineel', function () {
    it('should return the error from a hook when the hook throws an error.', function (done) {
        request(app).put('/leesplank/origineel?letPutHookFail=true').send({}).expect(500).end(done)
    })
})

describe('Behaviour: POST /leesplank/noTimeStamps', function () {
    it('should return documents without timestamps, asserts that validator runs after addTimeStamp', function (done) {
        request(app)
            .post('/leesplank/noTimeStamps')
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
                assert.strictEqual(res.body[0].__created, undefined)
                assert.strictEqual(res.body[0].__modified, undefined)
            })
            .end(done)
    })
})
describe('Behaviour: Delete /leesplank/noTimeStamps?filter', function () {
    it('should documents that satisfy the filter expression', function (done) {
        request(app)
            .delete('/leesplank/noTimeStamps?filter=id=="Aap" or id=="Noot"')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body[0].id, 'Aap')
                assert.strictEqual(res.body[1].id, 'Noot')
            })
            .end(done)
    })
})
describe('Behaviour: Get /leesplank/noTimeStamps after delete', function () {
    it('should return the remaining document', function (done) {
        request(app)
            .get('/leesplank/noTimeStamps')
            .set('Content-type', 'application/json')
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .expect(function (res) {
                assert.strictEqual(res.body[0].id, 'Mies')
                assert.strictEqual(res.body.length, 1)
            })
            .end(done)
    })
})
describe('Behaviour: clearCache', function () {
    it('executing a clearCache command without a collection should return a 400', function (done) {
        request(app).post('/leesplank').set('Content-type', 'application/json').send({ command: 'clearCache' }).expect(400).end(done)
    })
})

describe('Behaviour: clearCache', function () {
    it('executing a clearCache command with a collection should return a 200', function (done) {
        request(app).post('/leesplank').set('Content-type', 'application/json').send({ command: 'clearCache', collection: 'origineel' }).expect(200).end(done)
    })
})

describe('Behaviour: POST /leesplank/big0', function () {
    it('store big datain a new collection', function (done) {
        request(app)
            .post('/leesplank/big0')
            .set('Content-type', 'application/json')
            .send([
                { id: 'big00', description: '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789' },
                { id: 'big01', description: '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789' },
                { id: 'big02', description: '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789' },
                { id: 'big03', description: '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789' },
                { id: 'big04', description: '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789' },
                { id: 'big05', description: '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789' },
                { id: 'big06', description: '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789' },
                { id: 'big07', description: '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789' },
                { id: 'big08', description: '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789' },
                { id: 'big09', description: '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789' },
            ])
            .expect(200)
            .expect('Content-Type', 'application/json; charset=utf-8')
            .end(done)
    })
})

describe('Behaviour: expect GC after adding a lot of data', function () {
    it('should remove some collections from cache', function (done) {
        request(app)
            .post('/leesplank')
            .set('Content-type', 'application/json')
            .send({ command: 'inspect' })
            .expect(200)
            .expect(function (res) {
                assert.strictEqual(res.body.usedBytesInMemory, 486)
                assert.strictEqual(res.body.nrCollectionsInCache, 1)
            })
            .end(done)
    })
})

describe('Behaviour: get /', function () {
    it('should return list of all collections', function (done) {
        request(app)
            .get('/leesplank')
            .set('Content-type', 'application/json')
            .expect(200)
            .expect(function (res) {
                assert.strictEqual(res.body.collections[0].name, 'origineel')
                assert.strictEqual(res.body.collections[1].name, 'big0')
                assert.strictEqual(res.body.collections[2].name, 'complex')
                assert.strictEqual(res.body.collections[3].name, 'genids')
                assert.strictEqual(res.body.collections[4].name, 'noTimeStamps')
                assert.strictEqual(res.body.collections[5].name, 'silent')
                assert.strictEqual(res.body.collections[6].name, 'array')
                assert.strictEqual(res.body.collections[7].name, 'array2')
                assert.strictEqual(res.body.collections.length, 8)
            })
            .end(done)
    })
})

server.close()
