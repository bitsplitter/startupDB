/*
A collection is only served when its oplog is intact: contiguous <id>.json files with valid JSON.
A corrupt oplog latches the collection as faulty until an operator repairs it and runs clearCache.
*/
const request = require('supertest')
const assert = require('assert')
const express = require('express')
const fs = require('fs')
const fsx = require('fs-extra')
const path = require('path')
const startupDB = require('../dist/server.js')

const root = path.join(__dirname, '..', 'tmp')
fs.mkdirSync(root, { recursive: true })
const dataDir = fs.mkdtempSync(path.join(root, 'integrity-'))

const app = express()
app.use(express.json())
app.use('/db', startupDB.db({ dataFiles: dataDir }))

const J = { 'Content-Type': 'application/json' }
const post = (collection, body) => request(app).post(`/db/${collection}`).set(J).send(body)
const get = (collection) => request(app).get(`/db/${collection}`)
const dbaCommand = (body) => request(app).post('/db').set(J).send(body)
const oplogFile = (collection, name) => path.join(dataDir, 'oplog', collection, name)
const listed = async (collection) => (await request(app).get('/db').expect(200)).body.collections.find((c) => c.name == collection)

// Silence the expected error logging of the server
const quietly = async (fn) => {
    const original = console.error
    console.error = () => {}
    try {
        return await fn()
    } finally {
        console.error = original
    }
}
const seed = async (collection, n) => {
    for (let i = 1; i <= n; i++) await post(collection, { id: 'd' + i }).expect(200)
}

describe('Oplog integrity', function () {
    after(function () {
        fs.rmSync(dataDir, { recursive: true, force: true })
    })

    describe('a missing file in the middle of the oplog', function () {
        const collection = 'gap'
        let saved
        before(async function () {
            await seed(collection, 3)
            saved = fs.readFileSync(oplogFile(collection, '2.json'), 'utf8')
            fs.unlinkSync(oplogFile(collection, '2.json'))
            await dbaCommand({ command: 'clearCache', collection }).expect(200)
        })
        it('fails the request with a diagnostic 500', async function () {
            const res = await quietly(() => get(collection).expect(500))
            assert.strictEqual(res.body.errorId, 'OPLOG_CORRUPT')
            assert.strictEqual(res.body.collection, collection)
            assert.strictEqual(res.body.opLogId, 2)
            assert.ok(res.body.error.includes('2.json'))
        })
        it('shows the collection as faulty in the list', async function () {
            const c = await listed(collection)
            assert.strictEqual(c.faulty, true)
            assert.ok(c.faultError.includes('missing oplog file 2.json'))
        })
        it('keeps failing without reading the disk again, even once the file is back', async function () {
            fs.writeFileSync(oplogFile(collection, '2.json'), saved)
            const res = await quietly(() => post(collection, { id: 'new' }).expect(500))
            assert.strictEqual(res.body.errorId, 'OPLOG_CORRUPT')
            await quietly(() => request(app).get(`/db/${collection}?fromOpLogId=0`).expect(500))
        })
        it('recovers after clearCache', async function () {
            await dbaCommand({ command: 'clearCache', collection }).expect(200)
            const res = await get(collection).expect(200)
            assert.deepStrictEqual(res.body.map((d) => d.id).sort(), ['d1', 'd2', 'd3'])
            assert.strictEqual((await listed(collection)).faulty, false)
            await post(collection, { id: 'd4' }).expect(200)
        })
    })

    describe('corrupt files', function () {
        for (const [name, content] of [
            ['empty', ''],
            ['truncated', '{"operation":"crea'],
            ['wrongId', JSON.stringify({ operation: 'create', collection: 'wrongId', data: [{ id: 'x' }], opLogId: 7 })],
        ]) {
            it(`a ${name} oplog file latches the collection`, async function () {
                await seed(name, 3)
                fs.writeFileSync(oplogFile(name, '2.json'), content)
                await dbaCommand({ command: 'clearCache', collection: name }).expect(200)
                const res = await quietly(() => get(name).expect(500))
                assert.strictEqual(res.body.errorId, 'OPLOG_CORRUPT')
                assert.strictEqual(res.body.opLogId, 2)
                assert.strictEqual((await listed(name)).faulty, true)
            })
        }
        it('clearCache accepts a comma separated list', async function () {
            await dbaCommand({ command: 'clearCache', collection: 'empty,truncated' }).expect(200)
            assert.strictEqual((await listed('empty')).faulty, false)
            assert.strictEqual((await listed('truncated')).faulty, false)
            assert.strictEqual((await listed('wrongId')).faulty, true)
        })
        it('drop removes the fault', async function () {
            await dbaCommand({ command: 'drop', collection: 'wrongId' }).expect(200)
            assert.strictEqual(await listed('wrongId'), undefined)
        })
    })

    describe('files that are not oplog files', function () {
        it('are ignored', async function () {
            const collection = 'stray'
            await seed(collection, 2)
            for (const name of ['3.0b9c.tmp', '3.json.tmp', '3.ndjson', '3.json.gz']) fs.writeFileSync(oplogFile(collection, name), 'not an operation')
            await dbaCommand({ command: 'clearCache', collection }).expect(200)
            const res = await get(collection).expect(200)
            assert.deepStrictEqual(res.body.map((d) => d.id).sort(), ['d1', 'd2'])
            await post(collection, { id: 'd3' }).expect(200)
            assert.ok(fs.readFileSync(oplogFile(collection, '3.json'), 'utf8').includes('"d3"'))
        })
    })

    describe('the first file after the checkpoint is missing', function () {
        const collection = 'leading'
        before(async function () {
            await seed(collection, 2)
            await dbaCommand({ command: 'flush', collection }).expect(200) // checkpoint 2
            await post(collection, { id: 'd3' }).expect(200)
            await post(collection, { id: 'd4' }).expect(200)
            fs.unlinkSync(oplogFile(collection, '3.json'))
            await dbaCommand({ command: 'clearCache', collection }).expect(200)
        })
        it('fails transiently on load, because another process may just have flushed', async function () {
            const res = await quietly(() => get(collection).expect(503))
            assert.strictEqual(res.body.errorId, 'OPLOG_LEADING_GAP')
            assert.strictEqual((await listed(collection)).faulty, false)
        })
        it('a client that is behind the oplog still gets a 404 to reload', async function () {
            await request(app).get(`/db/${collection}?fromOpLogId=1`).expect(404)
        })
        it('is not reported by the health check, which does not read the checkpoint', async function () {
            const res = await dbaCommand({ command: 'opLogHealthCheck', collection }).expect(200)
            assert.strictEqual(res.body.checked, 1)
            assert.deepStrictEqual(res.body.faultyCollections, [])
        })
    })

    describe('opLogHealthCheck', function () {
        it('finds faults in collections that are not in memory', async function () {
            await seed('healthy', 3)
            await seed('broken', 3)
            fs.unlinkSync(oplogFile('broken', '2.json'))
            await dbaCommand({ command: 'clearCache', collection: '*' }).expect(200)

            const res = await quietly(() => dbaCommand({ command: 'opLogHealthCheck', collection: '*' }).expect(200))
            const faulty = res.body.faultyCollections.map((f) => f.collection)
            assert.ok(faulty.includes('broken'))
            assert.ok(!faulty.includes('healthy'))
            assert.deepStrictEqual(res.body.errors, [])
            assert.strictEqual((await listed('broken')).faulty, true)
            assert.strictEqual((await listed('healthy')).faulty, false)
        })
        it('only reads file names: a corrupt file in an unbroken chain is not reported', async function () {
            await seed('garbled', 2)
            fs.writeFileSync(oplogFile('garbled', '2.json'), 'not json')
            const readFile = fsx.readFile
            let reads = 0
            fsx.readFile = (...args) => (reads++, readFile.apply(fsx, args))
            try {
                const res = await dbaCommand({ command: 'opLogHealthCheck', collection: 'garbled' }).expect(200)
                assert.ok(!res.body.faultyCollections.map((f) => f.collection).includes('garbled'))
            } finally {
                fsx.readFile = readFile
            }
            assert.strictEqual(reads, 0)
        })
        it('does not clear a fault: only clearCache does', async function () {
            fs.unlinkSync(oplogFile('broken', '3.json')) // the files are contiguous again
            const res = await dbaCommand({ command: 'opLogHealthCheck', collection: 'broken' }).expect(200)
            assert.ok(res.body.faultyCollections.map((f) => f.collection).includes('broken'))
            await dbaCommand({ command: 'clearCache', collection: 'broken' }).expect(200)
            await get('broken').expect(200)
        })
        it('rejects invalid collection names', async function () {
            await dbaCommand({ command: 'opLogHealthCheck', collection: '../escape' }).expect(400)
        })
    })
})
