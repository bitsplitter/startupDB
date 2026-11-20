/*
Array collections have no ids, so a write never needs their data. Their manifest (checkpoint/<collection>/manifest.json)
holds what a write does need: when the data is not in memory, a POST loads only the manifest and appends to the oplog.
*/
const request = require('supertest')
const assert = require('assert')
const express = require('express')
const fs = require('fs')
const path = require('path')
const startupDB = require('../dist/server.js')

const root = path.join(__dirname, '..', 'tmp')
fs.mkdirSync(root, { recursive: true })
const dataDir = fs.mkdtempSync(path.join(root, 'arrayManifest-'))

const app = express()
app.use(express.json())
// GET /db/meta?collection=<name> returns req.startupDB.getCollectionMeta(<name>)
app.use(
    '/db/meta',
    startupDB.beforeGet(async (req) => ({ statusCode: 200, data: (await req.startupDB.getCollectionMeta(req.query.collection)) || null }))
)
app.use('/db', startupDB.db({ dataFiles: dataDir }))
// One server bound to the loopback address for the whole file, instead of an ephemeral one per request
const server = app.listen(0, '127.0.0.1')

const J = { 'Content-Type': 'application/json' }
const post = (collection, body) => request(server).post(`/db/${collection}`).set(J).send(body)
const get = (collection) => request(server).get(`/db/${collection}`)
const dbaCommand = (body) => request(server).post('/db').set(J).send(body)
const meta = async (collection) => (await request(server).get(`/db/meta?collection=${collection}`).expect(200)).body
const manifestFile = (collection) => path.join(dataDir, 'checkpoint', collection, 'manifest.json')
const readManifest = (collection) => JSON.parse(fs.readFileSync(manifestFile(collection), 'utf8'))
const oplogIds = (collection) =>
    fs
        .readdirSync(path.join(dataDir, 'oplog', collection))
        .filter((f) => /^\d+\.json$/.test(f))
        .map((f) => parseInt(f))
        .sort((a, b) => a - b)
const listed = async (collection) => (await request(server).get('/db').expect(200)).body.collections.find((c) => c.name == collection)
const ensureArray = (collection) => dbaCommand({ command: 'ensureCollection', collection, options: { storageType: 'array' } }).expect(200)
const rows = (n, from = 1) => Array.from({ length: n }, (_, i) => ({ ean: 'e' + (from + i), qty: 1 }))

describe('Array collections write through their manifest', function () {
    after(function () {
        server.close()
        fs.rmSync(dataDir, { recursive: true, force: true })
    })

    describe('a new array collection', function () {
        const c = 'shard/2026'
        before(async function () {
            await ensureArray(c)
        })
        it('gets a manifest from ensureCollection', function () {
            const manifest = readManifest(c)
            assert.strictEqual(manifest.nextOpLogId, 1)
            assert.strictEqual(manifest.checkPoint, 0)
            assert.strictEqual(manifest.options.storageType, 'array')
        })
        it('reports itself as empty', async function () {
            assert.strictEqual((await meta(c)).nextOpLogId, 1)
        })
        it('accepts POSTs without loading its data', async function () {
            await post(c, rows(2)).expect(200)
            await post(c, rows(2, 3)).expect(200)
            assert.deepStrictEqual(oplogIds(c), [1, 2])
            const entry = await listed(c)
            assert.strictEqual(entry.inCache, true)
            assert.strictEqual(entry.dataLoaded, false)
            assert.strictEqual((await meta(c)).nextOpLogId, 3)
        })
        it('continues the oplog after the cache is cleared', async function () {
            await dbaCommand({ command: 'clearCache', collection: c }).expect(200)
            await post(c, rows(1, 5)).expect(200)
            assert.deepStrictEqual(oplogIds(c), [1, 2, 3])
            assert.strictEqual((await listed(c)).dataLoaded, false)
        })
        it('returns every row on a GET, which loads the data', async function () {
            const res = await get(c).expect(200)
            assert.deepStrictEqual(
                res.body.map((r) => r.ean),
                ['e1', 'e2', 'e3', 'e4', 'e5']
            )
            assert.strictEqual((await listed(c)).dataLoaded, true)
        })
        it('keeps loaded data in sync with later POSTs', async function () {
            await post(c, rows(1, 6)).expect(200)
            const res = await get(c).expect(200)
            assert.strictEqual(res.body.length, 6)
            assert.strictEqual(res.body[5].ean, 'e6')
        })
        it('rejects a PUT with a 409', async function () {
            await request(server).put(`/db/${c}`).set(J).send(rows(1)).expect(409)
        })
    })

    describe('flush', function () {
        const c = 'shard/flushed'
        before(async function () {
            await ensureArray(c)
            await post(c, rows(3)).expect(200)
            await post(c, rows(3, 4)).expect(200)
        })
        it('writes the manifest with the new checkpoint and releases data it loaded itself', async function () {
            assert.strictEqual((await listed(c)).dataLoaded, false)
            await dbaCommand({ command: 'flush', collection: c }).expect(200)
            const manifest = readManifest(c)
            assert.strictEqual(manifest.checkPoint, 2)
            assert.strictEqual(manifest.nextOpLogId, 3)
            assert.ok(manifest.savedAt)
            assert.deepStrictEqual(oplogIds(c), [])
            assert.strictEqual((await listed(c)).dataLoaded, false)
        })
        it('does not report a flushed collection as empty', async function () {
            const m = await meta(c)
            assert.strictEqual(m.checkPoint, 2)
            assert.strictEqual(m.nextOpLogId, 3)
        })
        it('lets the next POST continue after the checkpoint, also from a cold cache', async function () {
            await dbaCommand({ command: 'clearCache', collection: c }).expect(200)
            await post(c, rows(1, 7)).expect(200)
            assert.deepStrictEqual(oplogIds(c), [3])
            const res = await get(c).expect(200)
            assert.deepStrictEqual(
                res.body.map((r) => r.ean),
                ['e1', 'e2', 'e3', 'e4', 'e5', 'e6', 'e7']
            )
        })
        it('leaves data loaded when it was loaded before the flush', async function () {
            assert.strictEqual((await listed(c)).dataLoaded, true)
            await dbaCommand({ command: 'flush', collection: c }).expect(200)
            assert.strictEqual((await listed(c)).dataLoaded, true)
            assert.strictEqual(readManifest(c).checkPoint, 3)
        })
    })

    describe('an array collection without a manifest (created before manifests existed)', function () {
        const c = 'shard/legacy'
        before(async function () {
            await ensureArray(c)
            await post(c, rows(2)).expect(200)
            await post(c, rows(2, 3)).expect(200)
            fs.unlinkSync(manifestFile(c))
            await dbaCommand({ command: 'clearCache', collection: c }).expect(200)
        })
        it('is loaded once by its first POST, gets a manifest and is released again', async function () {
            await post(c, rows(1, 5)).expect(200)
            assert.deepStrictEqual(oplogIds(c), [1, 2, 3])
            assert.strictEqual(readManifest(c).nextOpLogId, 3)
            assert.strictEqual((await listed(c)).dataLoaded, false)
        })
        it('then takes writes without loading', async function () {
            await post(c, rows(1, 6)).expect(200)
            assert.deepStrictEqual(oplogIds(c), [1, 2, 3, 4])
            assert.strictEqual((await listed(c)).dataLoaded, false)
            assert.strictEqual((await get(c).expect(200)).body.length, 6)
        })
        it('gets a manifest from getCollectionMeta as well', async function () {
            const c2 = 'shard/legacymeta'
            await ensureArray(c2)
            await post(c2, rows(1)).expect(200)
            fs.unlinkSync(manifestFile(c2))
            await dbaCommand({ command: 'clearCache', collection: c2 }).expect(200)
            assert.strictEqual((await meta(c2)).nextOpLogId, 2)
            assert.strictEqual(readManifest(c2).nextOpLogId, 2)
            assert.strictEqual((await listed(c2)).dataLoaded, false)
        })
    })

    describe('the manifest is a lower bound for the next opLogId', function () {
        const c = 'shard/lowerbound'
        before(async function () {
            await ensureArray(c)
            for (let i = 1; i <= 4; i++) await post(c, rows(1, i)).expect(200)
        })
        it('a manifest behind the oplog does not reuse ids', async function () {
            // The manifest still says nextOpLogId 1, the oplog runs to 4.json
            assert.strictEqual(readManifest(c).nextOpLogId, 1)
            await dbaCommand({ command: 'clearCache', collection: c }).expect(200)
            await post(c, rows(1, 5)).expect(200)
            assert.deepStrictEqual(oplogIds(c), [1, 2, 3, 4, 5])
        })
        it('a manifest ahead of the checkpoint (crash between manifest and checkpoint) still loads', async function () {
            const manifest = readManifest(c)
            fs.writeFileSync(manifestFile(c), JSON.stringify({ ...manifest, checkPoint: 5, nextOpLogId: 6 }))
            await dbaCommand({ command: 'clearCache', collection: c }).expect(200)
            await post(c, rows(1, 6)).expect(200)
            assert.deepStrictEqual(oplogIds(c), [1, 2, 3, 4, 5, 6])
            assert.strictEqual((await get(c).expect(200)).body.length, 6)
        })
        it('a damaged manifest is ignored and rewritten', async function () {
            fs.writeFileSync(manifestFile(c), '{"checkPoint":')
            await dbaCommand({ command: 'clearCache', collection: c }).expect(200)
            await post(c, rows(1, 7)).expect(200)
            assert.deepStrictEqual(oplogIds(c), [1, 2, 3, 4, 5, 6, 7])
            assert.strictEqual(readManifest(c).nextOpLogId, 7)
            assert.strictEqual((await get(c).expect(200)).body.length, 7)
        })
    })

    describe('concurrent POSTs on a cold array collection', function () {
        const c = 'shard/concurrent'
        before(async function () {
            await ensureArray(c)
            await dbaCommand({ command: 'clearCache', collection: c }).expect(200)
        })
        it('get consecutive opLogIds and lose nothing', async function () {
            await Promise.all(Array.from({ length: 20 }, (_, i) => post(c, rows(1, i + 1)).expect(200)))
            assert.deepStrictEqual(
                oplogIds(c),
                Array.from({ length: 20 }, (_, i) => i + 1)
            )
            assert.strictEqual((await get(c).expect(200)).body.length, 20)
        })
        it('interleaved with a flush lose nothing', async function () {
            await dbaCommand({ command: 'clearCache', collection: c }).expect(200)
            await Promise.all([
                ...Array.from({ length: 10 }, (_, i) => post(c, rows(1, 21 + i)).expect(200)),
                dbaCommand({ command: 'flush', collection: c }).expect(200),
                ...Array.from({ length: 10 }, (_, i) => post(c, rows(1, 31 + i)).expect(200)),
            ])
            await dbaCommand({ command: 'clearCache', collection: c }).expect(200)
            const eans = (await get(c).expect(200)).body.map((r) => r.ean)
            assert.strictEqual(eans.length, 40)
            assert.strictEqual(new Set(eans).size, 40)
        })
    })

    describe('object collections', function () {
        it('are loaded on write as before and get no manifest', async function () {
            const c = 'objects'
            await post(c, { id: 'a' }).expect(200)
            await dbaCommand({ command: 'clearCache', collection: c }).expect(200)
            await post(c, { id: 'b' }).expect(200)
            assert.strictEqual((await listed(c)).dataLoaded, true)
            await post(c, { id: 'a' }).expect(409)
            assert.ok(!fs.existsSync(manifestFile(c)))
        })
    })
})
