/*
An oplog file is written once. It must never be overwritten, whatever the reason the server picked an id that is already on disk.
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
const dataDir = fs.mkdtempSync(path.join(root, 'noOverwrite-'))
const secondaryDir = fs.mkdtempSync(path.join(root, 'noOverwriteSecondary-'))

const app = express()
app.use(express.json())
app.use('/plain', startupDB.db({ dataFiles: dataDir }))
app.use('/mirrored', startupDB.db({ dataFiles: dataDir + '-mirrored', secondaryDataDirs: [secondaryDir] }))

const J = { 'Content-Type': 'application/json' }
const post = (route, collection, body) => request(app).post(`/${route}/${collection}`).set(J).send(body)
const patch = (route, collection, body) => request(app).patch(`/${route}/${collection}`).set(J).send(body)
const dbaCommand = (route, body) => request(app).post(`/${route}`).set(J).send(body)
const oplogDir = (base, collection) => path.join(base, 'oplog', collection)
const files = (base, collection) => fs.readdirSync(oplogDir(base, collection)).sort((a, b) => parseInt(a) - parseInt(b))
const read = (base, collection, name) => fs.readFileSync(path.join(oplogDir(base, collection), name), 'utf8')

// Temporarily replace a function on fs-extra (the module persistence.ts uses)
const stub = (name, impl) => {
    const original = fsx[name]
    fsx[name] = impl(original)
    return () => (fsx[name] = original)
}
const errno = (code) => Object.assign(new Error(code), { code })

describe('Oplog files are never overwritten', function () {
    after(function () {
        for (const dir of [dataDir, dataDir + '-mirrored', secondaryDir]) fs.rmSync(dir, { recursive: true, force: true })
    })

    describe('id already on disk (stale nextOpLogId)', function () {
        const collection = 'stale'
        before(async function () {
            for (let i = 1; i <= 3; i++) await post('plain', collection, { id: 'd' + i }).expect(200)
        })
        it('fails with a 500 and leaves the existing file untouched', async function () {
            // Another process / an earlier run wrote the file we are about to write
            const intruder = JSON.stringify({ operation: 'create', collection, data: [{ id: 'original' }], opLogId: 4 })
            fs.writeFileSync(path.join(oplogDir(dataDir, collection), '4.json'), intruder)

            await post('plain', collection, { id: 'new' }).expect(500)
            assert.strictEqual(read(dataDir, collection, '4.json'), intruder)
        })
        it('the failed document is not in the collection', async function () {
            const res = await request(app).get(`/plain/${collection}`).expect(200)
            assert.deepStrictEqual(res.body.map((d) => d.id).sort(), ['d1', 'd2', 'd3'])
        })
        it('retries hit the same id and fail again, and each rejection is logged', async function () {
            const logged = []
            const original = console.error
            console.error = (...args) => logged.push(args.join(' '))
            try {
                await post('plain', collection, { id: 'again' }).expect(500)
            } finally {
                console.error = original
            }
            assert.deepStrictEqual(files(dataDir, collection), ['1.json', '2.json', '3.json', '4.json'])
            assert.ok(read(dataDir, collection, '4.json').includes('"original"'))
            const conflict = logged.find((line) => line.includes('STARTUPDB OPLOG CONFLICT'))
            assert.ok(conflict, 'expected an oplog conflict log line')
            assert.ok(conflict.includes(`collectionId ${dataDir}/${collection}`))
            assert.ok(conflict.includes('oplogId 4'))
        })
        it('succeeds with that same id once the existing file is removed', async function () {
            fs.unlinkSync(path.join(oplogDir(dataDir, collection), '4.json'))
            await post('plain', collection, { id: 'after' }).expect(200)
            assert.deepStrictEqual(files(dataDir, collection), ['1.json', '2.json', '3.json', '4.json'])
            assert.ok(read(dataDir, collection, '4.json').includes('"after"'))
        })
    })

    describe('unreadable newest file after a reload (crash left a truncated file)', function () {
        const collection = 'truncated'
        it('does not overwrite the truncated file', async function () {
            for (let i = 1; i <= 3; i++) await post('plain', collection, { id: 'd' + i }).expect(200)
            const f = path.join(oplogDir(dataDir, collection), '3.json')
            const truncated = fs.readFileSync(f, 'utf8').slice(0, 20)
            fs.writeFileSync(f, truncated)
            await dbaCommand('plain', { command: 'clearCache', collection }).expect(200) // same as a restart for this collection

            await post('plain', collection, { id: 'new' }).expect(500)
            assert.strictEqual(fs.readFileSync(f, 'utf8'), truncated)
            assert.deepStrictEqual(files(dataDir, collection), ['1.json', '2.json', '3.json'])
        })
        it('clearCache does not unblock while the truncated file is still there', async function () {
            await dbaCommand('plain', { command: 'clearCache', collection }).expect(200)
            await post('plain', collection, { id: 'new' }).expect(500)
            assert.deepStrictEqual(files(dataDir, collection), ['1.json', '2.json', '3.json'])
        })
        it('clearCache unblocks once the truncated file is gone', async function () {
            fs.unlinkSync(path.join(oplogDir(dataDir, collection), '3.json'))
            await dbaCommand('plain', { command: 'clearCache', collection }).expect(200)
            await post('plain', collection, { id: 'new' }).expect(200)
            assert.ok(read(dataDir, collection, '3.json').includes('"new"'))
        })
    })

    describe('PUT, DELETE and PATCH', function () {
        // A rejected write does not consume the id, so the same file stays in the way until it is removed.
        const collection = 'verbs'
        const blocked = () => path.join(oplogDir(dataDir, collection), '2.json')
        const block = () => fs.writeFileSync(blocked(), 'occupied')
        const unblock = () => fs.unlinkSync(blocked())
        before(async function () {
            await post('plain', collection, { id: 'a', v: 1 }).expect(200) // 1.json
        })
        it('PUT fails and does not change memory', async function () {
            block()
            await request(app).put(`/plain/${collection}`).set(J).send({ id: 'a', v: 2 }).expect(500)
            assert.strictEqual(read(dataDir, collection, '2.json'), 'occupied')
            const res = await request(app).get(`/plain/${collection}?id=a`).expect(200)
            assert.strictEqual(res.body.v, 1)
            unblock()
        })
        it('DELETE fails and does not change memory', async function () {
            block()
            await request(app).delete(`/plain/${collection}?id=a`).expect(500)
            assert.strictEqual(read(dataDir, collection, '2.json'), 'occupied')
            await request(app).get(`/plain/${collection}?id=a`).expect(200)
            unblock()
        })
        it('PATCH fails and rolls back the in-memory patch', async function () {
            block()
            await patch('plain', collection, { id: 'a', v: 3 }).expect(500)
            assert.strictEqual(read(dataDir, collection, '2.json'), 'occupied')
            const res = await request(app).get(`/plain/${collection}?id=a`).expect(200)
            assert.strictEqual(res.body.v, 1)
            unblock()
        })
        it('PATCH on a not yet existing id leaves nothing behind when rejected', async function () {
            block()
            await patch('plain', collection, { id: 'ghost', v: 1 }).expect(500)
            await request(app).get(`/plain/${collection}?id=ghost`).expect(404)
            unblock()
        })
        it('works again once the file is gone', async function () {
            await patch('plain', collection, { id: 'a', v: 4 }).expect(200)
            const res = await request(app).get(`/plain/${collection}?id=a`).expect(200)
            assert.strictEqual(res.body.v, 4)
            assert.ok(read(dataDir, collection, '2.json').includes('"v":4') || read(dataDir, collection, '2.json').includes('"v": 4'))
        })
    })

    describe('secondary data dirs', function () {
        it('a conflict in a secondary dir fails the request and removes the primary file this request created', async function () {
            const collection = 'mirror'
            await post('mirrored', collection, { id: 'd1' }).expect(200)
            fs.mkdirSync(oplogDir(secondaryDir, collection), { recursive: true })
            fs.writeFileSync(path.join(oplogDir(secondaryDir, collection), '2.json'), 'precious backup')

            await post('mirrored', collection, { id: 'd2' }).expect(500)
            assert.strictEqual(read(secondaryDir, collection, '2.json'), 'precious backup')
            assert.ok(!fs.existsSync(path.join(oplogDir(dataDir + '-mirrored', collection), '2.json')), 'primary 2.json must be rolled back')
            await post('mirrored', collection, { id: 'd2' }).expect(500) // same id, still blocked by the secondary file

            fs.unlinkSync(path.join(oplogDir(secondaryDir, collection), '2.json'))
            await post('mirrored', collection, { id: 'd2' }).expect(200)
            assert.deepStrictEqual(files(dataDir + '-mirrored', collection), ['1.json', '2.json'])
            assert.deepStrictEqual(files(secondaryDir, collection), ['1.json', '2.json'])
        })
    })

    describe('write failures', function () {
        it('a write that fails halfway leaves no partial file and does not burn the id', async function () {
            const collection = 'partial'
            await post('plain', collection, { id: 'd1' }).expect(200)
            const restore = stub('writeFile', (original) => async (target, ...rest) => {
                if (typeof target == 'number') throw errno('ENOSPC') // writing to the fd we got from the exclusive open
                return original.call(fsx, target, ...rest)
            })
            try {
                await post('plain', collection, { id: 'd2' }).expect(500)
            } finally {
                restore()
            }
            assert.deepStrictEqual(files(dataDir, collection), ['1.json'])
            await post('plain', collection, { id: 'd2' }).expect(200)
            assert.deepStrictEqual(files(dataDir, collection), ['1.json', '2.json']) // no gap
        })
        it('a failed write releases the collection lock', async function () {
            const collection = 'partial'
            const restore = stub('writeFile', (original) => async (target, ...rest) => {
                if (typeof target == 'number') throw errno('EIO')
                return original.call(fsx, target, ...rest)
            })
            try {
                await post('plain', collection, { id: 'x' }).expect(500)
            } finally {
                restore()
            }
            await post('plain', collection, { id: 'y' }).expect(200) // would hang when the mutex was never released
        })
    })

    describe('Windows: transient sharing violations', function () {
        for (const code of ['EPERM', 'EBUSY', 'EACCES']) {
            it(`${code} while creating the file is retried and then succeeds`, async function () {
                const collection = 'transient' + code
                await post('plain', collection, { id: 'd1' }).expect(200)
                let failures = 2
                const restore = stub('open', (original) => async (target, flags, ...rest) => {
                    if (flags == 'wx' && String(target).includes(collection) && failures-- > 0) throw errno(code)
                    return original.call(fsx, target, flags, ...rest)
                })
                try {
                    await post('plain', collection, { id: 'd2' }).expect(200)
                } finally {
                    restore()
                }
                assert.deepStrictEqual(files(dataDir, collection), ['1.json', '2.json'])
            })
        }
        it('a persistent EPERM fails the request and never touches the existing file', async function () {
            const collection = 'persistentEperm'
            await post('plain', collection, { id: 'd1' }).expect(200)
            const existing = path.join(oplogDir(dataDir, collection), '2.json')
            fs.writeFileSync(existing, 'delete pending lookalike')
            const unlinked = []
            const restoreOpen = stub('open', (original) => async (target, flags, ...rest) => {
                if (flags == 'wx' && String(target).endsWith('2.json')) throw errno('EPERM')
                return original.call(fsx, target, flags, ...rest)
            })
            const restoreUnlink = stub('unlink', (original) => async (target, ...rest) => {
                unlinked.push(target)
                return original.call(fsx, target, ...rest)
            })
            try {
                await post('plain', collection, { id: 'd2' }).expect(500)
            } finally {
                restoreOpen()
                restoreUnlink()
            }
            assert.strictEqual(fs.readFileSync(existing, 'utf8'), 'delete pending lookalike')
            assert.deepStrictEqual(unlinked, [], 'a file we did not create must never be removed')
        })
    })

    describe('concurrency', function () {
        it('parallel writes to one collection all get their own file', async function () {
            const collection = 'parallel'
            const results = await Promise.all(Array.from({ length: 25 }, (_, i) => post('plain', collection, { id: 'p' + i })))
            assert.ok(results.every((r) => r.status == 200))
            assert.strictEqual(files(dataDir, collection).length, 25)
            const ids = files(dataDir, collection).map((f) => JSON.parse(read(dataDir, collection, f)).opLogId)
            assert.deepStrictEqual(ids, Array.from({ length: 25 }, (_, i) => i + 1))
        })
    })
})
