import { Mutex, MutexInterface } from 'async-mutex'
import { Req, DBCommandParameters, Database } from './types'
import persist from './persistence'
import tools from './tools'
import util from 'util'
const debugLogger = util.debuglog('startupdb')

// Checkpoint archives are named yyyymmddhhmmss_ms.ndjson by flush
const ARCHIVED_CHECKPOINT_PATTERN = /^\d{14}_\d+\.ndjson(\.gz)?$/

const flush = async function (req: Req, commandParameters: DBCommandParameters, { startupDB, initStartupDB }: { startupDB: Database; initStartupDB: Function }) {
    const collection = commandParameters.collection
    const archive = commandParameters.options?.archive
    if (!collection) return { statusCode: 400, message: { error: 'No collection specified', errorId: 'tp5ut557FOBN' } }
    if (req.startupDB.options.opLogArchive != undefined && archive !== true && archive !== false)
        return { statusCode: 400, message: { error: 'No archive option specified', errorId: 'pL40dIKj81aW' } }
    const force = commandParameters.options?.force
    if (!persist.existsSync('./oplog/' + collection + '/latest.ndjson', req.startupDB) && !force) return { response: 'OK' }
    const dataFiles = req.startupDB.dataFiles
    const collectionId = dataFiles + '/' + collection
    if (!startupDB[collectionId]?.data) await initStartupDB(req.startupDB, collection)

    let mutex: MutexInterface = startupDB[collectionId]?.lock
    if (!mutex?.acquire) {
        mutex = new Mutex()
        startupDB[collectionId].lock = mutex
    }
    const release = await mutex.acquire()
    try {
        const checkpointDir = './checkpoint/' + collection
        const latestFile = checkpointDir + '/latest.ndjson'
        const tmpFile = latestFile + '.tmp'
        const archiveFileName = tools.yyyymmddhhmmss_ms(new Date()) + '.ndjson'

        // If memory says "never checkpointed" while archived checkpoints exist on disk, this collection was
        // (re)loaded from a missing/incomplete latest checkpoint. Flushing now would overwrite the latest
        // checkpoint with (near) empty data and rotate the oplog away, so refuse.
        if (!startupDB[collectionId].savedAt) {
            const checkpointFiles = await persist.readdir(checkpointDir, req.startupDB)
            if (checkpointFiles.some((file: string) => ARCHIVED_CHECKPOINT_PATTERN.test(file)))
                return { statusCode: 500, message: { error: 'Refusing to overwrite checkpoint: no checkpoint in memory but archived checkpoints exist on disk', errorId: 'kQ2vNxR8mEpz' } }
        }

        // Write the new checkpoint to a temporary file first so the latest checkpoint never ceases to exist.
        const savedAt = new Date().getTime()
        const json = startupDB[collectionId]
        const ndJsonHeader = {
            options: json.options,
            lastAccessed: json.lastAccessed,
            lastModified: json.lastModified,
            data: Array.isArray(json.data) ? [] : {},
            savedAt: savedAt,
            dbEngine: '2.5',
        }
        try {
            await persist.writeCheckpointToStream(ndJsonHeader, json.data, checkpointDir, 'latest.ndjson.tmp', req.startupDB)
        } catch (err) {
            debugLogger('' + err)
            return { statusCode: 500, message: { error: 'Cannot save checkpoint', errorId: 'Wms3x0goxHni' } }
        }

        // Preserve the current checkpoint under its archive name (copy, not rename), then atomically replace it.
        // On failure the old checkpoint is still intact and no in-memory state has changed.
        try {
            if (persist.existsSync(latestFile, req.startupDB)) await persist.copyFile(latestFile, checkpointDir + '/' + archiveFileName, req.startupDB)
            await persist.replaceFile(tmpFile, latestFile, req.startupDB)
        } catch (err) {
            debugLogger('' + err)
            return { statusCode: 500, message: { error: 'Unable to rename checkpoint', errorId: '2aH6sQe0Ojkc' } }
        }

        // Only commit in-memory state after the new checkpoint is durably in place.
        startupDB[collectionId].savedAt = savedAt

        // Rotate the oplog: all operations in it are included in the new checkpoint.
        // We hold the collection lock, so no operation can be appended in the meantime.
        if (persist.existsSync(`./oplog/${collection}/latest.ndjson`, req.startupDB)) {
            try {
                await persist.rename(`oplog/${collection}/latest.ndjson`, `oplog/${collection}/${archiveFileName}`, req.startupDB)
            } catch (err) {
                debugLogger('' + err)
                return { statusCode: 500, message: { error: 'Unable to rename oplog', errorId: 'aH6sQe0O2jkc' } }
            }
            if (req.startupDB.options.opLogArchive != undefined && archive == true)
                await persist.archive(`oplog/${collection}/${archiveFileName}`, `oplog/${collection}/${archiveFileName}`, req.startupDB)
            else await persist.rmdirSync(`oplog/${collection}/${archiveFileName}`, req.startupDB)
        }
        return { response: 'OK' }
    } finally {
        release()
    }
}
const create = async function (req: Req, commandParameters: DBCommandParameters, { startupDB }) {
    const collection = commandParameters.collection
    if (!collection) return { statusCode: 400, message: { error: 'No collection specified', errorId: 'z3CZhGh6zoSR' } }
    const dataFiles = req.startupDB.dataFiles
    const collectionId = dataFiles + '/' + collection
    try {
        if (persist.existsSync('./checkpoint/' + collection + '/latest.ndjson', req.startupDB))
            return { statusCode: 409, message: { error: 'Collection already exists', errorId: 'CuQn5ZSSIN79' } }
    } catch (err) {
        console.log(err)
    }
    startupDB[collectionId] = tools.deepCopy(tools.EMPTY_COLLECTION)
    startupDB[collectionId].lastAccessed = new Date().getTime()
    if (commandParameters.options) startupDB[collectionId].options = commandParameters.options
    if (commandParameters.options?.storageType == 'array') startupDB[collectionId].data = []
    startupDB[collectionId].checkPoint = 0
    startupDB[collectionId].savedAt = new Date()
    const json = startupDB[collectionId]
    const ndJsonHeader = {
        options: json.options,
        lastAccessed: json.lastAccessed,
        lastModified: json.lastModified,
        data: Array.isArray(json.data) ? [] : {},
        savedAt: json.savedAt,
        dbEngine: '3.0',
    }
    await persist.writeCheckpointToStream(ndJsonHeader, json.data, './checkpoint/' + collection, 'latest.ndjson', req.startupDB)
    return { response: 'OK' }
}
const ensureCollection = async function (req: Req, commandParameters: DBCommandParameters, { startupDB }) {
    const collection = commandParameters.collection
    if (!collection) return { statusCode: 400, message: { error: 'No collection specified', errorId: 'z3CZhGh6zoSR' } }
    const dataFiles = req.startupDB.dataFiles
    const collectionId = dataFiles + '/' + collection
    try {
        if (startupDB[collectionId] !== undefined || persist.existsSync('./checkpoint/' + collection + '/latest.ndjson', req.startupDB)) return { response: 'OK' }
    } catch (err) {
        console.log(err)
    }
    await create(req, commandParameters, { startupDB: startupDB })
    return { response: 'OK' }
}
const drop = async function (req: Req, commandParameters: DBCommandParameters, { startupDB }) {
    const collection = commandParameters.collection
    if (!collection) return { statusCode: 400, message: { error: 'No collection specified', errorId: '3CzZhhG6zuQ8' } }
    const dataFiles = req.startupDB.dataFiles
    const collectionId = dataFiles + '/' + collection
    if (!persist.rmdirSync('./oplog/' + collection, req.startupDB)) return { statusCode: 500, message: { error: 'Cannot remove files from oplog', errorId: 'QWmnJ8sUdhzw' } }
    if (!persist.rmdirSync('./checkpoint/' + collection, req.startupDB))
        return { statusCode: 500, message: { error: 'Cannot remove files from checkpoint', errorId: 'Zmn8smIUdhzw' } }
    delete startupDB[collectionId]
    return { response: 'OK' }
}
const purgeOplog = async function (req: Req, commandParameters: DBCommandParameters, { startupDB, initStartupDB }) {
    const collections = commandParameters.collection
    if (!collections) return { statusCode: 400, message: { error: 'No collection specified', errorId: 'CIvNZ51YQM6q' } }
    if (collections == '*') {
        if (!persist.rmdirSync('./oplog', req.startupDB)) return { statusCode: 500, message: { error: 'Cannot remove files from oplog', errorId: 'WLmnUdhzwJ8s' } }
        for (const collectionId in startupDB) startupDB[collectionId] = {}
        return { response: 'OK' }
    }
    const dataFiles = req.startupDB.dataFiles
    for (const collection of collections.split(',')) {
        const collectionId = dataFiles + '/' + collection
        if (!persist.rmdirSync('./oplog/' + collection, req.startupDB)) return { statusCode: 500, message: { error: 'Cannot remove files from oplog', errorId: 'J8nUdhzWLmws' } }
        startupDB[collectionId] = {}
        await initStartupDB(req.startupDB, collection)
    }
    return { response: 'OK' }
}
const clearCache = async function (req: Req, commandParameters: DBCommandParameters, { startupDB, initStartupDB }) {
    const collection = commandParameters.collection
    if (!collection) return { statusCode: 400, message: { error: 'No collection specified', errorId: 'CIvNZ51YQM6q' } }
    if (collection == '*') {
        for (const collectionId in startupDB) startupDB[collectionId] = {}
        return { response: 'OK' }
    }
    const dataFiles = req.startupDB.dataFiles
    const collectionId = dataFiles + '/' + collection
    startupDB[collectionId] = {}
    return { response: 'OK' }
}
const garbageCollector = async function (req: Req, commandParameters: DBCommandParameters, { startupDBGC }) {
    const deletedCollections = startupDBGC()
    return { status: 'success', deletedCollections: deletedCollections }
}
const inspect = async function (
    req: Req,
    commandParameters: DBCommandParameters,
    { startupDB, MAX_BYTES_IN_MEMORY, usedBytesInMemory }: { startupDB: any; MAX_BYTES_IN_MEMORY: number; usedBytesInMemory: number }
) {
    const v8 = require('v8')
    const heap = v8.getHeapStatistics()
    const orderedCollections = Object.keys(startupDB)
        .map((collection) => ({ collection: collection, lastAccessed: startupDB[collection].lastAccessed }))
        .sort((a, b) => a.lastAccessed - b.lastAccessed)

    return {
        status: 'success',
        usedBytesInMemory: usedBytesInMemory,
        MAX_BYTES_IN_MEMORY: MAX_BYTES_IN_MEMORY,
        total_heap_size: heap.total_heap_size,
        heap_size_limit: heap.heap_size_limit,
        heap_used: ((heap.total_heap_size / heap.heap_size_limit) * 100).toFixed(2) + '%',
        leastRecentlyUsed: {
            collection: orderedCollections[0]?.collection,
            lastAccessed: orderedCollections[0]?.lastAccessed,
        },
        nrCollectionsInCache: orderedCollections.length,
    }
}
const list = async function (req: Req, commandParameters: DBCommandParameters, { startupDB }) {
    const dataFiles = req.startupDB.dataFiles
    const collectionsList = Object.keys(startupDB).map((collectionName) => {
        return {
            name: collectionName,
            inCache: true,
            count: startupDB[collectionName].data ? Object.keys(startupDB[collectionName].data)?.length : 0,
        }
    })
    function addCollectionsFromFolder(folder) {
        const listedCollectionIndex = {}
        collectionsList.forEach((collection) => {
            listedCollectionIndex[collection.name] = true
        })
        const files = persist.readdirRecursive(dataFiles + '/' + folder).map((dir: string) => dir.replace('/' + folder, '').replace('\\' + folder, ''))
        const list = files.filter((file) => !listedCollectionIndex[file])
        list.forEach((file) => {
            collectionsList.push({ name: file, inCache: false, count: 0 })
        })
    }
    addCollectionsFromFolder('oplog')
    addCollectionsFromFolder('checkpoint')
    collectionsList.forEach((c) => (c.name = c.name.replace(dataFiles + '/', '')))
    collectionsList.forEach((c) => (c.name = c.name.replace(dataFiles + '\\', '')))
    collectionsList.forEach((c) => (c.name = c.name.replace(/\\/g, '/')))

    return { collections: collectionsList }
}
export default {
    create,
    drop,
    ensureCollection,
    flush,
    garbageCollector,
    inspect,
    list,
    purgeOplog,
    clearCache,
}
