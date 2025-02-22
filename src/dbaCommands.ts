import { Req, DBConfig, DBCommandParameters } from './types'
import persist from './persistence'
import tools from './tools'
import debug from 'debug'
const debugLogger = debug('startupdb')

/**
 * Move all files in an oplog folder that are obsolete to it's accompanying archive folder
 */
const moveOpLog = async function (collection: string, oldCheckPoint: number, newCheckPoint: number, db: DBConfig) {
    for (let operation = oldCheckPoint; operation <= newCheckPoint; operation++) {
        await persist.archive(`oplog/${collection}/${operation}.json`, db)
    }
}
/**
 * Remove all files in an oplog folder that are obsolete
 */
const clearOplog = async function (collection: string, oldCheckPoint: number, newCheckPoint: number, db: DBConfig) {
    for (let operation = oldCheckPoint; operation <= newCheckPoint; operation++) {
        await persist.remove(`oplog/${collection}/${operation}.json`, db)
    }
}
const flush = async function (req: Req, commandParameters: DBCommandParameters, { startupDB, initStartupDB }) {
    const collection = commandParameters.collection
    const archive = commandParameters.options?.archive
    if (!collection) return { statusCode: 400, message: { error: 'No collection specified', errorId: 'tp5ut557FOBN' } }
    if (req.startupDB.options.opLogArchive != undefined && archive !== true && archive !== false)
        return { statusCode: 400, message: { error: 'No archive option specified', errorId: 'pL40dIKj81aW' } }

    const contentType = commandParameters.options?.contentType
    const force = commandParameters.options?.force
    const fileType = contentType == 'ndjson' ? '.ndjson' : '.json'
    const oplogFiles = await persist.readdir('./oplog/' + collection, req.startupDB)
    if (oplogFiles.length == 0 && !force) return { response: 'OK' }
    const dataFiles = req.startupDB.dataFiles
    const collectionId = dataFiles + '/' + collection
    if (!startupDB[collectionId]?.data) await initStartupDB(req.startupDB, collection)
    const oldCheckPoint = startupDB[collectionId].checkPoint
    const newCheckPoint = startupDB[collectionId].nextOpLogId - 1
    if (oldCheckPoint > 0 && persist.existsSync('./checkpoint/' + collection + '/latest' + fileType, req.startupDB))
        try {
            persist.rename('./checkpoint/' + collection + '/latest' + fileType, './checkpoint/' + collection + '/' + oldCheckPoint + fileType, req.startupDB)
        } catch (err) {
            return { statusCode: 500, message: { error: 'Unable to rename checkpoint', errorId: '2aH6sQe0Ojkc' } }
        }

    startupDB[collectionId].checkPoint = newCheckPoint
    startupDB[collectionId].savedAt = new Date()
    let bufferToPersist = ''
    if (fileType == '.ndjson') {
        const json = startupDB[collectionId]
        const ndJsonHeader = {
            options: json.options,
            lastAccessed: json.lastAccessed,
            lastModified: json.lastModified,
            data: Array.isArray(json.data) ? [] : {},
            checkPoint: json.checkPoint,
            nextOpLogId: json.nextOpLogId,
            savedAt: json.savedAt,
            dbEngine: '2.0',
        }
        await persist.writeCheckpointToStream(ndJsonHeader, json.data, './checkpoint/' + collection, 'latest.ndjson', req.startupDB)
    } else {
        try {
            bufferToPersist = JSON.stringify(startupDB[collectionId])
        } catch (err) {
            debugLogger(err)
            return { statusCode: 500, message: { error: 'Cannot serialize checkpoint, object too large?', errorId: 'RKdCqPkPyr7p' } }
        }

        try {
            await persist.writeFile('./checkpoint/' + collection, 'latest.json', bufferToPersist, req.startupDB)
        } catch (err) {
            debugLogger(err)
            return { statusCode: 500, message: { error: 'Cannot save checkpoint', errorId: 'Wms3x0goxHni' } }
        }
    }
    if (req.startupDB.options.opLogArchive != undefined && archive == true) await moveOpLog(collection, oldCheckPoint, newCheckPoint, req.startupDB)
    else await clearOplog(collection, oldCheckPoint, newCheckPoint, req.startupDB)
    return { response: 'OK' }
}
const create = async function (req: Req, commandParameters: DBCommandParameters, { startupDB }) {
    const collection = commandParameters.collection
    if (!collection) return { statusCode: 400, message: { error: 'No collection specified', errorId: 'z3CZhGh6zoSR' } }
    const dataFiles = req.startupDB.dataFiles
    const collectionId = dataFiles + '/' + collection
    try {
        if (persist.existsSync('./checkpoint/' + collection + '/latest.json', req.startupDB))
            return { statusCode: 409, message: { error: 'Collection already exists', errorId: 'CuQn5ZSSIN79' } }
    } catch (err) {
        console.log(err)
    }
    startupDB[collectionId] = tools.deepCopy(tools.EMPTY_COLLECTION)
    startupDB[collectionId].lastAccessed = new Date().getTime()
    if (commandParameters.options) startupDB[collectionId].options = commandParameters.options
    if (commandParameters.options?.storageType == 'array') startupDB[collectionId].data = []
    await persist.writeFile('./checkpoint/' + collection, 'latest.json', JSON.stringify(startupDB[collectionId]), req.startupDB)
    return { response: 'OK' }
}
const ensureCollection = async function (req: Req, commandParameters: DBCommandParameters, { startupDB }) {
    const collection = commandParameters.collection
    if (!collection) return { statusCode: 400, message: { error: 'No collection specified', errorId: 'z3CZhGh6zoSR' } }
    const dataFiles = req.startupDB.dataFiles
    const collectionId = dataFiles + '/' + collection
    try {
        if (startupDB[collectionId] !== undefined || persist.existsSync('./checkpoint/' + collection + '/latest.json', req.startupDB)) return { response: 'OK' }
    } catch (err) {
        console.log(err)
    }
    startupDB[collectionId] = tools.deepCopy(tools.EMPTY_COLLECTION)
    if (commandParameters.options) startupDB[collectionId].options = commandParameters.options
    if (commandParameters.options?.storageType == 'array') startupDB[collectionId].data = []
    await persist.writeFile('./checkpoint/' + collection, 'latest.json', JSON.stringify(startupDB[collectionId]), req.startupDB)
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
            checkPoint: startupDB[collectionName].checkPoint,
            lastOplogId: startupDB[collectionName].lastOplogId || 0,
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
            collectionsList.push({ name: file, inCache: false, count: 0, checkPoint: 0, lastOplogId: 0 })
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
