import { Req, DBConfig, DBCommandParameters } from './types'
import persist from './persistence'
import tools from './tools'
import debug from 'debug'
const debugLogger = debug('startupdb')
import chalk from 'chalk'
const logError = chalk.red

/**
 * Move all files in an oplog folder to it's accompanying archive folder
 */
const moveOpLog = async function (collection: string, oldCheckPoint: number, newCheckPoint: number, db: DBConfig) {
    for (let operation = oldCheckPoint; operation <= newCheckPoint; operation++) {
        persist.archive(`oplog/${collection}/${operation}.json`, db)
    }
}
const flush = async function (req: Req, commandParameters: DBCommandParameters, { startupDB, initStartupDB }) {
    const collection = commandParameters.collection
    if (!collection) return { "statusCode": 400, "message": { "error": "No collection specified", "errorId": "tp5ut557FOBN" } }
    const dataFiles = req.startupDB.dataFiles
    const collectionId = dataFiles + '/' + collection
    if (!startupDB[collectionId]?.data) await initStartupDB(req.startupDB, collection)
    const oldCheckPoint = startupDB[collectionId].checkPoint
    const newCheckPoint = startupDB[collectionId].nextOpLogId - 1
    if (oldCheckPoint > 0)
        try {
            persist.rename('./checkpoint/' + collection + '/latest.json', './checkpoint/' + collection + '/' + oldCheckPoint + '.json', req.startupDB)
        } catch (err) {
            return { "statusCode": 500, "message": { "error": "Unable to rename checkpoint", "errorId": "2aH6sQe0Ojkc" } }
        }

    startupDB[collectionId].checkPoint = newCheckPoint
    startupDB[collectionId].savedAt = new Date()
    let bufferToPersist = ""
    try {
        const serialized = JSON.stringify(startupDB[collectionId])
        bufferToPersist = serialized
    } catch (err) {
        debugLogger(logError(err))
        return { "statusCode": 500, "message": { "error": "Cannot serialize checkpoint, object too large?", "errorId": "RKdCqPkPyr7p" } }
    }

    try {
        await persist.writeFile('./checkpoint/' + collection, 'latest.json', bufferToPersist, req.startupDB)
    } catch (err) {
        debugLogger(logError(err))
        return { "statusCode": 500, "message": { "error": "Cannot save checkpoint", "errorId": "Wms3x0goxHni" } }
    }
    if (req.startupDB.options.opLogArchive != undefined) moveOpLog(collection, oldCheckPoint, newCheckPoint, req.startupDB)
    return { "response": "OK" }
}
const create = async function (req: Req, commandParameters: DBCommandParameters, { startupDB }) {
    const collection = commandParameters.collection
    if (!collection) return { "statusCode": 400, "message": { "error": "No collection specified", "errorId": "z3CZhGh6zoSR" } }
    const dataFiles = req.startupDB.dataFiles
    const collectionId = dataFiles + '/' + collection
    try {
        if (persist.existsSync('./checkpoint/' + collection + '/latest.json', req.startupDB)) return { "statusCode": 409, "message": { "error": "Collection already exists", "errorId": "CuQn5ZSSIN79" } }
    } catch (err) {
        console.log(err)
    }
    startupDB[collectionId] = tools.deepCopy(tools.EMPTY_COLLECTION)
    if (commandParameters.options) startupDB[collectionId].options = commandParameters.options
    if (commandParameters.options?.storageType == 'array') startupDB[collectionId].data = []
    const serialized = JSON.stringify(startupDB[collectionId])
    let bufferToPersist = serialized
    await persist.writeFile('./checkpoint/' + collection, 'latest.json', bufferToPersist, req.startupDB)
    return { "response": "OK" }
}
const ensureCollection = async function (req: Req, commandParameters: DBCommandParameters, { startupDB }) {
    const collection = commandParameters.collection
    if (!collection) return { "statusCode": 400, "message": { "error": "No collection specified", "errorId": "z3CZhGh6zoSR" } }
    const dataFiles = req.startupDB.dataFiles
    const collectionId = dataFiles + '/' + collection
    try {
        if (startupDB[collectionId] !== undefined || persist.existsSync('./checkpoint/' + collection + '/latest.json', req.startupDB)) return { "response": "OK" }
    } catch (err) {
        console.log(err)
    }
    startupDB[collectionId] = tools.deepCopy(tools.EMPTY_COLLECTION)
    if (commandParameters.options) startupDB[collectionId].options = commandParameters.options
    if (commandParameters.options?.storageType == 'array') startupDB[collectionId].data = []
    const serialized = JSON.stringify(startupDB[collectionId])
    let bufferToPersist = serialized
    await persist.writeFile('./checkpoint/' + collection, 'latest.json', bufferToPersist, req.startupDB)
    return { "response": "OK" }
}
const drop = async function (req: Req, commandParameters: DBCommandParameters, { startupDB }) {
    const collection = commandParameters.collection
    if (!collection) return { "statusCode": 400, "message": { "error": "No collection specified", "errorId": "3CzZhhG6zuQ8" } }
    const dataFiles = req.startupDB.dataFiles
    const collectionId = dataFiles + '/' + collection
    try {
        persist.rmdirSync('./oplog/' + collection, req.startupDB)
    } catch (err) {
        // No worries
    }
    try {
        persist.rmdirSync('./checkpoint/' + collection, req.startupDB)
    } catch (err) {
        // No worries
    }
    delete startupDB[collectionId]
    return { "response": "OK" }
}
const purgeOplog = async function (req: Req, commandParameters: DBCommandParameters, { startupDB, initStartupDB }) {
    const collection = commandParameters.collection
    if (!collection) return { "statusCode": 400, "message": { "error": "No collection specified", "errorId": "CIvNZ51YQM6q" } }
    const dataFiles = req.startupDB.dataFiles
    const collectionId = dataFiles + '/' + collection
    if (!persist.rmdirSync('./oplog/' + collection, req.startupDB)) return { "statusCode": 500, "message": { "error": "Cannot remove files from oplog", "errorId": "J8nUdhzWLmws" } }
    startupDB[collectionId] = {}
    await initStartupDB(req.startupDB, collection)
    return { "response": "OK" }
}
const garbageCollector = async function (req: Req, commandParameters: DBCommandParameters, { startupDBGC }) {
    const deletedCollections = startupDBGC()
    return { "status": "success", "deletedCollections": deletedCollections }
}
const inspect = async function (req: Req, commandParameters: DBCommandParameters, { startupDB, MAX_BYTES_IN_MEMORY, usedBytesInMemory }: { startupDB: any, MAX_BYTES_IN_MEMORY: number, usedBytesInMemory: number }) {
    const v8 = require('v8')
    const heap = v8.getHeapStatistics()
    const orderedCollections = Object.keys(startupDB).map(collection => {
        return { "collection": collection, "lastAccessed": startupDB[collection].lastAccessed }
    }).sort((a, b) => a.lastAccessed - b.lastAccessed)

    return {
        "status": "success",
        "usedBytesInMemory": usedBytesInMemory,
        "MAX_BYTES_IN_MEMORY": MAX_BYTES_IN_MEMORY,
        "total_heap_size": heap.total_heap_size,
        "heap_size_limit": heap.heap_size_limit,
        "heap_used": (heap.total_heap_size / heap.heap_size_limit * 100).toFixed(2) + '%',
        "leastRecentlyUsed": {
            "collection": orderedCollections[0]?.collection,
            "lastAccessed": orderedCollections[0]?.lastAccessed
        }
    }
}
const list = async function (req: Req, commandParameters: DBCommandParameters, { startupDB }) {
    const dataFiles = req.startupDB.dataFiles
    const collectionsList = Object.keys(startupDB).map(collectionName => {
        return {
            "name": collectionName,
            "inCache": true,
            "count": startupDB[collectionName].data ? Object.keys(startupDB[collectionName].data)?.length : 0,
            "checkPoint": startupDB[collectionName].checkPoint,
            "lastOplogId": startupDB[collectionName].lastOplogId || 0
        }
    })
    function addCollectionsFromFolder(folder) {
        const listedCollectionIndex = {}
        collectionsList.forEach(collection => { listedCollectionIndex[collection.name] = true })
        const files = persist.readdirRecursive(dataFiles + "/" + folder).map((dir: string) => dir.replace("/" + folder, "").replace("\\" + folder, ""))
        const list = files.filter(file => !listedCollectionIndex[file])
        list.forEach(file => {
            collectionsList.push({ "name": file, "inCache": false, "count": 0, "checkPoint": 0, "lastOplogId": 0 })
        })

    }
    addCollectionsFromFolder('oplog')
    addCollectionsFromFolder('checkpoint')
    collectionsList.forEach(c => c.name = c.name.replace(dataFiles + "/", ""))
    collectionsList.forEach(c => c.name = c.name.replace(dataFiles + "\\", ""))
    collectionsList.forEach(c => c.name = c.name.replace(/\\/g, "/"))

    return { "collections": collectionsList }
}
export default {
    create,
    drop,
    ensureCollection,
    flush,
    garbageCollector,
    inspect,
    list,
    purgeOplog
}
