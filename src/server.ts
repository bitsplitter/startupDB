import v8 from 'v8'
import { Req, Res, DBResponse, DBDataObject, DBOptions, DBConfig, Operation, CollectionOfObjects, Database, ArrayOfDBDataObjects, SomeFunctions } from './types'
import { NextFunction } from 'express'
import path from 'path'
import { compileExpression } from 'filtrex'
import { v4 as uuidv4 } from 'uuid'
import jsonPatch from 'fast-json-patch'
import { Mutex, MutexInterface } from 'async-mutex'
import util from 'util'
const debugLogger = util.debuglog('startupdb')
import persist from './persistence'
import dbaCommands from './dbaCommands'
import tools from './tools'
import fs from 'fs-extra'
import streams from './streams'

/**
* startupDB is a "no DB". A filebased database.
* It is meant to fast and cost effective for databases that 'fit in memory'.
* It implements CRUD operations over a REST API
*
* StartupDB is organized into collections.
* Collection names can be namespaced arbitrariliy
* Access to different endpoints can be implemented with existing Express Auth middleware
*
* GET /collection
* returns 200: the entire collection
* 
* GET /collection?id={id}
* returns 200: the document from collection with id = {id}
*
* GET /collection?filter={condition}
* returns 200: the documents from collection that satisfy the {condition}
* see https://www.npmjs.com/package/filtrex for more information on filters
* custom functions available in filters:
*   - lower(s) => return s.toLowerCase() 
*   - upper(s) => return s.toUpperCase() 

*
* POST /collection
* creates a document in "collection"
* if an "id" property does not exist, a uuid is added as id
* returns 200: the id of the document
* returns 409: if the document with the given id already exists
*
* PUT /collection
* updates a document in "collection"
* returns 200: the id of the document
* returns 404: if the document with the given does not exist
*
* Data is stored in files
* <dataFiles>/checkpoint/latest.json -- last flushed collection (zipped)
* <dataFiles>/oplog/*.json -- one file per CRUD operation
*
*
* Usage:
* app.use(startupDB(options))
*
* Options:
* - dataFiles: path to store the DB files. default .<route>
* - readOnly
* 
*
* Examples:
* app.use(startupDB({ "route": "/brands", "dataFiles": "/Users/jeroen/brands", "readOnly": true }))
* app.use(startupDB({ "route": "/data" }))
*
*/

const startupDB: Database = {} // This is where we keep the database in memory
// It's hard to calculate the memory footprint of JSON objects in memory, the best we can do is estimations
// and recalculate when we veer off course too much
let usedBytesInMemory = 0 // Keeps track of the total memory ised by the database.

/**
 * DB_CACHE_OVERHEAD_RATIO:
 * Ratio of stringified vs. in-memory objects
 *
 * Everything >= 2 sounds OK.
 * i.e.:
 * {"a":"a"}: Size on disk=9. Allocated>18. Size in memory is 4. 2 bytes for key and 2 byts for value (UTF-8)
 * {"a":0}:   Size on disk=7. Allocated>14. Size in memory is 10. 2 bytes for key, 8 for float.
 * {"key":"value","key1":0,:"key2":3.14}:
 *            Size on disk=37. Allocated>74. Size in memory is 48. 22 for keys, 10 for string value, 2*8 for floats
 **/
const DB_CACHE_OVERHEAD_RATIO = 2 // TODO: Make configurable
const DB_CACHE_FRACTION = 80 / 100 // Fraction of memory to use for Database cache, leave the rest for API and NodeJS. TODO: Make configurable
const MAX_BYTES_IN_MEMORY = parseInt('' + (v8.getHeapStatistics().heap_size_limit * DB_CACHE_FRACTION) / DB_CACHE_OVERHEAD_RATIO)
const GC_SWEEP_PERCENTAGE = 20 // Percentage of memory to sweep to prevent the GC to kick in too frequently

const propFunction = function (propertyName: string, get: (arg0: string) => any, obj: any) {
    const positionOfDot = propertyName.indexOf('.')
    if (positionOfDot < 0 && propertyName in obj) return get(propertyName)
    return propertyName.split('.').reduce((o, k) => (o || {})[k], obj)
}
let myFilter = function (s: string) {
    return false
}
const lower = function (s: string) {
    return s.toLowerCase()
}
const upper = function (s: string) {
    return s.toUpperCase()
}
const extract = function (array: Array<any>, property: string) {
    if (!Array.isArray(array)) return undefined
    return array.map((v) => v[property])
}
const filtrexOptions = { extraFunctions: { lower, upper, extract }, customProp: propFunction }

function logDateFormat(date: Date) {
    return date.toString().substring(0, 33)
}

function allObjectIdsExistInCollection(payload: Array<DBDataObject>, collectionCache: CollectionOfObjects): boolean {
    return !payload.find((item) => !item.id || !collectionCache[item.id])
}
function anyObjectIdExistsInCollection(payload: Array<DBDataObject>, collectionCache: CollectionOfObjects): boolean {
    return !!payload.find((item) => item.id && collectionCache[item.id])
}
function getObjectsForIdsInPayload(payload: Array<DBDataObject>, collectionCache: CollectionOfObjects): ArrayOfDBDataObjects {
    return payload.filter((item) => collectionCache[item.id]).map((item) => collectionCache[item.id])
}
function addIdsToItemsThatHaveNone(payload: Array<DBDataObject>): void {
    for (const item of payload) if (!item.id) item.id = uuidv4()
}
function validateDocuments(validator: Function, collection: string, documents: CollectionOfObjects) {
    try {
        const errors = validator(collection, documents)
        if (errors) return { statusCode: 400, message: { error: errors, errorId: '8XMXXtvHbEVP' } }
    } catch (e) {
        return { statusCode: 500, message: { error: 'Error during validation', errorId: 'WGVaQoFs1xy2' } }
    }
    return { statusCode: 0 }
}
const crud = <SomeFunctions>{}
crud.create = function (operation: Operation, collectionId: string, db: DBConfig, length: number) {
    if (startupDB[collectionId]?.options?.storageType == 'array') {
        for (const item of operation.data) {
            const arr = <ArrayOfDBDataObjects>startupDB[collectionId].data
            arr.push(item)
        }
        const delta = length || JSON.stringify(operation.data).length
        startupDB[collectionId].length += delta

        usedBytesInMemory += delta
    } else {
        for (const item of operation.data) {
            if (!startupDB[collectionId].data[item.id]) {
                // If length is not given, calculate dynamically
                const delta = length == 0 ? JSON.stringify(item).length : 0
                startupDB[collectionId].length += delta
                usedBytesInMemory += delta
            }

            startupDB[collectionId].data[item.id] = item
        }
        if (length != 0) {
            // If length is given, it describes the whole payload so ony add it once
            const delta = length
            startupDB[collectionId].length += delta
            usedBytesInMemory += delta
        }
    }
}
crud.delete = function (operation: Operation, collectionId: string, db: DBConfig, length: number) {
    for (const item of operation?.oldData || []) delete startupDB[collectionId].data[item.id]
    // We do not update memory consumption, deletes are rare and not worth the uncertainty of inacurate lengths
}
crud.update = function (operation: Operation, collectionId: string, db: DBConfig, length: number) {
    crud.create(operation, collectionId, db, length)
}
crud.patch = function (operation: Operation, collectionId: string, db: DBConfig, length: number) {
    for (const item of operation.data) {
        const document = <DBDataObject>tools.deepCopy(startupDB[collectionId].data[item.id] || {})
        let patchedDocument = document
        try {
            if (item.patch) jsonPatch.applyPatch(document, item.patch).newDocument
            else patchedDocument = Object.assign(patchedDocument, item)
        } catch (err) {
            return { statusCode: 400, message: { error: 'Invalid patch', errorId: 'SYtSsvvMlKiE' } }
        }
        const addTimeStamps = db.options.addTimeStamps
        if (typeof addTimeStamps == 'function') addTimeStamps('modified', patchedDocument, startupDB[collectionId].data[item.id])
        if (typeof db.options.validator == 'function') {
            const errors = validateDocuments(db.options.validator, operation.collection, patchedDocument)
            if (errors.statusCode > 0) return errors
        }
        startupDB[collectionId].data[item.id] = patchedDocument
        // If length is not given, calculate dynamically
        const delta = length == 0 ? JSON.stringify(item).length : 0
        startupDB[collectionId].length += delta
        usedBytesInMemory += delta
    }
    if (length != 0) {
        // If length is given, it describes the whole payload so ony add it once
        const delta = length
        startupDB[collectionId].length += delta
        usedBytesInMemory += delta
    }
}
const applyCRUDoperation = function (operation: Operation, db: DBConfig, length: number) {
    const crudOperation = operation.operation
    const collection = operation.collection
    const collectionId = db.dataFiles + '/' + collection
    const crudFunction = crud[crudOperation]

    const response = crudFunction(operation, collectionId, db, length)
    startupDB[collectionId].lastModified = new Date().getTime()
    startupDB[collectionId].lastAccessed = startupDB[collectionId].lastModified
    return response
}
function getOplogIDsFromFileNames(files: Array<string>, checkPoint: number): Array<number> {
    const opLogIds: Array<number> = []
    for (const fileName of files) {
        if (fileName.indexOf('.json') >= 0) {
            const opLogId = parseInt(fileName.replace('.json', ''))
            if (opLogId <= checkPoint) continue // Not relevant because they are already flushed
            opLogIds.push(opLogId)
        }
    }
    return opLogIds
}
//
// Helper function to loop over all opLog files after a certain checkPoint and call a function on it's content
//
const processOplog = async function (collection: string, db: DBConfig, checkPoint: number, func: (operation: Operation, length: number) => void) {
    const dataDirectory = './oplog/' + collection
    const files = await persist.readdir(dataDirectory, db)
    const opLogIds = getOplogIDsFromFileNames(files, checkPoint).sort(function (a: number, b: number) {
        return a - b
    }) // opLog must be processed in order.
    for (const opLogId of opLogIds) {
        try {
            const operation = <string>(<unknown>await persist.readFile(dataDirectory, opLogId + '.json', db))
            if (operation != '') func(JSON.parse(operation), operation.length)
        } catch (err) {
            // return debugLogger(logError(err))
        }
    }
}
/**
 * Initialize DB
 * Look for the latest checkpoint, load it when available
 * Look for operations in the opLog folder that happened after the latest checkpoint was made
 */
const initStartupDB = async function (db: DBConfig, collection: string) {
    const dataFiles = db.dataFiles
    const collectionId = dataFiles + '/' + collection
    const dataDirectory = './checkpoint/' + collection
    let checkPoint = 0

    // We're entering a critical section here that should not run concurrently.
    let mutex: MutexInterface = startupDB[collectionId]?.lock
    if (!mutex?.acquire) {
        mutex = new Mutex()
        startupDB[collectionId] = tools.deepCopy(tools.EMPTY_COLLECTION)
        startupDB[collectionId].lock = mutex
        startupDB[collectionId].lastAccessed = new Date().getTime()
    }
    const release = await mutex.acquire()
    // We got the lock so we know we're the only one running this code now.
    // First check if the data we're after isn't already there (then someone else had the lock first and finished the work and we don't do anything)
    // Under high load, startupDB[collectionId] sometimes does not exist. startupDBGC might have kicked it out so we check here for !startupDB[collectionId]
    if (!startupDB[collectionId] || startupDB[collectionId].nextOpLogId == 1) {
        debugLogger('Locked ' + collection)
        try {
            const ndJsononObject = await persist.readCheckpointFromStream(dataDirectory, 'latest.ndjson', db)
            if (ndJsononObject.savedAt) {
                startupDB[collectionId] = ndJsononObject
                startupDB[collectionId].length = ndJsononObject.totalBytes
            } else {
                const raw = <string>(<unknown>await persist.readFile(dataDirectory, 'latest.json', db))
                // This contains a previously saved checkpoint
                startupDB[collectionId] = JSON.parse(raw)
                startupDB[collectionId].length = raw.length
            }
        } catch (err) {
            if (err.code == 'ENOENT') {
                // Pretend that there is an empty collection
                startupDB[collectionId] = tools.deepCopy(tools.EMPTY_COLLECTION)
            }
        }
        startupDB[collectionId].lock = mutex
        startupDB[collectionId].lastAccessed = new Date().getTime()
        checkPoint = startupDB[collectionId].checkPoint
        usedBytesInMemory += startupDB[collectionId].length
        await processOplog(collection, db, checkPoint, function (operation: Operation, length: number) {
            startupDB[collectionId].nextOpLogId = operation.opLogId + 1
            applyCRUDoperation(operation, db, length)
        })
    } else {
        debugLogger('Locked... no need to retrieve', collection)
    }
    release() // Release the lock!
    debugLogger('Released ' + collection)
}

const loadCollection = async function (db: DBConfig, collection: string): Promise<boolean> {
    const collectionId = db.dataFiles + '/' + collection
    if (!startupDB[collectionId]?.data || startupDB[collectionId].nextOpLogId == 1) await initStartupDB(db, collection)
    if (!startupDB[collectionId]?.data) return false
    startupDB[collectionId].lastAccessed = new Date().getTime()
    return true
}

const writeOperationToOpLog = async function (operation: Operation, db: DBConfig) {
    const dataFiles = db.dataFiles
    const collection = operation.collection
    const collectionId = dataFiles + '/' + collection
    const oplogId = startupDB[collectionId].nextOpLogId++
    const date = new Date()
    const unixTimestamp = date.getTime()
    const timestamp = logDateFormat(date)

    operation.opLogId = oplogId
    operation.timestamp = {
        unixTimeStamp: unixTimestamp,
        timeStamp: timestamp,
    }
    const mutex: MutexInterface = startupDB[collectionId]?.lock
    const release = await mutex.acquire()

    await persist.writeFile('./oplog/' + collection, oplogId + '.json', JSON.stringify(operation), db)
    release()
}

const startupDBGC = function (options: any): number {
    let deletedCollections = 0
    const threshold = options?.testing ? 2048 : MAX_BYTES_IN_MEMORY
    if (usedBytesInMemory < threshold) return deletedCollections
    // Remove least recently used collections
    const orderedCollections = Object.keys(startupDB)
        .map((collection) => ({ collection: collection, lastAccessed: startupDB[collection].lastAccessed }))
        .sort((a, b) => a.lastAccessed - b.lastAccessed)

    let indexOfCollectionToDelete = 0
    do {
        const collectionToDelete = orderedCollections[indexOfCollectionToDelete++]?.collection
        if (collectionToDelete && startupDB[collectionToDelete]?.data) {
            usedBytesInMemory -= startupDB[collectionToDelete].length
            delete startupDB[collectionToDelete]
            deletedCollections++
        }
    } while (usedBytesInMemory >= (threshold * (100 - GC_SWEEP_PERCENTAGE)) / 100 && indexOfCollectionToDelete < orderedCollections.length)
    return deletedCollections
}

const dbExecuteDBAcommand = async function (req: Req, query: any, context: any) {
    const command = query.command
    if (command === undefined || !(command in dbaCommands)) return { statusCode: 400, message: { error: 'unknown command:' + command, errorId: '7WMWWvtJbE6P' } }
    return await dbaCommands[command](req, query, context)
}

const executeDBAcommand = async function (req: Req, res: Res, next: NextFunction, preHooks, postHooks) {
    let error = { statusCode: 0, data: {}, message: '', headers: {} }
    try {
        for (const beforeFn of preHooks) {
            error = await beforeFn(req, res, next, req.startupDB.collection)
            if (error.statusCode != 0) return res.status(error.statusCode).send(error.data)
        }
        let response = await dbExecuteDBAcommand(req, req.body, { usedBytesInMemory, MAX_BYTES_IN_MEMORY, startupDB, initStartupDB, startupDBGC })
        if (response.statusCode) return res.status(response.statusCode).send(response.message)
        for (const afterFn of postHooks) response = await afterFn(req, response)
        return res.send(response)
    } catch (e) {
        return res.status(500).send('')
    }
}
/**
 * Send the oplog starting from the given opLogId as an array op operations
 * This will allow a client to update it's remote copy of the database
 */
const sendOpLog = async function (req: Req, res: Res, next: NextFunction, fromOpLogId: number) {
    const collection = req.startupDB.collection
    if (isNaN(fromOpLogId)) return res.sendStatus(400)

    const opLog: Array<any> = []
    // When a client has old data and requests an opLogId that has been deleted (due to flush), we respond with a 404 signalling the client to reload
    let tooOld = true
    let prevId = -1 // Used to detect a gap in the oplog (because of a pending operation). Only send the contigues opLog
    await processOplog(collection, req.startupDB, fromOpLogId, function (operation: Operation) {
        if (operation.opLogId == fromOpLogId + 1) tooOld = false
        if (prevId != -1 && operation.opLogId > prevId + 1) return // a gap, don't send the rest
        prevId = operation.opLogId
        if (operation.operation == 'patch' || operation.operation == 'update') {
            opLog.push({ operation: operation.operation, collection: operation.collection, data: operation.data, opLogId: operation.opLogId, timestamp: operation.timestamp })
        }
        if (operation.operation == 'create' || operation.operation == 'delete') {
            opLog.push(operation)
        }
    })
    if (tooOld && opLog.length > 0) return res.sendStatus(404)
    return res.send(opLog)
}
const getHeaders = function (collectionId: string) {
    return {
        'x-last-checkpoint-time': startupDB[collectionId].savedAt || 0,
        'x-last-oplog-id': startupDB[collectionId].nextOpLogId - 1,
    }
}
const getOfflineHeaders = async function (collectionId: string, db: DBConfig) {
    const ndJsonFileName = './checkpoint/' + collectionId + '/latest.ndjson'
    const oplogFolder = './oplog/' + collectionId
    if (persist.existsSync(ndJsonFileName, db) || persist.existsSync(ndJsonFileName + '.gz', db)) {
        const fsStats = await persist.fileStats(ndJsonFileName, db)
        return {
            'x-last-checkpoint-time': fsStats.birthtimeMs,
            'x-last-oplog-id': (await persist.mostRecentFile(oplogFolder, db)) || -1,
            'Content-Type': 'application/x-ndjson',
        }
    }
    const fileName = './checkpoint/' + collectionId + '/latest.json'

    return {
        'x-last-checkpoint-time': (await persist.fileStats(fileName, db)).birthtimeMs,
        'x-last-oplog-id': (await persist.mostRecentFile(oplogFolder, db)) || -1,
        'Content-Type': 'application/json',
    }
}
/**
 *
 * Get metadata about objects from a given collection.
 *
 */
const dbGetHeaders = async function (db: DBConfig, collectionId: string) {
    return { headers: await getOfflineHeaders(collectionId, db) }
}

/**
 *
 * Get objects from a given collection.
 * Load the collection in memory if needed
 *
 * Return 200: one object if queried by id, otherwise an array of objects
 * Return 400: when the query is malformed
 * Return 404: when the collection does not exist
 *
 * @query param id: retrieve one document by id
 * @query param filter: retrieve multiple documents according to filter
 *       (see https://www.npmjs.com/package/filtrex)
 */
const dbGetObjects = async function (db: DBConfig, collection: string, payload: ArrayOfDBDataObjects, query = {}): Promise<DBResponse> {
    const collectionId = db.dataFiles + '/' + collection
    const id = query['id']
    const filter = query['filter']
    const returnType = (query['returnType'] || 'array').toLowerCase()
    const limit = parseInt(query['limit']) || 0
    const offset = parseInt(query['offset']) || 0
    if (!(await loadCollection(db, collection))) return { statusCode: 500, message: { error: 'Cannot load resource', errorId: '7d91kl3nw5z0' } }

    const headers = getHeaders(collectionId)
    if (id) {
        if (!startupDB[collectionId].data[id]) return { statusCode: 404, message: { error: `Id (${id}) not found`, errorId: '7qMhSaYDj7Vg' } }
        if (returnType == 'checkpoint') {
            const theOneObject = {}
            theOneObject[id] = startupDB[collectionId].data[id]
            const json = startupDB[collectionId]
            return {
                statusCode: 200,
                data: {
                    options: json.options,
                    lastAccessed: json.lastAccessed,
                    lastModified: json.lastModified,
                    data: theOneObject,
                    checkPoint: json.checkPoint,
                    nextOpLogId: json.nextOpLogId,
                    savedAt: json.savedAt,
                    length: json.length,
                },
                headers: headers,
            }
        }
        return { statusCode: 200, data: startupDB[collectionId].data[id] }
    } else if (filter) {
        try {
            myFilter = compileExpression(filter, filtrexOptions)
        } catch (err) {
            return { statusCode: 400, message: { error: '<p style="font-family:\'Courier New\'">' + err.message.replace(/\n/g, '<br>') + '</p>', errorId: 'is9IBEetHorq' } }
        }
        const filteredIds = Object.keys(startupDB[collectionId].data).filter((id) => myFilter(startupDB[collectionId].data[id]))
        const filteredArray = filteredIds.map((id) => startupDB[collectionId].data[id])
        if (returnType == 'checkpoint') {
            const filteredObject = filteredArray.reduce((acc, item) => {
                acc[item.id] = item
                return acc
            }, {})
            const json = startupDB[collectionId]
            return {
                statusCode: 200,
                data: {
                    options: json.options,
                    lastAccessed: json.lastAccessed,
                    lastModified: json.lastModified,
                    data: filteredObject,
                    checkPoint: json.checkPoint,
                    nextOpLogId: json.nextOpLogId,
                    savedAt: json.savedAt,
                    length: json.length,
                },
                headers: headers,
            }
        }
        if (limit) return { statusCode: 200, data: filteredArray.slice(offset, offset + limit) }
        else return { statusCode: 200, data: filteredArray }
    } else {
        // Return entire collection
        switch (returnType) {
            case 'object':
                return { statusCode: 200, data: startupDB[collectionId].data, headers: headers }
            case 'tally':
                return { statusCode: 200, data: { tally: Object.keys(startupDB[collectionId].data).length }, headers: headers }
            case 'checkpoint':
                return { statusCode: 200, data: startupDB[collectionId], headers: headers }
            default:
                if (limit)
                    return {
                        statusCode: 200,
                        data: Object.keys(startupDB[collectionId].data)
                            .map((id) => startupDB[collectionId].data[id])
                            .slice(offset, offset + limit),
                        headers: headers,
                    }
                else return { statusCode: 200, data: Object.keys(startupDB[collectionId].data).map((id) => startupDB[collectionId].data[id]), headers: headers }
        }
    }
}

const getRawCheckpoint = function (req: Req, res: Res, next: NextFunction, collection: string, fileName: string) {
    const db = req.startupDB
    const dataDirectory = './checkpoint/' + collection
    if (fileName.includes('.gz')) res.set('Content-Encoding', 'gzip')
    if (fileName.includes('.ndjson')) res.set('Content-Type', 'application/x-ndjson')
    else res.set('Content-Type', 'application/json')
    fs.createReadStream(path.join(db.dataFiles, dataDirectory, fileName)).pipe(res)
    return
}
const rawCheckpointExists = async function (req: Req, res: Res, next: NextFunction, collection: string, fileName: string) {
    const db = req.startupDB
    const x = persist.existsSync('./checkpoint/' + collection + '/' + fileName, db)
    return x
}
/**
 *
 * Delete one or more objects from a given collection.
 *
 * Create an entry in the opLog
 * Apply the delete CRUD operation
 *
 * Return 200: the 'old' object(s)
 * Return 400: if no id or fiter is specified
 */
const dbDeleteObjects = async function (db: DBConfig, collection: string, payload: ArrayOfDBDataObjects, query = <any>{}): Promise<DBResponse> {
    const dataFiles = db.dataFiles
    const collectionId = dataFiles + '/' + collection
    const id = query['id']
    const filter = query['filter']
    if (!(await loadCollection(db, collection))) return { statusCode: 500, message: { error: 'Cannot load resource', errorId: 'nw57d91kl3z0' } }

    if (startupDB[collectionId]?.options?.storageType == 'array') return { statusCode: 409, message: { error: 'Cannot delete from an array collection', errorId: 'MPqDs0QgPc8g' } }
    let oldData = <ArrayOfDBDataObjects>[]
    if (id) {
        if (!startupDB[collectionId].data[id]) return { statusCode: 404, message: { error: `Id (${id}) not found`, errorId: '8qMhWeKDj7Fg' } }
        oldData = [startupDB[collectionId].data[id]]
    } else if (filter) {
        try {
            myFilter = compileExpression(filter, filtrexOptions)
        } catch (err) {
            return { statusCode: 400, message: { error: '<p style="font-family:\'Courier New\'">' + err.message.replace(/\n/g, '<br>') + '</p>', errorId: 'is9IBEetHorq' } }
        }
        const filteredIds = Object.keys(startupDB[collectionId].data).filter((id) => myFilter(startupDB[collectionId].data[id]))
        oldData = filteredIds.map((id) => startupDB[collectionId].data[id])
    } else {
        return { statusCode: 400, message: { error: 'No id or filter specified', errorId: 'lMeRiqyICPbU' } }
    }
    const operation = {
        operation: 'delete',
        collection: collection,
        oldData: oldData,
        opLogId: 0,
        data: [],
        timestamp: {
            unixTimeStamp: 0,
            timeStamp: '',
        },
    }
    await writeOperationToOpLog(operation, db)
    applyCRUDoperation(operation, db, db.contentLength)
    return { statusCode: 200, data: query?.returnType != 'tally' ? oldData : { tally: oldData.length } }
}

/**
 *
 * Update one or more objects in a given collection. Create them if they don't exist
 *
 * Create an entry in the opLog
 * Apply the update CRUD operation
 *
 * Return 200: the 'old' objects
 */
const dbUpdateObjects = async function (db: DBConfig, collection: string, payload: ArrayOfDBDataObjects, query = <any>{}): Promise<DBResponse> {
    const dataFiles = db.dataFiles
    const collectionId = dataFiles + '/' + collection
    if (!(await loadCollection(db, collection))) return { statusCode: 500, message: { error: 'Cannot load resource', errorId: 'n1kl3z0w57d9' } }

    if (startupDB[collectionId]?.options?.storageType == 'array') return { statusCode: 409, message: { error: 'Cannot update an array collection', errorId: 'S7lC7Y1ffWp8' } }
    addIdsToItemsThatHaveNone(payload)

    const oldData = getObjectsForIdsInPayload(payload, startupDB[collectionId].data)
    if (typeof db.options.addTimeStamps == 'function') {
        const addTimeStamps = db.options.addTimeStamps
        for (const item of payload)
            if (!startupDB[collectionId].data[item.id]) addTimeStamps('created', item)
            else addTimeStamps('modified', item, startupDB[collectionId].data[item.id])
    }
    if (typeof db.options.validator == 'function') {
        const errors = validateDocuments(db.options.validator, collection, payload)
        if (errors.statusCode > 0) return errors
    }
    const operation = {
        operation: 'update',
        collection: collection,
        oldData: oldData,
        data: payload,
        opLogId: 0,
        timestamp: {
            unixTimeStamp: 0,
            timeStamp: '',
        },
    }
    await writeOperationToOpLog(operation, db)
    applyCRUDoperation(operation, db, db.contentLength)
    return { statusCode: 200, data: query?.returnType != 'tally' ? payload : { tally: payload.length } }
}

/**
 *
 * Update one or more objects in a given collection by applying the patches in the body
 *
 * Create an entry in the opLog
 * Apply the patch CRUD operation
 *
 * Return 200: the original POST body
 * Return 400: if one of the object does not exit or an id is missing from the request
 */
const dbPatchObjects = async function (db: DBConfig, collection: string, payload: ArrayOfDBDataObjects, query = <any>{}): Promise<DBResponse> {
    const dataFiles = db.dataFiles
    const collectionId = dataFiles + '/' + collection
    if (!(await loadCollection(db, collection))) return { statusCode: 500, message: { error: 'Cannot load resource', errorId: 'l3z0w57n1kd9' } }

    if (startupDB[collectionId]?.options?.storageType == 'array')
        return { statusCode: 409, message: { error: 'Cannot apply patch to an array collection', errorId: 'ZCssBbz1nevT' } }
    addIdsToItemsThatHaveNone(payload)

    const oldData = getObjectsForIdsInPayload(payload, startupDB[collectionId].data)
    const operation = {
        operation: 'patch',
        collection: collection,
        oldData: oldData,
        data: payload,
        opLogId: 0,
        timestamp: {
            unixTimeStamp: 0,
            timeStamp: '',
        },
    }
    // We're a bit lazy here. We should have checked wheter all the patches could be applied, instead, we let applyCRUDoperation figure it out.
    // If an error occurs, we don't write this update to the oplog. Worstcase scenario, we'll loose this patch in case of a DB crash
    try {
        const dbResponse = applyCRUDoperation(operation, db, db.contentLength)
        if (dbResponse?.statusCode) return dbResponse
    } catch (e) {
        return { statusCode: 400, message: { error: 'Could not apply all patches', errorId: 'PUpDKuw4NqyU' } }
    }
    await writeOperationToOpLog(operation, db)
    return { statusCode: 200, data: query?.returnType != 'tally' ? payload : { tally: payload.length } }
}

/**
 *
 * Create one or more objects in a given collection.
 * Load the collection in memory if needed
 *
 * If the id property is missing from an pbject, create one
 *
 * Create an entry in the opLog
 * Apply the create CRUD operation
 *
 * Return 200: the documents just created
 * Return 400: if one of the object does already exits
 */
const dbCreateObjects = async function (db: DBConfig, collection: string, payload: ArrayOfDBDataObjects, query = <any>{}): Promise<DBResponse> {
    const dataFiles = db.dataFiles
    const collectionId = dataFiles + '/' + collection
    if (!(await loadCollection(db, collection))) return { statusCode: 500, message: { error: 'Cannot load resource', errorId: 'l57n3z0w19dk' } }

    if (startupDB[collectionId]?.options?.storageType != 'array') {
        if (anyObjectIdExistsInCollection(payload, startupDB[collectionId].data))
            return { statusCode: 409, message: { error: "One or more id's already exist", errorId: '9s0UuxMbjK4x' } }
        addIdsToItemsThatHaveNone(payload)
        if (typeof db.options.addTimeStamps == 'function') {
            const addTimeStamps = db.options.addTimeStamps
            for (const item of payload) addTimeStamps('created', item)
        }
    }
    if (typeof db.options.validator == 'function') {
        const errors = validateDocuments(db.options.validator, collection, payload)
        if (errors.statusCode > 0) return errors
    }
    const operation = {
        operation: 'create',
        collection: collection,
        data: payload,
        opLogId: 0,
        timestamp: {
            unixTimeStamp: 0,
            timeStamp: '',
        },
    }
    await writeOperationToOpLog(operation, db)
    applyCRUDoperation(operation, db, db.contentLength)
    return { statusCode: 200, data: query?.returnType != 'tally' ? payload : { tally: payload.length } }
}

const processMethod = async function (req: Req, res: Res, next: NextFunction, collection: string, query: any, preHooks: Function[], method: Function, postHooks: Function[]) {
    let response = { statusCode: 0, data: {}, message: '', headers: {} }
    try {
        for (const beforeFn of preHooks) {
            response = await beforeFn(req, res, next, req.startupDB.collection)
            if (response?.statusCode >= 300) return res.status(response.statusCode).send(response.data)
            if (response?.statusCode != 0) break
        }
        if (response?.statusCode == 0) {
            if (req.method == 'GET' && query['fromOpLogId']) return await sendOpLog(req, res, next, parseInt(query['fromOpLogId']))
            if (req.method == 'GET' && query.returnType == 'checkPoint') {
                const fileNameScanOrder = req.headers['accept'] == 'application/x-ndjson' ? ['latest.ndjson.gz', 'latest.ndjson'] : ['latest.json.gz', 'latest.json']
                for (const fileName of fileNameScanOrder) {
                    if (req.startupDB.options.serveRawCheckpoint && (await rawCheckpointExists(req, res, next, collection, fileName)))
                        return getRawCheckpoint(req, res, next, collection, fileName)
                }
            }
            response = await method(req.startupDB, collection, req.body, query)
        }
        for (const afterFn of postHooks) response = await afterFn(req, response)
        if (response.headers) res.set(response.headers)
        if (response.statusCode > 200) return res.status(response.statusCode).send(response.message)
        if (query['fromOpLogId']) return await sendOpLog(req, res, next, parseInt(query['fromOpLogId']))
    } catch (e) {
        console.log('STARTUPDB Error', e)
        if (typeof req.startupDB.options.sentry?.captureException == 'function') req.startupDB.options.sentry.captureException(e)
        return res.status(500).send('')
    }
    if (!req.startupDB.options?.streamObjects || !response.data || typeof response.data == 'string') return res.send(response.data)
    res.set('Content-Type', 'application/json')
    if (Array.isArray(response.data)) {
        const jsonArrayStream = new streams.jsonArrayStream(response.data)
        jsonArrayStream.pipe(res)
        return
    }
    const jsonObjectStream = new streams.jsonObjectStream(response.data)
    jsonObjectStream.pipe(res)
}

const setupStartupDB = function (): DBConfig {
    const noop = function () {}
    return {
        options: {},
        dataFiles: '',
        collection: '',
        contentLength: 0,
        createObjects: noop,
        getObjects: noop,
        updateObjects: noop,
        deleteObjects: noop,
        patchObjects: noop,
        executeDBAcommand: noop,
    }
}
/**
 * Implement the main Express middleware function.
 * Store data on disk in the location specified by options.dataFiles. (default: baserUrl)
 * Add the options object to the request (as dbOptions) so we can read it where we need it.
 */
const db = function (options: DBOptions) {
    return async function (req: Req, res: Res, next: NextFunction) {
        options = options || {}
        startupDBGC(options)
        options.dataFiles = options.dataFiles || path.join(process.cwd(), req.baseUrl)
        if (options.dataFiles[0] == '.') options.dataFiles = path.join(process.cwd(), options.dataFiles)

        const rootRoute = req.path == '/'
        if (!req.startupDB) req.startupDB = setupStartupDB()
        if (!req.startupDB.beforeGet) req.startupDB.beforeGet = []
        if (!req.startupDB.beforePost) req.startupDB.beforePost = []
        if (!req.startupDB.beforePatch) req.startupDB.beforePatch = []
        if (!req.startupDB.beforePut) req.startupDB.beforePut = []
        if (!req.startupDB.beforeDelete) req.startupDB.beforeDelete = []
        if (!req.startupDB.afterGet) req.startupDB.afterGet = []
        if (!req.startupDB.afterPost) req.startupDB.afterPost = []
        if (!req.startupDB.afterPatch) req.startupDB.afterPatch = []
        if (!req.startupDB.afterPut) req.startupDB.afterPut = []
        if (!req.startupDB.afterDelete) req.startupDB.afterDelete = []

        req.startupDB.options = options
        req.startupDB.dataFiles = options.dataFiles
        try {
            req.url = decodeURIComponent(req.url)
        } catch (err) {
            return res.sendStatus(400)
        }
        req.startupDB.collection = req.url.split('?')[0].substring(1)

        // Define 'internal' CRUD functions that can be called from hooks functions
        req.startupDB.createObjects = async function (collection: string, payload: ArrayOfDBDataObjects) {
            return dbCreateObjects(req.startupDB, collection, tools.ensureArray(payload))
        }
        req.startupDB.getObjects = async function (collection: string, query) {
            return dbGetObjects(req.startupDB, collection, [], query)
        }
        req.startupDB.updateObjects = async function (collection: string, payload: ArrayOfDBDataObjects) {
            return dbUpdateObjects(req.startupDB, collection, tools.ensureArray(payload))
        }
        req.startupDB.deleteObjects = async function (collection: string, payload: ArrayOfDBDataObjects) {
            return dbDeleteObjects(req.startupDB, collection, tools.ensureArray(payload))
        }
        req.startupDB.patchObjects = async function (collection: string, payload: ArrayOfDBDataObjects) {
            return dbPatchObjects(req.startupDB, collection, tools.ensureArray(payload))
        }
        req.startupDB.executeDBAcommand = async function (payload: ArrayOfDBDataObjects) {
            return dbExecuteDBAcommand(req, payload, { usedBytesInMemory, startupDB, initStartupDB, startupDBGC })
        }

        const collection = req.startupDB.collection
        const query = req.query
        req.startupDB.contentLength = parseInt(req.headers['content-length'] || '0')
        if (req.method == 'GET' && rootRoute) {
            req.body = { command: 'list' }
            return await executeDBAcommand(req, res, next, req.startupDB.beforeGet, req.startupDB.afterGet)
        }
        if (req.method == 'GET') return await processMethod(req, res, next, collection, query, req.startupDB.beforeGet, dbGetObjects, req.startupDB.afterGet)
        if (req.method == 'HEAD') return await processMethod(req, res, next, collection, query, [], dbGetHeaders, [])

        if (options.readOnly) return res.sendStatus(403)
        // Be strict about what we consume

        if (
            req.method == 'DELETE' &&
            Object.keys(req.body).length > 1 &&
            req.headers['content-type'] != 'application/json' &&
            req.headers['content-type'] != 'application/json;charset=UTF-8'
        )
            return res.sendStatus(400)
        if (req.method != 'DELETE' && req.headers['content-type'] != 'application/json' && req.headers['content-type'] != 'application/json;charset=UTF-8')
            return res.sendStatus(400)

        if (req.method == 'POST' && rootRoute) return await executeDBAcommand(req, res, next, req.startupDB.beforePost, req.startupDB.afterPost)
        req.body = tools.ensureArray(req.body)

        if (req.method == 'POST') return await processMethod(req, res, next, collection, query, req.startupDB.beforePost, dbCreateObjects, req.startupDB.afterPost)
        if (req.method == 'PUT') return await processMethod(req, res, next, collection, query, req.startupDB.beforePut, dbUpdateObjects, req.startupDB.afterPut)
        if (req.method == 'DELETE') return await processMethod(req, res, next, collection, query, req.startupDB.beforeDelete, dbDeleteObjects, req.startupDB.afterDelete)
        if (req.method == 'PATCH') return await processMethod(req, res, next, collection, query, req.startupDB.beforePatch, dbPatchObjects, req.startupDB.afterPatch)

        next()
    }
}

// Register one or more hooks
// Expect a string or an array of strings with the names of the hooks and a function to register
const registerHook = function (hooks: Array<string>, fn: Function) {
    if (typeof fn != 'function') throw 'Registered hook is not a function'
    return async function (req: Req, res: Res, next: NextFunction) {
        if (!req.startupDB) req.startupDB = setupStartupDB()

        for (const hook of hooks) {
            if (!req.startupDB[hook]) req.startupDB[hook] = []
            req.startupDB[hook].push(fn)
        }
        next()
    }
}

module.exports = {
    db,
    beforeGet: function (fn: Function) {
        return registerHook(['beforeGet'], fn)
    },
    beforePost: function (fn: Function) {
        return registerHook(['beforePost'], fn)
    },
    beforePatch: function (fn: Function) {
        return registerHook(['beforePatch'], fn)
    },
    beforePut: function (fn: Function) {
        return registerHook(['beforePut'], fn)
    },
    beforeDelete: function (fn: Function) {
        return registerHook(['beforeDelete'], fn)
    },
    beforeAll: function (fn: Function) {
        return registerHook(['beforeGet', 'beforePost', 'beforePatch', 'beforePut', 'beforeDelete'], fn)
    },
    afterGet: function (fn: Function) {
        return registerHook(['afterGet'], fn)
    },
    afterPost: function (fn: Function) {
        return registerHook(['afterPost'], fn)
    },
    afterPatch: function (fn: Function) {
        return registerHook(['afterPatch'], fn)
    },
    afterPut: function (fn: Function) {
        return registerHook(['afterPut'], fn)
    },
    afterDelete: function (fn: Function) {
        return registerHook(['afterDelete'], fn)
    },
    afterAll: function (fn: Function) {
        return registerHook(['afterGet', 'afterPost', 'afterPatch', 'afterPut', 'afterDelete'], fn)
    },
}
