import { Request, Response } from 'express'
import { MutexInterface } from 'async-mutex'

export type DBCommandParameters = {
    options?: any
    collection?: string
}

export type DBResponse = {
    statusCode: number
    data?: object
    message?: any
    headers?: object
}

export type DBOptions = {
    dataFiles?: string
    readOnly?: boolean
    serveRawCheckpoint?: boolean
    streamObjects?: boolean
    secondaryDataDirs?: Array<string>
    opLogArchive?: string
    addTimeStamps?: Function
    validator?: Function
    sentry?: any
}

export type DBConfig = {
    beforeGet?: Array<Function>
    beforePost?: Array<Function>
    beforePatch?: Array<Function>
    beforePut?: Array<Function>
    beforeDelete?: Array<Function>
    afterGet?: Array<Function>
    afterPost?: Array<Function>
    afterPatch?: Array<Function>
    afterPut?: Array<Function>
    afterDelete?: Array<Function>
    options: DBOptions
    dataFiles: string
    collection: string
    createObjects: Function
    getObjects: Function
    updateObjects: Function
    deleteObjects: Function
    patchObjects: Function
    executeDBAcommand: Function
    contentLength: number
}
export interface DBDataObject {
    id: string
    [key: string]: any
}

export type ArrayOfDBDataObjects = Array<DBDataObject>
export type CollectionOfObjects = { [key: string]: DBDataObject } | ArrayOfDBDataObjects
export interface Collection {
    options: any
    lastAccessed: number
    lastModified: number
    data: CollectionOfObjects
    lock: MutexInterface
    checkPoint: number
    nextOpLogId: number
    savedAt: number
    length: number
}

export interface Operation {
    operation: string
    opLogId: number
    collection: string
    data: Array<DBDataObject>
    oldData?: Array<DBDataObject>
    timestamp: {
        unixTimeStamp: number
        timeStamp: string
    }
}

export interface Database {
    [key: string]: Collection
}

export type Req = Request & { startupDB: DBConfig }
export type Res = Response

export interface SomeFunctions {
    [key: string]: Function
}
