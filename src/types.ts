import { Request, Response } from 'express'
import { MutexInterface } from 'async-mutex'

export type DBCommandType = 'create' | 'drop' | 'ensureCollection' | 'flush' | 'garbageCollector' | 'inspect' | 'list' | 'purgeOplog' | 'clearCache'

export type StorageType = 'array' | 'object'

export type FlushOptions = {
    archive?: boolean
    contentType?: 'json' | 'ndjson'
    force?: boolean
}

export type CreateOptions = {
    storageType?: StorageType
    [key: string]: any
}

export type CreateCommandParams = {
    command: 'create' | 'ensureCollection'
    collection?: string
    options?: CreateOptions
}

export type FlushCommandParams = {
    command: 'flush'
    collection?: string
    options?: FlushOptions
}

export type PurgeCommandParams = {
    command: 'purgeOplog' | 'clearCache'
    collection?: string | '*'
}

export type DropCommandParams = {
    command: 'drop'
    collection?: string
}

export type SimpleCommandParams = {
    command: 'garbageCollector' | 'inspect' | 'list'
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
    streamObjects?: boolean
    secondaryDataDirs?: Array<string>
    opLogArchive?: string
    addTimeStamps?: Function
    validator?: Function
    sentry?: any
}

export type DBbeforeHookFunction = (req: Req, res: Res, next: () => void, collection: string) => Promise<DBResponse>
export type DBafterHookFunction = (req: Req, response: DBResponse) => Promise<DBResponse>

export type DBConfig = {
    beforeGet?: Array<DBbeforeHookFunction>
    beforePost?: Array<DBbeforeHookFunction>
    beforePatch?: Array<DBbeforeHookFunction>
    beforePut?: Array<DBbeforeHookFunction>
    beforeDelete?: Array<DBbeforeHookFunction>
    afterGet?: Array<DBafterHookFunction>
    afterPost?: Array<DBafterHookFunction>
    afterPatch?: Array<DBafterHookFunction>
    afterPut?: Array<DBafterHookFunction>
    afterDelete?: Array<DBafterHookFunction>
    params?: object
    options: DBOptions
    dataFiles: string
    collection: string
    createObjects: (collection: string, payload: ArrayOfDBDataObjects) => Promise<DBResponse>
    getObjects: (collection: string, query: any) => Promise<DBResponse>
    updateObjects: (collection: string, payload: ArrayOfDBDataObjects) => Promise<DBResponse>
    deleteObjects: (collection: string, payload: ArrayOfDBDataObjects) => Promise<DBResponse>
    patchObjects: (collection: string, payload: ArrayOfDBDataObjects) => Promise<DBResponse>
    executeDBAcommand: (payload: any) => Promise<any>
    pullOplog: (collection: string) => Promise<void>
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
    loading: boolean
    finishedLoading: boolean
}

export enum OperationType {
    CREATE = 'create',
    READ = 'read',
    UPDATE = 'update',
    DELETE = 'delete',
    PATCH = 'patch',
}
export interface Operation {
    operation: OperationType
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

export interface CrudFunctions {
    [key: string]: (operation: Operation, collectionId: string, db: DBConfig, length: number) => DBResponse
}
