import { MutexInterface } from 'async-mutex'

function deepCopy<Type>(obj: Type): Type {
    return <Type>structuredClone(obj)
}
const EMPTY_COLLECTION = {
    checkPoint: 0,
    nextOpLogId: 1,
    savedAt: 0,
    lock: <MutexInterface>{},
    data: {},
    options: {},
    lastAccessed: 0,
    lastModified: 0,
    length: 0,
}
/*
 * Ensure that an object is an array. If so, return object, else return a one-element array with the object
 */
function ensureArray(object: any): Array<any> {
    if (!Array.isArray(object)) return [object]
    return object
}

export default {
    deepCopy,
    EMPTY_COLLECTION,
    ensureArray,
}
