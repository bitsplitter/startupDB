import { MutexInterface } from 'async-mutex'

function deepCopy<Type>(obj: Type): Type {
    return <Type>JSON.parse(JSON.stringify(obj))
}
const EMPTY_COLLECTION = {
    checkPoint: 0,
    savedAt: 0,
    lock: <MutexInterface>{},
    data: {},
    options: {},
    lastAccessed: 0,
    lastModified: 0,
    length: 0,
    opLogSize: 0,
    loading: false,
    finishedLoading: false,
}
/*
 * Ensure that an object is an array. If so, return object, else return a one-element array with the object
 */
function ensureArray(object: any): Array<any> {
    if (!Array.isArray(object)) return [object]
    return object
}

function yyyymmddhhmmss_ms(date): string {
    function pad2(n) {
        return (n < 10 ? '0' : '') + n
    }
    return (
        date.getFullYear() +
        pad2(date.getMonth() + 1) +
        pad2(date.getDate()) +
        pad2(date.getHours()) +
        pad2(date.getMinutes()) +
        pad2(date.getSeconds()) +
        '_' +
        date.getMilliseconds()
    )
}
export default {
    deepCopy,
    EMPTY_COLLECTION,
    ensureArray,
    yyyymmddhhmmss_ms,
}
