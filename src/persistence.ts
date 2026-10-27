import { DBConfig } from './types'

import fs from 'fs-extra'
import path from 'path'
const highWaterMark = 64 * 1024
const OPLOG_FILE_EXISTS = 'OPLOG_FILE_EXISTS'

const writeFileSync = function (dirName: string, fileName: string, payload: string, db: DBConfig) {
    db.options.secondaryDataDirs?.forEach((rootDir) => {
        fs.ensureDirSync(path.join(rootDir, dirName))
        fs.writeFileSync(path.join(rootDir, dirName, fileName), payload, 'utf8')
    })
    fs.ensureDirSync(path.join(db.dataFiles, dirName))
    fs.writeFileSync(path.join(db.dataFiles, dirName, fileName), payload, 'utf8')
}
const writeFile = async function (dirName: string, fileName: string, payload: string, db: DBConfig) {
    for (const rootDir of db.options.secondaryDataDirs || []) {
        await fs.ensureDir(path.join(rootDir, dirName))
        await fs.writeFile(path.join(rootDir, dirName, fileName), payload, 'utf8')
    }
    fs.ensureDirSync(path.join(db.dataFiles, dirName))
    return await fs.writeFile(path.join(db.dataFiles, dirName, fileName), payload, 'utf8')
}
const unlinkWithRetry = async function (file: string) {
    let delay = 50
    for (let attempt = 0; ; attempt++) {
        try {
            return await fs.unlink(file)
        } catch (err) {
            if (err.code == 'ENOENT') return
            if (attempt >= 4 || !RETRYABLE_FS_ERRORS.includes(err.code)) throw err
            await sleep(delay)
            delay *= 2
        }
    }
}

/**
 * Open a file for writing, failing when it already exists (O_CREAT | O_EXCL).
 *
 * EEXIST is final: an existing file is never opened, so it is never truncated or overwritten.
 * EPERM/EBUSY/EACCES are retried because on Windows they are transient when an AV scanner/indexer
 * holds a handle, or when a previous file with this name is still 'delete pending'. Retrying is safe:
 * the exclusive open can never succeed on a file that exists.
 * If the error persists we give up (and the caller fails the request), we never fall back to overwriting.
 */
const openNewFile = async function (file: string): Promise<number> {
    let delay = 50
    for (let attempt = 0; ; attempt++) {
        try {
            return await fs.open(file, 'wx')
        } catch (err) {
            if (err.code == 'EEXIST') {
                const conflict: any = new Error(`Refusing to overwrite existing oplog file: ${file}`)
                conflict.code = OPLOG_FILE_EXISTS
                conflict.statusCode = 500
                throw conflict
            }
            if (attempt >= 4 || !RETRYABLE_FS_ERRORS.includes(err.code)) throw err
            await sleep(delay)
            delay *= 2
        }
    }
}

const createNewFile = async function (file: string, payload: string) {
    const fd = await openNewFile(file) // an error here means the file is not ours: never unlink it
    try {
        await fs.writeFile(fd, payload, 'utf8')
    } catch (err) {
        // The file is ours and incomplete. Remove it, a partial oplog file would poison this id.
        try {
            await fs.close(fd)
        } catch {}
        try {
            await unlinkWithRetry(file)
        } catch {}
        throw err
    }
    try {
        await fs.close(fd)
    } catch (err) {
        try {
            await unlinkWithRetry(file)
        } catch {}
        throw err
    }
}

/**
 * Create a new file in the data dir and all secondary dirs. Never overwrites an existing file.
 * The primary dir goes first: it is the one we read from, so it decides whether the id is free.
 * When anything fails, the files created by this call are removed again (and only those).
 * Throws an error with code OPLOG_FILE_EXISTS if the file already existed in one of the dirs.
 */
const writeNewFile = async function (dirName: string, fileName: string, payload: string, db: DBConfig) {
    const created: string[] = []
    try {
        for (const rootDir of [db.dataFiles, ...(db.options.secondaryDataDirs || [])]) {
            await fs.ensureDir(path.join(rootDir, dirName))
            const file = path.join(rootDir, dirName, fileName)
            await createNewFile(file, payload)
            created.push(file)
        }
    } catch (err) {
        for (const file of created) {
            try {
                await unlinkWithRetry(file)
            } catch {}
        }
        throw err
    }
}

const readFile = async function (dirName: string, fileName: string, db: DBConfig) {
    return await fs.readFile(path.join(db.dataFiles, dirName, fileName))
}
const readdir = async function (dirName: string, db: DBConfig) {
    try {
        return await fs.readdir(path.join(db.dataFiles, dirName))
    } catch (e) {
        return []
    }
}

function readdirRecursive(dirName: string) {
    const list: string[] = []
    function readdirRecursive2(dirName: string) {
        try {
            const entries = fs.readdirSync(dirName, { withFileTypes: true })
            const dirs = entries.filter((dir) => dir.isDirectory()).map((dir) => path.join(dirName, dir.name))

            if (dirs.length == 0) list.push(dirName)
            const subDirs = [] as any
            for (const dir of dirs) readdirRecursive2(dir)
        } catch {
            return
        }
    }
    readdirRecursive2(dirName)
    return list
}
const rename = function (oldFile: string, newFile: string, db: DBConfig) {
    db.options.secondaryDataDirs?.forEach((rootDir) => {
        fs.renameSync(path.join(rootDir, oldFile), path.join(rootDir, newFile))
    })
    return fs.renameSync(path.join(db.dataFiles, oldFile), path.join(db.dataFiles, newFile))
}
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))
// Windows can fail renames/deletes with transient sharing violations when another process (or an AV scanner) holds the file open.
const RETRYABLE_FS_ERRORS = ['EPERM', 'EBUSY', 'EACCES']
const renameWithRetry = async function (oldPath: string, newPath: string) {
    let delay = 50
    for (let attempt = 0; ; attempt++) {
        try {
            return await fs.rename(oldPath, newPath)
        } catch (err) {
            if (attempt >= 4 || !RETRYABLE_FS_ERRORS.includes(err.code)) throw err
            await sleep(delay)
            delay *= 2
        }
    }
}
/**
 * Atomically replace fileName with tmpFile (both relative to the data root(s)).
 * The target file never ceases to exist: it either holds its old or its new content.
 */
const replaceFile = async function (tmpFile: string, fileName: string, db: DBConfig) {
    for (const rootDir of db.options.secondaryDataDirs || []) {
        try {
            await renameWithRetry(path.join(rootDir, tmpFile), path.join(rootDir, fileName))
        } catch (err) {
            // Not all file types are mirrored to secondary dirs (e.g. ndjson checkpoints)
            if (err.code != 'ENOENT') throw err
        }
    }
    return await renameWithRetry(path.join(db.dataFiles, tmpFile), path.join(db.dataFiles, fileName))
}
const copyFile = async function (srcFile: string, destFile: string, db: DBConfig) {
    for (const rootDir of db.options.secondaryDataDirs || []) {
        try {
            await fs.copyFile(path.join(rootDir, srcFile), path.join(rootDir, destFile))
        } catch (e) {
            // Don't panic
        }
    }
    return await fs.copyFile(path.join(db.dataFiles, srcFile), path.join(db.dataFiles, destFile))
}
const archive = async function (fileName: string, db: DBConfig) {
    const archiveDir = db.options.opLogArchive!
    db.options.secondaryDataDirs?.forEach((rootDir) => {
        try {
            fs.unlinkSync(path.join(rootDir, fileName))
        } catch (e) {
            // Don't panic
        }
    })
    try {
        await fs.move(path.join(db.dataFiles, fileName), path.join(archiveDir, fileName), { overwrite: true })
    } catch (e) {
        // Don't panic
    }
    return
}
const remove = async function (fileName: string, db: DBConfig) {
    for (const rootDir of db.options.secondaryDataDirs || []) {
        try {
            await fs.unlink(path.join(rootDir, fileName))
        } catch (e) {
            // Don't panic
        }
    }
    try {
        await fs.unlink(path.join(db.dataFiles, fileName))
    } catch (e) {
        // Don't panic
    }
    return
}
const rmdirSync = function (dirName: string, db: DBConfig) {
    db.options.secondaryDataDirs?.forEach((rootDir) => {
        try {
            fs.rmSync(path.join(rootDir, dirName), { recursive: true })
        } catch (err) {
            if (err.code == 'ENOENT') return true // ignore
            return false
        }
    })
    try {
        fs.rmSync(path.join(db.dataFiles, dirName), { recursive: true })
    } catch (err) {
        if (err.code == 'ENOENT') return true // ignore
        console.log(err)
        return false
    }
    return true
}
const existsSync = function (fileName: string, db: DBConfig) {
    return fs.existsSync(path.join(db.dataFiles, fileName))
}
const fileStats = async function (fileName: string, db) {
    try {
        const stat = await fs.stat(path.join(db.dataFiles, fileName))
        return { birthtimeMs: stat.birthtimeMs, size: stat.size }
    } catch {
        return { birthtimeMs: 0, size: 0 }
    }
}
/**
 * find the last file in an opLog folder.
 */
const mostRecentFile = async function (dirName: string, db: DBConfig) {
    const files = await readdir(dirName, db)
    let max = -1
    for (const file of files) {
        const fileNr = parseInt(file)
        if (fileNr > max) max = fileNr
    }
    return max
}

function drain(writer) {
    return new Promise((resolve) => writer.once('drain', resolve))
}

async function writeCheckpointToStream(metaData: any, json: any, dirName: string, fileName: string, db: DBConfig) {
    fs.ensureDirSync(path.join(db.dataFiles, dirName))

    // 'w' (not 'a'): a leftover file from a previously failed write must not be appended to
    const writer = fs.createWriteStream(path.join(db.dataFiles, dirName, fileName), { highWaterMark: highWaterMark, flags: 'w' })
    const finished = new Promise<void>((resolve, reject) => {
        writer.once('error', reject)
        writer.once('finish', () => resolve())
    })
    const writeOrDrain = async function (payload: string) {
        if (!writer.write(payload)) await Promise.race([drain(writer), finished])
    }
    await writeOrDrain(JSON.stringify(metaData) + '\n')
    if (Array.isArray(json)) {
        for (const obj of json) await writeOrDrain(JSON.stringify(obj) + '\n')
    } else {
        for (const obj of Object.values(json)) await writeOrDrain(JSON.stringify(obj) + '\n')
    }
    writer.end()
    await finished
}

async function readCheckpointFromStream(dirName: string, fileName: string, db: DBConfig): Promise<any> {
    return new Promise((resolve, reject) => {
        const newObject = {} as any
        let buf = ''
        let totalBytes = 0
        let reader
        try {
            reader = fs.createReadStream(path.join(db.dataFiles, dirName, fileName), { highWaterMark: highWaterMark })
        } catch (err) {
            resolve({})
            return
        }
        reader.on('data', function (chunk) {
            buf += chunk
            totalBytes += chunk.length
            do {
                const newLinePos = buf.indexOf('\n')
                if (newLinePos == -1) break
                const line = buf.substring(0, newLinePos)
                buf = buf.substring(newLinePos + 1)

                if (!line) continue
                const obj = JSON.parse(line)
                if (!newObject.data) Object.assign(newObject, obj)
                else {
                    if (Array.isArray(newObject.data)) newObject.data.push(obj)
                    else newObject.data[obj.id] = obj
                }
            } while (true)
        })
        reader.on('end', function () {
            newObject.totalBytes = totalBytes
            resolve(newObject)
        })
        reader.on('error', (err) => {
            if (err.code == 'ENOENT') resolve({})
            reject()
        })
    })
}
export default {
    archive,
    copyFile,
    existsSync,
    fileStats,
    mostRecentFile,
    OPLOG_FILE_EXISTS,
    readdir,
    readdirRecursive,
    readFile,
    rename,
    replaceFile,
    remove,
    rmdirSync,
    writeFile,
    writeFileSync,
    writeNewFile,
    writeCheckpointToStream,
    readCheckpointFromStream,
}
