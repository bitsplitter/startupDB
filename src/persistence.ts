import { DBConfig } from './types'
const { PassThrough } = require('stream')
import { pipeline, Transform } from 'stream'
import { TextDecoder } from 'util'

import fs from 'fs-extra'
import path from 'path'
const highWaterMark = 64 * 1024

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
const rename = function (newFile: string, oldFile: string, db: DBConfig) {
    let x
    try {
        db.options.secondaryDataDirs?.forEach((rootDir) => {
            fs.renameSync(path.join(rootDir, newFile), path.join(rootDir, oldFile))
        })
        x = fs.renameSync(path.join(db.dataFiles, newFile), path.join(db.dataFiles, oldFile))
    } catch (e) {
        console.log('@ERROR', e)
    }
    return x
}
const archive = async function (fileName: string, archiveFileName: string, db: DBConfig) {
    const archiveDir = db.options.opLogArchive!
    db.options.secondaryDataDirs?.forEach((rootDir) => {
        try {
            fs.unlinkSync(path.join(rootDir, fileName))
        } catch (e) {
            // Don't panic
        }
    })
    try {
        fs.ensureFileSync(path.join(archiveDir, archiveFileName))
        await fs.move(path.join(db.dataFiles, fileName), path.join(archiveDir, archiveFileName), { overwrite: true })
    } catch (e) {
        console.log(e)
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

function drain(writer) {
    return new Promise((resolve) => writer.once('drain', resolve))
}

async function writeCheckpointToStream(metaData: any, json: any, dirName: string, fileName: string, db: DBConfig) {
    try {
        db.options.secondaryDataDirs?.forEach((rootDir) => {
            fs.ensureDirSync(path.join(rootDir, dirName))
        })
        fs.ensureDirSync(path.join(db.dataFiles, dirName))
        const passthroughStream = new PassThrough()

        const secondaryFileNames = db.options.secondaryDataDirs?.map((rootDir) => path.join(rootDir, dirName, fileName))
        const writer = fs.createWriteStream(path.join(db.dataFiles, dirName, fileName), { highWaterMark: highWaterMark, flags: 'w' })
        const secondaryStreams = secondaryFileNames?.map((fileName) => fs.createWriteStream(fileName))
        passthroughStream.pipe(writer)

        secondaryStreams?.forEach((stream) => {
            passthroughStream.pipe(stream)
        })
        if (!passthroughStream.write(JSON.stringify(metaData) + '\n')) await drain(passthroughStream)
        if (Array.isArray(json)) {
            for (const obj of json) if (!passthroughStream.write(JSON.stringify(obj) + '\n')) await drain(passthroughStream)
        } else {
            for (const obj of Object.values(json)) if (!passthroughStream.write(JSON.stringify(obj) + '\n')) await drain(passthroughStream)
        }
        writer.end()
        passthroughStream.end()
        secondaryStreams?.forEach((stream) => {
            stream.end()
        })
    } catch (err) {
        console.log(err)
    }
}

async function readCheckpointFromStream(dirName: string, fileName: string, db: DBConfig): Promise<any> {
    const newObject = {} as any
    let totalBytes = 0

    const processLine = (obj: any, length: number) => {
        totalBytes += length
        if (!newObject.data) Object.assign(newObject, obj)
        else {
            if (Array.isArray(newObject.data)) newObject.data.push(obj)
            else newObject.data[obj.id] = obj
        }
    }

    try {
        await processOplog(dirName, fileName, db, 0, processLine)
        newObject.totalBytes = totalBytes
        return newObject
    } catch (err) {
        return {} // Return an empty object in case of error
    }
}

const BUFFER_POOL_SIZE = 1024 * 64 // 64KB reusable pool
const pooledBuffer = new Uint8Array(BUFFER_POOL_SIZE)

const processOplog = async (dirName: string, fileName: string, db: DBConfig, offset: number, func: (operation: any, length: number) => void): Promise<void> => {
    return new Promise((resolve, reject) => {
        let bufferLength = 0

        const decodeUtf8Stream = new Transform({
            readableObjectMode: true,
            transform(chunk, encoding, callback) {
                const chunkArray = new Uint8Array(chunk)

                // Ensure buffer has enough capacity
                if (bufferLength + chunkArray.length > pooledBuffer.length) {
                    callback(new Error('Line buffer overflow'))
                    return
                }

                pooledBuffer.set(chunkArray, bufferLength)
                bufferLength += chunkArray.length

                let start = 0
                for (let i = 0; i < bufferLength; i++) {
                    if (pooledBuffer[i] === 10) {
                        // '\n' character
                        const lineBytes = pooledBuffer.slice(start, i)
                        this.push(lineBytes)
                        start = i + 1
                    }
                }

                // Move remaining bytes to beginning of buffer
                if (start < bufferLength) {
                    pooledBuffer.copyWithin(0, start, bufferLength)
                    bufferLength = bufferLength - start
                } else {
                    bufferLength = 0
                }

                callback()
            },
            flush(callback) {
                if (bufferLength > 0) {
                    this.push(pooledBuffer.slice(0, bufferLength))
                }
                callback()
            },
        })

        const sharedDecoder = new TextDecoder('utf-8')

        const splitLines = new Transform({
            readableObjectMode: true,
            writableObjectMode: true,
            transform(chunk: Uint8Array, encoding, callback) {
                // Streaming decode ensures multibyte characters split across chunks are preserved
                const text = sharedDecoder.decode(chunk, { stream: true })
                callback(null, text)
            },
            flush(callback) {
                const final = sharedDecoder.decode()
                if (final) this.push(final)
                callback()
            },
        })

        const inputStream = fs.createReadStream(path.join(db.dataFiles, dirName, fileName), {
            highWaterMark,
            start: offset,
        })

        pipeline(inputStream, decodeUtf8Stream, splitLines, (err) => {
            if (err) {
                if ((err as NodeJS.ErrnoException).code === 'ENOENT') resolve()
                else reject(err)
            } else {
                resolve()
            }
        })

        splitLines.on('data', (line: string) => {
            if (line) {
                try {
                    const obj = JSON.parse(line)
                    func(obj, line.length)
                } catch {
                    func({}, 0)
                }
            }
        })
    })
}

const appendFile = async function (dirName: string, fileName: string, payload: string, db: DBConfig) {
    if (db.options.secondaryDataDirs)
        for await (const rootDir of db.options.secondaryDataDirs) {
            fs.ensureDirSync(path.join(rootDir, dirName))
            await fs.appendFile(path.join(rootDir, dirName, fileName), payload, 'utf8')
        }
    fs.ensureDirSync(path.join(db.dataFiles, dirName))
    return await fs.appendFile(path.join(db.dataFiles, dirName, fileName), payload, 'utf8')
}
export default {
    archive,
    appendFile,
    existsSync,
    fileStats,
    processOplog,
    readdirRecursive,
    rename,
    rmdirSync,
    writeCheckpointToStream,
    readCheckpointFromStream,
}
