import { DBConfig } from './types'

import fs from 'fs-extra'
import path from 'path'
const highWaterMark = 64 * 1024

const writeFileSync = function (dirName: string, fileName: string, payload: string, db: DBConfig) {
    db.options.secondaryDataDirs?.forEach((rootDir) => {
        fs.ensureDirSync(path.join(rootDir, dirName))
        fs.writeFileSync(path.join(rootDir, dirName, fileName), payload, 'utf8')
    })
    fs.ensureDirSync(path.join(db.dataFiles, dirName))
    fs.writeFileSync(path.join(db.dataFiles, dirName, fileName), payload, 'utf8')
}
const writeFile = async function (dirName: string, fileName: string, payload: string, db: DBConfig) {
    db.options.secondaryDataDirs?.forEach((rootDir) => {
        fs.ensureDirSync(path.join(rootDir, dirName))
        fs.writeFile(path.join(rootDir, dirName, fileName), payload, 'utf8')
    })
    fs.ensureDirSync(path.join(db.dataFiles, dirName))
    return await fs.writeFile(path.join(db.dataFiles, dirName, fileName), payload, 'utf8')
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
const rename = function (newFile: string, oldFile: string, db: DBConfig) {
    db.options.secondaryDataDirs?.forEach((rootDir) => {
        fs.renameSync(path.join(rootDir, newFile), path.join(rootDir, oldFile))
    })
    return fs.renameSync(path.join(db.dataFiles, newFile), path.join(db.dataFiles, oldFile))
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
    db.options.secondaryDataDirs?.forEach((rootDir) => {
        try {
            fs.unlinkSync(path.join(rootDir, fileName))
        } catch (e) {
            // Don't panic
        }
    })
    try {
        await fs.unlinkSync(path.join(db.dataFiles, fileName))
    } catch (e) {
        // Don't panic
    }
    return
}
const rmdirSync = function (dirName: string, db: DBConfig) {
    db.options.secondaryDataDirs?.forEach((rootDir) => {
        try {
            fs.rmSync(path.join(rootDir, dirName), { recursive: true })
        } catch (err) {}
    })
    try {
        fs.rmSync(path.join(db.dataFiles, dirName), { recursive: true })
    } catch (err) {
        // return false
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
    try {
        fs.ensureDirSync(path.join(db.dataFiles, dirName))

        const writer = fs.createWriteStream(path.join(db.dataFiles, dirName, fileName), { highWaterMark: highWaterMark, flags: 'a' })
        if (!writer.write(JSON.stringify(metaData) + '\n')) await drain(writer)
        if (Array.isArray(json)) {
            for (const obj of json) if (!writer.write(JSON.stringify(obj) + '\n')) await drain(writer)
        } else {
            for (const obj of Object.values(json)) if (!writer.write(JSON.stringify(obj) + '\n')) await drain(writer)
        }
        writer.end()
    } catch (err) {
        console.log(err)
    }
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
    existsSync,
    fileStats,
    mostRecentFile,
    readdir,
    readdirRecursive,
    readFile,
    rename,
    remove,
    rmdirSync,
    writeFile,
    writeFileSync,
    writeCheckpointToStream,
    readCheckpointFromStream,
}
