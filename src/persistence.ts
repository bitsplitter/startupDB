import { DBConfig } from './types'

import fs from 'fs-extra'
import path from 'path'

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
const archive = function (fileName: string, db: DBConfig) {
    const archiveDir = db.options.opLogArchive!
    db.options.secondaryDataDirs?.forEach((rootDir) => {
        try {
            fs.unlinkSync(path.join(rootDir, fileName))
        } catch (e) {
            // Don't panic
        }
    })
    try {
        fs.moveSync(path.join(db.dataFiles, fileName), path.join(archiveDir, fileName), { overwrite: true })
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
const fileTimestampSync = function (fileName: string, db) {
    try {
        return fs.statSync(path.join(db.dataFiles, fileName))?.mtimeMs
    } catch {
        return 0
    }
}
/**
 * find the last file in an opLog folder.
 */
const mostRecentFile = async function (dirName: string, db: DBConfig) {
    const files = (await readdir(dirName, db)).map((file) => parseInt(file)).sort((a, b) => a - b)
    const nrFiles = files.length
    return files[nrFiles - 1]
}
export default {
    archive,
    existsSync,
    fileTimestampSync,
    mostRecentFile,
    readdir,
    readdirRecursive,
    readFile,
    rename,
    rmdirSync,
    writeFile,
    writeFileSync,
}
