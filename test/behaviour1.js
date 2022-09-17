/*
This test suite tests the implementation of a few edge cases that require specifice db configuration. 
*/
const request = require("supertest")
const assert = require("assert")
const express = require("express")
const app = express()
const startupDB = require("../dist/server.js")

try {
    fs.rmSync("./archive", { recursive: true })
} catch (e) { }
try {
    fs.rmSync("./leesplank", { recursive: true })
} catch (e) { }
try {
    fs.rmSync("./backup1", { recursive: true })
} catch (e) { }
try {
    fs.rmSync("./backup2", { recursive: true })
} catch (e) { }

app.use(express.json({ inflate: true, limit: "100mb" }))

app.use("/leesplank", startupDB.db({ readOnly: true }))
const server = app.listen(3456)

describe("Behaviour: PUT command", function () {
    it("executing a PUT command on a readonly database shoud return an error", function (done) {
        request(app)
            .put("/leesplank")
            .set('Content-type', 'application/json')
            .send([
                { "id": "Wim", "description": "Een broer." },
                { "id": "Zus", "description": "Een baby." },
                { "id": "Jet", "description": "Een zus." }
            ])
            .expect(403)
            .end(done)
    })
})


server.close()
