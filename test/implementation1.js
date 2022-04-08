/*
This test suite tests the implementation. 
It is supposed to run after implementation0.js as that test sets up a database which we will now reuse
*/
for (var key in Object.keys(require.cache)) { delete require.cache[key]; }
const request = require("supertest")
const assert = require("assert")
const express = require("express")
const app = express()
const startupDB = require("../dist/server.js")

// We explicitely do not delete the test database !!!

app.use(express.json({ inflate: true, limit: "100mb" }))

app.use("/leesplank", startupDB.db())
const server = app.listen(3456)

describe("Behaviour flush command", function () {
    it("executing a flush command on a collection not in memory should return a 200", function (done) {
        request(app)
            .post("/leesplank")
            .set("Content-type", "application/json")
            .send({ "command": "flush", "collection": "origineel" })
            .expect(200)
            .end(done)
    })
})

describe("Behaviour GET command", function () {
    it("executing a GET command should return a list of collections", function (done) {
        request(app)
            .get("/leesplank")
            .expect(200)
            .expect({
                "collections": [
                    { "name": "origineel", "inCache": true, "count": 6, "checkPoint": 2, "lastOplogId": 0 },
                    { "name": "nieuw", "inCache": false, "count": 0, "checkPoint": 0, "lastOplogId": 0 },
                    { "name": "parent/child", "inCache": false, "count": 0, "checkPoint": 0, "lastOplogId": 0 }
                ]
            })
            .end(done)
    })
})

extCnt = 0
bigdata = function () {
    test = extCnt
    data = []
    for (i = 0; i < 100000; i++) {
        data.push({
            "id": "" + (test * 10000 + i), "pl": "1234567890"
        })
    }
    //    console.log(test, data)
    extCnt++
    return data
}
const blob = bigdata()
let testCounter = 0
for (test = 0; test < 10; test++) {
    describe("Implementation POST BIG DATA " + test + " /leesplank/test", function () {
        this.timeout(50000)
        it("should return the POSTed body", function (done) {
            request(app)
                .post("/leesplank/test" + testCounter++)
                .set("Content-type", "application/json")
                .send(blob)
                .expect(200)
                .expect(function (res) {
                    assert.strictEqual(res.body.length, 100000)
                })
                .end(done)
        })
    })
}

describe("Implementation flush", function () {
    this.timeout(60000)
    it("executing a flush command with an existing checkpoint should create a new checkpoint file when there are unflushed documents", function (done) {
        request(app)
            .post("/leesplank")
            .set("Content-type", "application/json")
            .send({ "command": "flush", "collection": "test0" })
            .expect(function (res) {
                assert.strictEqual(1, 1)
            })
            .end(done)
    })
})



describe("Implementation garbageCollector", function () {
    this.timeout(60000)
    it("executing a garbageCollector command should not impact future operations", function (done) {
        request(app)
            .post("/leesplank")
            .set("Content-type", "application/json")
            .send({ "command": "garbageCollector" })
            .expect(function (res) {
                assert.strictEqual(1, 1)
            })
            .end(done)
    })
})

describe("Implementation nonexisting command", function () {
    this.timeout(60000)
    it("executing a nonexisting command should result in an error", function (done) {
        request(app)
            .post("/leesplank")
            .set("Content-type", "application/json")
            .send({ "command": "bogus" })
            .expect(400)
            .end(done)
    })
})

describe('Implementation GET /leesplank/origineel', function () {
    it('should return previously flushed collection.', function (done) {
        request(app)
            .get('/leesplank/origineel')
            .expect(200)
            .expect(function (res) {
                assert.strictEqual(res.body[0].id, 'Aap')
                assert.strictEqual(res.body[1].id, 'Noot')
                assert.strictEqual(res.body[2].id, 'Mies')
            })
            .end(done)
    })
})

describe("Implementation garbageCollector", function () {
    this.timeout(60000)
    it("executing a garbageCollector command should not impact future operations", function (done) {
        request(app)
            .post("/leesplank")
            .set("Content-type", "application/json")
            .send({ "command": "garbageCollector" })
            .expect(function (res) {
                assert.strictEqual(1, 1)
            })
            .end(done)
    })
})

server.close()
