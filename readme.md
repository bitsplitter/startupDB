# startupDB

startupDB is a database designed to create REST APIs. It is implemented as an Express middleware function and allows for easy implementation of persistent data endpoints. It features protection from dataloss during hardware failure by persisting individual operations in JSON files and offers high performance by serving all data from memory.

Its CRUD operations map directly to POST, GET, UPDATE/PATCH and DELETE methods.

## Usage

Example
```
const express = require('express')
const startupDB = require('startupDB')

const app = express()
app.use("/myDB", startupDB.db)
const server = app.listen(3000)
```
This will create a database under the `myDB` directory. Every endpoint that starts with `/myDB` will translate to a collection with the same name. So `localhost:3000/myDB/user` will implement POST, GET, PUT, DELETE and PATCH endpoints to create, find, update, delete and change user documents. Data will be persisted to disk in `checkpoint/user` and `oplog/user` directories.

## API

```
const startupDB = require('startupDB')
```

### startupDB([options])
Returns the database middleware using the given options

### Options

The options object allows you to configure the following options:
| Option Name | Type | Default value | Description |
|-|-|-|-|
| dataFiles | string | './' | Path to data directory |
| validator | function | undefined | Function to validate schema |
| addTimeStamps | function | undefined | Function to add create/modified timestamps. |
| opLogArchive | string | undefined| Path to archive directory |

#### Schema validation

A schema validator can be passed using the `options.validator` function.

Your function should implement the following interface:
```
/*
 * @param {string} operation: "created" or "modified"
 * @param {object} document: the document to change
 * @return false | array: false or an array with error messages
 */
validator(collection, documents)
```

#### Timestamps

startupDB can auto-timestamp your documents using the `options.addTimeStamps` function. 

This function will be called when documents are created or modified. The timestamp function wil be called before your documents will be validated so make sure your schema understand your timestamps.

Your function should implement the following interface:
```
/*
 * @param {string} operation: "created" or "modified"
 * @param {object} document: the document to change
 * @param {object} oldDocument: the old document (before modify)
 */
function(operation,document,oldDocument)
```

Example

```
function (operation, object, oldObject) {
        if (operation == "created") object.__created = new Date().getTime()
        if (operation == "modified") {
            object.__modified = new Date().getTime()
            if (oldObject) object.__created = oldObject.__created
        }
    }
```

## Hooks

startupDB support databasehooks to run endpoint specific code either before or after the CRUD operation. They can be used for everything from authentication to data conversion.

A 'before' hook should implement the following interface:
```
/*
 * @param {object} req: like in Express
 * @param {object} res: like in Express
 * @param {function} next: like in Express
 * @param {string} collection: the name of the collection
 *
 * @return:  {"statusCode":<HTTP StatusCode>,"data":<response body>,"message":<status message>}
 *
 * return {"statusCode":0} when there are no errors
 *
 */
function(req, res, next){
    return {
        "statusCode":200,
        "data":{
            "name":"value"
        },
        "message":"OK"
        }
}
```

An 'after' hook should implement the following interface:
```
/*
 * @param {object} req: like in Express
 * @param {object} response: response object from database
 *
 * @return:  {"error":<HTTP StatusCode>,"data":<response body>,"message":<status message>,"headers":<response headers>}
 *
 * Omit the error property in the response when there are no errors
 */
function(req, response){
    return {
        "data":response.data
        }
}
```
## Commands

startupDB supports several commands that can be executed by sending a POST or GET request to the root.

For example:
```
curl --header "Content-Type: application/json" \
  --request POST \
  --data "{\"command\":\"purgeOplog\",\"collection\":\"00000/sku\"}" \
  http://127.0.0.1:3000/data
```

| Command | Method | Function | Parameters |
|-|-|-|-|
|  | GET | List all collections. | |
| create | POST | Create collection, errors when it already exists. | collection:"string", storageType:"array" or "object" |
| drop | POST | Removes a collection from memory, oplog and checkpoint directories. | collection:"string" |
| ensureCollection | POST | Create collection if it does not exist, no error if it does. | collection:"string", storageType:"array" or "object" |
| flush | POST | Create checkpoint and flush oplog. | collection:"string" |
| inspect | POST | return totalNrObjects in memory. | |
| purgeOplog | POST | remove all operations from opLog, restoring collection to previous checkpoint. This is usefull for implementating tests. | collection:"string" |
