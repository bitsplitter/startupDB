# startupDB

startupDB is a database designed to create REST APIs. It is implemented as an Express middleware function and allows for easy implementation of persistent data endpoints. It features protection from dataloss during hardware failure by persisting individual operations in JSON files and offers high performance by serving all data from memory.

Its CRUD operations map directly to POST, GET, UPDATE/PUT/PATCH and DELETE methods.

## Usage

Example

```
const express = require('express')
const startupDB = require('./dist/server.js')

const app = express()
app.use(express.json())
app.use('/myDB', [startupDB.db({ dataFiles: './resources' })])
app.listen(3000)
```

This will create a database under the `myDB` directory. Every endpoint that starts with `/myDB` will translate to a collection with the same name. So `localhost:3000/myDB/user` will implement POST, GET, PUT, DELETE and PATCH endpoints to create, find, update, delete and change user documents. Data will be persisted to disk in `checkpoint/user` and `oplog/user` directories.

## Methods

StartupDB implements the following methods on all endpoints:

### GET

The `GET` method retrieved data from the database. Retrieving data from a non existing collection will result in a `200 OK` and an empty response, it will not return a `400` error.

### no parameters

`GET localhost:3000/myDB/user` will return all documents in the collection.

### id parameter

`GET localhost:3000/myDB/user?id=peter` will return the document with `id == 'peter'`.

### filter parameter

`GET localhost:3000/myDB/user?filter=lastname=="Smith"` will return all documents with `lastName == 'Smith'`.

The filter parameter supports sandboxed javascript expressions as implemented by [filtrex](https://www.npmjs.com/package/filtrex).

### offset and limit parameters

`GET localhost:3000/myDB/user?offset=10&limit=10` will return documents 11 - 20.

### returnType parameter

returnType parameter can be **object**, **checkPoint** or **array** (default)

`GET localhost:3000/myDB/user?returnType=object` will return all documents as an object using the **id** field as a key.

`GET localhost:3000/myDB/user?returnType=checkpoint` will return all documents as stored in the checkPoint including metadata. The nextOplogId in the metadata can be used for oplog polling.

### POST

The `POST` method adds new documents to the database. POSTing data to a non existing collection will create the collection. The body can contain one object or an array of objects. If the objects have no **id** property, one will be added to each document containing a version 4 UUID string.

If a document is POSTed with an **id** that already exists in the collection, a `409 conflict` error will be returned. To update an existing document, use the `PUT` or `PATCH` methods.

### PUT

The `PUT` method replaces existing documents or created new documents to the database. PUTing data to a non existing collection will create the collection. The body can contain one object or an array of objects. If the objects have no **id** property, one will be added to each document containing a version 4 UUID string. If a document exists in the collection with an **id** mentioned in the body of the `PUT`, the document will be replaced with the new document.

### DELETE

The `DELETE` method removes documents from the database.

### id parameter

`DELETE localhost:3000/myDB/user?id=peter` will delete the document with `id == 'peter'`.

### filter parameter

`DELETE localhost:3000/myDB/user?filter=lastname=="Smith"` will delete all documents with `lastName == 'Smith'`.

The filter parameter supports sandboxed javascript expressions as implemented by [filtrex](https://www.npmjs.com/package/filtrex).

### PATCH

The `PATCH` method updates documents in the database. The body can contain one object or an array of objects. If the objects have no **id** property, one will be added to each document containing a version 4 UUID string.

#### jsonpatch

PATCHes can be performed by [jsonpatch](https://jsonpatch.com/). This allows for lightweight, finegrained updates on large objects. To use **jsonpatch** the objects in the body should follow this schema:

```
{
    "id":string
    "patch":array
}
```

#### Object.assign

If the object has any other schema, the PATCH will be performed by javascript [Object.assign](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object/assign)

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
| serveRawCheckpoint | boolean | false| Stream checkpoint to client, does not keep resource in memory |
| streamObjects | boolean | false| Stream json repsonse to client, does not block the event loop even for large payloads |

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

This function will be called when documents are created or modified. The timestamp function wil be called before your documents will be validated so make sure your schema includes your timestamps when you use te optional schema validation.

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

### Route parameters in hooks

To define hooks for a specific route, one would add a middleware function before the catchall route like so:

```
app.use('/data/brands/:brandId/inventory', [beforeGetBrandInventory])
app.use('/data', [db])
```

The `beforeGetBrandInventory` 'hook' can be defined like this:

```
export default startupDB.beforeAll(async (req: LcRequestI, response: Response): Promise<object> => {}
```

In order to access named route parameters, use req.startupDB.params instead of req.params. This is because your hoo' code will effectively run from within the 'db' module which will have parse the catchall route instead of the route you defined the hook for. req.startupDB.params gathers params from the rout that the hook was defined for.

## Commands

startupDB supports several commands that can be executed by sending a POST or GET request to the root.

For example:

```
curl --header "Content-Type: application/json" \
  --request POST \
  --data "{\"command\":\"purgeOplog\",\"collection\":\"00000/sku\"}" \
  http://127.0.0.1:3000/data
```

| Command          | Method | Function                                                                                                                                                                                                                            | Parameters                                                                                  |
| ---------------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
|                  | GET    | List all collections.                                                                                                                                                                                                               |                                                                                             |
| create           | POST   | Create collection, errors when it already exists.                                                                                                                                                                                   | collection:"string", storageType:"array" or "object"                                        |
| drop             | POST   | Removes a collection from memory, oplog and checkpoint directories.                                                                                                                                                                 | collection:"string"                                                                         |
| ensureCollection | POST   | Create collection if it does not exist, no error if it does.                                                                                                                                                                        | collection:"string", storageType:"array" or "object"                                        |
| flush            | POST   | Create checkpoint and flush oplog.                                                                                                                                                                                                  | collection:"string"<br>options:{archive:true/false} mandatory when **opLogArchive** is used |
| inspect          | POST   | return totalNrObjects in memory.                                                                                                                                                                                                    |                                                                                             |
| purgeOplog       | POST   | remove all operations from opLog, restoring collection to previous checkpoint.<br>This is usefull for implementing tests. <br>Collection parameter can be "\*" to purge all collections or a comma separated string of collections. | collection:"string"                                                                         |
|                  |        |                                                                                                                                                                                                                                     |                                                                                             |
