# StartupDB 1.0.43: Streaming response

### Why

Serving a large payload involves JSON.stringify which blocks the event loop.

# StartupDB 2.0: NDJSON checkpoint

### Why

Reading a checkpoint involves JSON.parse which blocks the event loop too long.
Reading an NDJSON checkpoint will never block the event loop for any substantial amount of time because it will call JSON.parse on small strings.

-   1st line contains metadata.

            {
            options: {storageType: string <array | object>},
            lastAccessed: unix timestamp <last accessed in memory>,
            lastModified: unix timestamp <last modified in memory>,
            data: object | array <will hold data in memory>,
            checkPointTime: unix timestamp <last time checkPoint was saved>,
            checkPointSize: number <size in bytes>,
            nextOpLogId: number <id for next operation>,
            savedAt: string <HR checkpointTime>,
            dbEngine: "2.0"
            }

-   All other lines are objects
-   When reading a checkpoint file:

    -   parse individual json lines
    -   for array type collections: push each object to the array
    -   for object type collections: assign each object to it's associated id in the cache.

        Skip objects without an id

-   opLog processing remains unchanged
-   HEAD should:
    -   **Content-type (json | ndjson)**
    -   For ndjson return
        -   **'x-last-checkpoint-time': checkpoint file creation timestamp**
        -   'x-last-oplog-id': highest oplog filenumber
-   **FLUSH** command should:
    -   Get additional parameter: Content-type: <json | ndjson>
    -   When **ndjson** it should create an **ndjson** checkpoint
-   Client.ts
    -   Match request with content-type from HEAD
-   Client.ts JSON:
    -   No change
-   Client.js NDJSON:
    -   parse individual json line
    -   assign each object to it's associated id in the cache
    -   opLog processing remains unchanged

How to make this backwards compatible?

-   Filename latest.ndjson signifies this new paradigm as returned by HEAD as Content-type
-   Filename lastest.json should behave as is

### Conversion from 1.x to 2.0

-   Use of NDJSON checkpoints is optional. Create NDJSON checkpoints with the new Content-type parameter in the dba **FLUSH** command.

# StartupDB 2.5: NDJSON oplog

-   Changes metadata record:

    -   remove nexOpLogId
    -   change dbEngine to "2.5"

            {
            options: {storageType: string <array | object>},
            lastAccessed: unix timestamp <last accessed in memory>,
            lastModified: unix timestamp <last modified in memory>,
            data: object | array <will hold data in memory>,
            checkPointTime: unix timestamp <last time checkPoint was saved>,
            checkPointSize: number <size in bytes>,
            savedAt: string <HR checkpointTime>,
            dbEngine: "2.5"
            }

-   HEAD should:
    -   **Content-type (json | ndjson)**
    -   For **ndjson** :
        -   'x-last-checkpoint-time': checkpoint file creation timestamp
        -   'x-last-oplog-id': oplog file size **_( 'x-last-oplog-id': fsStats.size || -1)_**
-   oplog folder:

    -   opLog is stored in 1 file: latest.json.
    -   There is no need for a header.
    -   opLogId now becomes a byte offset in the oplog file

-   Operations:

    -   DELETE and PATCH operation no longer store the old data.
    -   Append operation to single oplog file, do not write every operation to it's own file

-   processOplog:
    -   parse individual json line
    -   if it is an object without a $operation property: do same as checkpoint
    -   if it is an object with $operation and $payload properties =>
        -   $operation == ‘delete’ delete $payload.id from cache
        -   $operation == ‘patchAssign’ Object.assign $payload to $payload.id in cache
        -   $operation == ‘patchDiff’ jsonPatch.applyPatch $payload to $payload.id in cache
-   sendOplog should stream the checkpoint starting at file location ‘opLogId’ (which is now an offset in the oplog file)
-   Client.js NDJSON:
    -   processOplog like server does. Only difference is that opLogId is not an operation counter but a byte counter.

# StartupDB 2.6

### Re-introduce gziped checkpoints

### Stream operations

-   Operations:

    -   POST should not check for existing docs, i.e. PUT == POST
    -   This way, we can stream operations by appending them to the oplog, no need to have the entire collection in memory. If the collection happens to be in memory, the server applies the operation on the in-memory cache after appending it to the oplog. If not, it only streams the operation to the oplog.
