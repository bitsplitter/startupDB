const v8 = require('v8')
const { estimateSizeOf } = require('../dist/util')


const nrElements = 1 * 1000 * 1000
let heap1 = v8.getHeapStatistics()
console.log('heapUsed', heap1.total_heap_size)
x = []
console.time('init')
for (i = 0; i < nrElements; i++) {
    x.push({
        "key1": ("value" + i + "000000000").substr(0, 16),
        "key2": ("value" + i + "000000000").substr(0, 16)
    })
    if (i % 100000 == 0)
        console.log(process.memoryUsage().heapUsed / 1024 / 1024)
}
console.timeEnd('init')
console.time('est')
console.log('est', estimateSizeOf(x) / nrElements)
console.log('est', JSON.stringify(x).length / nrElements)
console.timeEnd('est')
heap2 = v8.getHeapStatistics()
console.log('heapUsed', heap2.total_heap_size)
console.log('heapUsed', (heap2.total_heap_size - heap1.total_heap_size) / nrElements)
