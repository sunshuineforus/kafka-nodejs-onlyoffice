const kafka = require('kafka-node');
const co = require('co');
let conn = {
    // 'kafkaHost': '127.0.0.1:9092',
    'kafkaHost':'127.0.0.:9092',
    'connectTimeout':2000,
};
function createKafkaClient() {
    return new Promise(function (resolve, reject) {
        const client = new kafka.KafkaClient(conn);
            resolve(client)
    })
}

var a = yield createKafkaClient();
console.log(a.);
let producer = new kafka.Producer(a);
var b;