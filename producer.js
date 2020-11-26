/**
 * 生产者
 */

const kafka = require('kafka-node');

let conn = {'kafkaHost':'127.0.0.1:9092'};

KeyedMessage = kafka.KeyedMessage;

km = new KeyedMessage('key', '!'),

payloads = [
    { topic: 'broadcast', messages: ['hello', 'world', km] }
];

var MQ = function (){
    this.mq_producers = {};
}

MQ.prototype.AddProducer = function (conn, handler){
    console.log('增加生产者',conn, this);
    let client = new kafka.KafkaClient(conn);
    let producer = new kafka.Producer(client);

    producer.on('ready', function(){
        if(!!handler){
            handler(producer);
        }
        setTimeout(function(){
            client.close(function(){
                console.log("close !!!!");
            });
        },5000);
    });

    producer.on('error', function(err){
        console.error('producer error ',err.stack);
    });

    this.mq_producers['common'] = producer;
    return producer;
}
console.log(MQ);
var mq = new MQ();

mq.AddProducer(conn, function (producer){
    producer.createTopics(['broadcast'], function (){
        var i=0;
        mq.mq_producers['common'].send([{topic:['broadcast'], 
            messages:[JSON.stringify({"cmd":"testRpc","value":"Hello World"+i})]}], function (){
                console.log("..... "+i);
            })
    })
    
});