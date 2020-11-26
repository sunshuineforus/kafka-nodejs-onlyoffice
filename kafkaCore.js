'use strict';
const kafka = require('kafka-node');
var config = require('config');
let cfhKafkaUrl = config.get("kafka.url");

function createKafkaClient() {
    return new Promise(function (resolve, reject) {
        const client = new kafka.KafkaClient(cfhKafkaUrl);
            resolve(client);
    })
}

function createProducer(client) {
    return new Promise(function (resolve, reject) {
        const producer = new kafka.Producer(client);
            resolve(producer);
    })
}


function createConsumer(client,topics,options) {
    return new Promise(function (resolve, reject) {
        const consumer = new kafka.Consumer(client, topics, options);
            resolve(consumer);
    })
}

function closePromise(client){
    return new Promise(function(resolve,reject){
        client.close(function(err){
            if(err){
                reject(err);
            }else{
                resolve();
            }
        });
    });
}

function createTopicsPromise(client,topics){
    return new Promise(function(resolve,reject){
        client.createTopics(topics,function(err,ok){
            if(null!=err){
                reject(err);
            }else{
                resolve();
            }
        });
    });
}

function addTopicsPromise(consumer,topics){
    return new Promise(function(resolve,reject){
        consumer.addTopics(topics,function(err,ok){
            if(null!=err){
                reject(err);
            }else{
                resolve();
            }
        });
    });
}

function removeTopicsPromise(consumer,topics){
    return new Promise(function(resolve,reject){
        consumer.removeTopics(topics,function(err,ok){
            if(null!=err){
                reject(err);
            }else{
                resolve();
            }
        });
    });
}

module.exports.createTopicsPromise=createKafkaClient;
module.exports.createTopicsPromise=createProducer;
module.exports.createTopicsPromise=createConsumer;
module.exports.createTopicsPromise=createTopicsPromise;
module.exports.createTopicsPromise=closePromise;
module.exports.createTopicsPromise=addTopicsPromise;
module.exports.createTopicsPromise=removeTopicsPromise;