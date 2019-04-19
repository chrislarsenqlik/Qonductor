'use strict';

var os 	= require('os-utils');
const express = require('express');


// Constants
const PORT = 9090;
const HOST = '0.0.0.0';
//
// // App
const app = express();
app.use(express.json());

var qlikAppId='123';

app.get('/', (req, res) => {
  res.send('Hello world\n');
});

var inputType='Kafka'; //set as databot by default- Valid values - Kafka, Databot, REST
var recordQueue=[];

// Set the input type - Kafka, REST, Databot.. only want to accept one stream for now
app.post('/setInputType', function (req, res) {
  console.log('data: ', req.body);
  inputType=req.body;
  res.send(req.body);
});

// Set the input type - Kafka, REST, Databot.. only want to accept one stream for now
app.post('/setAppId', function (req, res) {
  console.log('data: ', req.body);
  qlikAppId=req.body;
  res.send(req.body);
});

// Set the # of records when a reload should occur
var reloadCount=10; //Default
app.post('/setReloadCount', function (req, res) {
  console.log('data: ', req.body);
  reloadCount=req.body;
  res.send(req.body);
});


app.post('/inputDatabot', function (req, res) {
  var queryResponse=req.body;
  recordQueue.push(queryResponse);
  console.log(req.body);
  res.sendStatus(200);
});

app.post('/inputREST', function (req, res) {
  console.log('data: ', req.body);
  recordQueue.push(req.body);
  res.send(200);
});

if (inputType==='Kafka') {
  
  var record={};

  console.log('kafka stuff goes here')
  var kafka = require('kafka-node');
  var Consumer = kafka.Consumer;
  var Offset = kafka.Offset;
  var Client = kafka.KafkaClient;
  var topic = 'test5';

  var client = new Client({ kafkaHost: '172.31.29.9:9092' });
  var topics = [{ topic: topic, partition: 1 }, { topic: topic, partition: 0 }];
  var options = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 };

  var consumer = new Consumer(client, topics, options);
  var offset = new Offset(client);
  
  var recordCount = 0;
  
  var initialTime=new Date();
  var previousMsgTime=new Date();
  
  //console.log('initial datetime: '+initialTime);
  var scriptLet = 'ADD LOAD * INLINE [';
  
  consumer.on('message', function (message) {
     
     //console.log(message);
     
     if ( message.value.charAt(0) === '{' ) { 
          record = JSON.parse(message.value); 
     } else { 
          record = ''
     };

     //console.log('initial datetime: '+initialTime);
     //console.log('previous MsgTime: '+previousMsgTime);
     var msgTime = new Date();
     var msSinceLastMsg = msgTime-previousMsgTime;
     previousMsgTime = msgTime;
     recordCount = recordCount+1;

     //console.log('records: ',recordCount,', MsSinceLastMsg', msSinceLastMsg);
     //console.log('record keys: ',Object.keys(record));
     var keys = Object.keys(record);

     //Loop through keys to generate column list for the first time:
     
     if (scriptLet==='ADD LOAD * INLINE [' || record !== '') {
       //console.log('scriptlet matched, first load');
       scriptLet+='\n'; 
       var i;
      if (recordCount==1) {
       for (i = 0; i < keys.length; i++) { 
         if (keys[i]==keys[keys.length-1]) {
            scriptLet += keys[i]+'\n';
         } else {
            scriptLet += keys[i] + ",";
         }
       }; 
      }

       var values = Object.values(record);
       var v;

       for (v = 0; v < values.length; v++) {
         if (values[v]==values[values.length-1]) {
            if(typeof values[v]==='number') {
               scriptLet += values[v]+' ];';
            } else if (typeof values[v]==='string') {
               scriptLet += '\''+values[v]+'\''+'];';
            } else if (values[v]==='null' || values[v]==null || values[v]==undefined) {
               scriptLet += ''+'];';
            }
         } else {
            if (typeof values[v]==='number') {
               scriptLet += values[v]+',';
            } else if (typeof values[v]==='string') {
               scriptLet += '\''+values[v]+'\''+',';
            } else if (values[v]==='null' || values[v]==null || values[v]==undefined) {
               scriptLet += ''+',';
            }

         }
       };       

       //console.log(scriptLet);
 
     } else {

        //console.log('scriptlet not set as expected');
     }

     console.log(scriptLet);

  });


  //If the record queue count matches the threshold count, trigger a reload
  if (recordQueue.length == reloadCount) {
     
  }

  consumer.on('error', function (err) {
    console.log('error', err);
  });

  /*
   * If consumer get `offsetOutOfRange` event, fetch data from the smallest(oldest) offset
  */
  consumer.on('offsetOutOfRange', function (topic) {
    topic.maxNum = 2;
    offset.fetch([topic], function (err, offsets) {
      if (err) {
        return console.error(err);
      }
      var min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
      consumer.setOffset(topic.topic, topic.partition, min);
    });
  }); 
}

app.get('/fetchQueue', function (req, res) {
  res.send(JSON.stringify(recordQueue));

});

app.get('/clearQueue', function (req, res) {
  res.sendStatus(200);
  recordQueue=[];

});

app.get('/resources', (req, res) => {
  var cpuLoad;
  var resources={};
  resources.freeMem=os.freememPercentage();
  resources.memoryTotal=os.totalmem();
  resources.loadLast1M=os.loadavg(1);
  resources.loadLast5M=os.loadavg(5);
  resources.loadLast15M=os.loadavg(15);
  res.send(JSON.stringify(resources));
  console.log(resources.toString());
});


app.listen(PORT, HOST);
console.log(`Running on http://${HOST}:${PORT}`);
