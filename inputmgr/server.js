'use strict';

/*
TODO's: 
-Dynamically determine whether to load new records one at a time, how many at a time
  ---> FIX - bulk records coming in, reload in chunks
-Multitable
-Allow for aliasing a specific column to join to another table
-Partial/additive vs replace
-Pass in parameter to hardcode record count load

*/

var os  = require('os-utils');
const express = require('express');
var router = express.Router();
const WebSocket = require('ws');
const enigma = require('enigma.js');
const schema = require('enigma.js/schemas/3.2.json');

const path = require('path');
const fs = require('fs');
const jwt = require('jsonwebtoken');

// Your Sense Enterprise installation hostname:
const senseHost = '172.31.16.161';

// Your configured virtual proxy prefix for JWT authentication:
const proxyPrefix = 'jwt';

// The Sense Enterprise-configured user directory for the user you want to identify
// as:
const userDirectory = 'EC2AMAZ-N1DUBMS';

// The user to use when creating the session:
const userId = 'qlik_service';

// Set the Qlik App Id
const appId='47c8312c-73cb-4a42-b810-52522b5b9498';

// The Sense Enterprise-configured JWT structure. Change the attributes to match
// your configuration:
const token = {
  userDirectory: userDirectory,
  userId: userId,
};

// Path to the private key used for JWT signing:
const privateKeyPath = './private.key';
const key = fs.readFileSync(path.resolve(__dirname, privateKeyPath));

//Sense server configuration using JWT
var config = {
  schema,
  url: `ws://${senseHost}/${proxyPrefix}/app/engineData`,
  // Notice how the signed JWT is passed in the 'Authorization' header using the
  // 'Bearer' schema:
  createSocket: url => new WebSocket(url, {
    headers: { Authorization: `Bearer ${signedToken}` },
  }),
};

// Sign the token using the RS256 algorithm:
const signedToken = jwt.sign(token, key, { algorithm: 'RS256' });


// Constants
const PORT = 9090;
const HOST = '0.0.0.0';
//
// // App
const app = express();

var reloadTrigger=0;
//const app2 = express()
var webpath = './site/index.html';
var indexLoc = path.resolve(__dirname, webpath) 
//app.use(express.json());

app.get("/",function(req,res){
  res.sendFile(indexLoc);
});

app.use("/",router);

var lastOffsetReloaded=fs.readFileSync('./lastOffset', 'utf8');

if( lastOffsetReloaded==='NaN') {
  lastOffsetReloaded=0;
}

app.get('/duh', function(req,res) {
  var html='<html><head><title></title></head><body>'
  html+="<div id='whateverdiv'>"+lastOffsetReloaded+"</div>"
  html+='</body></html>'
  res.send(html)
})

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


// Set the # of records when a reload should occur
var offsetFrom=0; //Default
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
  var numRecordsSinceReload=0;

  var record={};

  var kafka = require('kafka-node');
  var Consumer = kafka.Consumer;
  var Offset = kafka.Offset;
  var Client = kafka.KafkaClient;
  var topic = 'test10';

  var client = new Client({ kafkaHost: '172.31.29.9:9092' });
  var topics = [{ topic: topic, partition: 0}];
  var options = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024, fromOffset: lastOffsetReloaded };

  var consumer = new Consumer(client, topics, options);
  var offset = new Offset(client);

  consumer.addTopics([topic], function (err, added){
      console.log('topic added from last offset')
}, lastOffsetReloaded);

  var recordCount = 0;
  var reloadingBool = false;
  var initialTime=new Date();
  var previousMsgTime=new Date();
  var msgTime = new Date();
  var scriptLet='';
  var valuesForScript=[];
  var keys = [];
  var timeCounterSecs=0;
  var recsPerSec=0;
  var prevRecsPerSec=0;
  var numRecords=0;
  var numRecordsToReload=20;
  var velocityProfile=''; 
  var currentOffset=0;
  var batchCount=0;
  var maxReloadBatchSize=20;
  var preferredBatchSize=1;
  var idleDelay=5000  //set the amount of seconds with an idle pipe (threshold of ms since last message) to call for a reload
  var msSinceLastMsg=0;

  //Every Second run a time logger to count how many records, also inspect the reloadTrigger variable, this shouldn't be needed in the future
  setInterval(function(){ 
    var now = new Date();

    if (msgTime) {
        var msSinceLastMsg=now - msgTime;
    }
    
    timeCounterSecs=timeCounterSecs+1; 
    
    recsPerSec=valuesForScript.length/timeCounterSecs;
    prevRecsPerSec=recsPerSec;

    var prevRecsLower=prevRecsPerSec > recsPerSec;
    var speedVsPrevious = prevRecsPerSec/recsPerSec;

    console.log('msSinceLastMsg',msSinceLastMsg)
    console.log('idleDelay',idleDelay)

    if (msSinceLastMsg > idleDelay && valuesForScript.length > 0) {
      reloadTrigger=1;
    } else {
      reloadTrigger=0;
    }

    //Run the reload function if the reloadTrigger is set to 1 and do an extra check to make sure there's data and enough idle time with records ready
    if (reloadTrigger===1 ) {
      execReload(valuesForScript); //Reload function at the end
    }

  }, 1000);

  consumer.on('message', function (message) {
    var prevOffset=message.offset;
  	currentOffset=message.offset;
  	console.log('currentOffset: ',currentOffset)
    console.log('lastOffsetReloaded',lastOffsetReloaded)
    var freshRecord =false; // identify if this message is new for the app

    if (currentOffset > lastOffsetReloaded ) { //don't do anything unless the newest offset is more than the last offset reloaded 
      freshRecord=true;
      numRecordsSinceReload=currentOffset-lastOffsetReloaded;

      console.log('yes current offset > lastOffset stored',currentOffset-lastOffsetReloaded)

      previousMsgTime = msgTime;
      msgTime = new Date();
      msSinceLastMsg = msgTime-previousMsgTime;

      if (msSinceLastMsg >2000 && recsPerSec < .3) {
      	console.log('Possible batchEnd detected, > 2s since last message and # of recs per second ');
      }

      recordCount = recordCount+1;
        
      if ( message.value && message.value.charAt(0) === '{' ) {

          // Create a record object to load microbatch
          record = JSON.parse(message.value);
          record.kafkaOffset=message.offset;

          // The "Energy" coming through the pipe is a characteristic of activity. 
          // In order to handle the engergy properly, frequency and volume of reloads needs to be managed dynamically. 
          // Energy is determined by Volume * Acceleration (E=MC2)
          // How to calculate Volume - # of records in the load queue (aka Microbatch)
          // How to calculate Acceleration - # of records per second vs previous 10 seconds, 30 seconds, 1 minute, short term vs long term acceleration

          numRecords=numRecords+1

          recsPerSec=recordQueue.length/timeCounterSecs;

          record.msgTimestamp=msgTime;

          // Add each record into the main queue
          recordQueue.push(record)

          //take the keys from the last record record
          keys = Object.keys(record);

          // Gather the values for this json object message
          var values = Object.values(record);

          // Add the values as a record into the array used for loading the values in the load script (and console out)
          valuesForScript.push(values);

          console.log('numRecordsSinceReload', numRecordsSinceReload, 'timeCounterSecs', timeCounterSecs, 'recsPerSec',recsPerSec)

      } else {
          record = ''
      };

      // Get column names for the first part of the load script
      

       //Loop through keys to generate column list for the first time:
      // if (recordCount==1 && record !== '') {
      console.log('numRecordsToReload',numRecordsToReload)
      console.log('numRecordsSinceReload',numRecordsSinceReload)
        
      // Execute the parsing into the load script

      if ( numRecordsSinceReload >= numRecordsToReload && record !== '') {
        console.log('set reloadTrigger=',reloadTrigger)
        reloadTrigger=1;
      };

      var recordQueueLength=recordQueue.length;
    }
 
  }); //end of kafka message

   


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

} //end of kafka conditional


// Create the function to actually do the reload when called
function execReload(data){

  console.log('reloadTrigger set!')
  reloadingBool=true;


  // If the scriptlet is blank, add the first line
  if ( scriptLet.length === 0 ) {
    scriptLet = 'ADD LOAD * INLINE [ \n';
  }
  //Copy the current state of the values object, retrieved fromt he original "valuesForScript" array
  var newValuesForScript=data;

  // Then ap the original recordQueue so that starts filling up waiting for message
  valuesForScript=[];

  //Add the columns to the inline load script from the "keys" object updated on every record consume:
  var i;
  for (i = 0; i < keys.length; i++) {
    if (i===keys.length-1) {
      scriptLet += keys[i]+'\n';
    } else {
      scriptLet += keys[i] + ",";
    }
  } 

  // Get the values for the qlik load script by looping through the values in the array
  var v;
  for (v = 0; v < newValuesForScript.length; v++) { //get the values part of the record
    var recordLine='';
    var x;
    var myValue;
    for (x=0; x < newValuesForScript[v].length; x++) { //get each value.. determine if it gets quotes or not (string or int)
      if (x < newValuesForScript[v].length-1) {
          if ( typeof newValuesForScript[v][x]==='string' ) {
            recordLine += '\''+newValuesForScript[v][x]+'\''+','
          } else {
            recordLine += newValuesForScript[v][x]+','
          }
      } else {
        if ( typeof newValuesForScript[v][x]==='string' ) {
          recordLine += '\''+newValuesForScript[v][x]+'\''+'\n';
        } else {
          recordLine += newValuesForScript[v][x]+'\n';
        }

        scriptLet+=recordLine;
        
      }
    }
  } 

  //Define the hypercube we'll get after loading the last record from that batch into the app
  const hypercube_properties = {
    qInfo: { qType: 'hello-offset' },
    qHyperCubeDef: {
      qMeasures: [
        {
          qDef: {
            qFallbackTitle: 'kafkaOffset',
            qType: 'I',
            qDef: "=max(kafkaOffset)",
            qFieldLabels: [
                "kafkaOffset"
            ]
          }
        }
      ],
      qInitialDataFetch: [{ qHeight: 1, qWidth: 1 }]
    }
  };
  scriptLet+=']';
    
    //console.log('scriptlet after cwriting last value: ', scriptLet);
  const session = enigma.create(config);
    

  //Qlik session for reload

  console.log('running the reload')
  session.open().then(global => {
    global.openDoc(appId)
    .then(app =>  
      app
      .setScript(scriptLet) //Set the load script
      .then(() => { console.log('scriptLet before load: ',scriptLet)
      }) //console the load script after being set successfully
      .then(() => app.doReload(0,true,false)
      .then(() => app.doSave()
        .then(() => {  //after it's saved, make sure to reset variables for batch
          reloadingBool=false;
          reloadTrigger=0;
          recordCount=0;
          numRecordsSinceReload=0;
          keys=[];
          newValuesForScript=[];
          recordQueue=[];
          scriptLet='';
        })
      )
      .then(() => app.createSessionObject(hypercube_properties) //once the app is saved after reload, create a hypercube telling us the value of the last kafka offset loaded, we'll use that to fetch records as appropriate later
      .then(function(cube) {
          //console.log(cube)
          cube.getLayout().then(function(layout) { //Get the data from the hypercube
              lastOffsetReloaded=layout.qHyperCube.qDataPages[0].qMatrix[0][0].qNum;
              fs.writeFile("./lastOffset", lastOffsetReloaded, function(err) { //write the last offset in the app down to a file
                  if(err) {
                      return console.log(err);
                  }
                  console.log("Last Offset Saved");
              })   
          }) //getLayout for qube, inside
          .catch(layouterr => {
              console.log(ummm)
          })
        }) //end cube creation/view for lastOffset inspection
        )
      .catch(cuberr => {
            console.log(cuberr)
        })
      )
    
    ).catch(scripterr => {
        console.log(scripterr)
    }) //end of app.setScript

  }) //end of session.open, then close session next
  // .then(global => session.close())

} //end of reloadingBool condition for reload

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

// app2.listen(9999, HOST);
// console.log(`Running on http://${HOST}:${PORT}`);