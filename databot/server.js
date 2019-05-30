'use strict';

var os 	= require('os-utils');
const express = require('express');
const http = require('http');

var options = {
  hostname: 'inputs',
  port: 9090,
  path: '/inputDatabot',
  method: 'POST',
  json: true,
  headers: {"content-type": "application/json"}
};

// Constants
const PORT = 9080;
const HOST = '0.0.0.0';

// App
const app = express();
app.use(express.json());

app.get('/', (req, res) => {
  res.send('Hi this is the databot\n');
});

var runToggle=0;
var generateFreq=500; //500ms

app.get('/start', (req, res) => {
  runToggle=1;
  console.log('Toggle set to 1;');
  res.send('Started');
});

console.log('random')

var timerId;
var readId=0;

function randomIntInc(low, high) {
  return Math.floor(Math.random() * (high - low + 1) + low)
}

setInterval(function() {
  if (runToggle==1) {
    readId=readId+1;

    var postdata = {
       readId: readId,
       readValue: randomIntInc(1, 6)
    };

    var body='';

    var request = http.request(options, function(response) {
      //When we receive data, we want to store it in a string
      response.on('data', function (chunk) {
        body += chunk;
      });
      //On end of the request, run what we need to
      response.on('end',function() {
        //Do Something with the data
        console.log(body);
      });
    });

    request.on('error', function(e) {
      console.log('problem with request: ' + e.message);
    });

    //Write our post data to the request
    request.write(JSON.stringify(postdata));
    //End the request.
    request.end();

    console.log('tick'); 





  } else if (runToggle==2) {
     
  }
}, 1000);

app.get('/stop', (req, res) => {
  runToggle=2;
  console.log('Toggle set to '+runToggle);
  res.send('Databot Stopped');
});

app.listen(PORT, HOST);
console.log(`Running on http://${HOST}:${PORT}`);
