var fs = require("fs");
const args = require('minimist')(process.argv.slice(2));
var kafka = require('kafka-node'),
HighLevelProducer = kafka.HighLevelProducer,
Client = kafka.KafkaClient;
client = new Client({ kafkaHost: '172.31.29.9:9092' }),
producer = new HighLevelProducer(client,producer_options);
var topic=args['topic'];
var samplefile=args['samplefile'];
var delayms=args['delayms'];
var jsondata = fs.readFileSync('./'+samplefile);
var dataparsed = JSON.parse(jsondata);


var producer_options = {
  // Configuration for when to consider a message as acknowledged, default 1
  requireAcks: 1,
  // The amount of time in milliseconds to wait for all acks before considered, default 100ms
  ackTimeoutMs: 100,
  // Partitioner type (default = 0, random = 1, cyclic = 2, keyed = 3), default 0
  partitionerType: 2
}

var messageHandler = function(err, data) {
  if (err) { console.log(err); return; } 
  else { console.log(data); return; }
};

producer.on('ready', function(){
  var i;
  for (i = 0; i < dataparsed.length; i++) {
    (function(i){
      setTimeout(function(){
        var dataString=JSON.stringify(dataparsed[i]);
        payload = [{ topic: topic, messages: dataString, partition: 0 }];
        console.log('payload: ',payload);
    	producer.send(payload, function(err, data) {
        	return messageHandler(err, data);
	}) 
     }, i * delayms);
    }(i));
  }
})

