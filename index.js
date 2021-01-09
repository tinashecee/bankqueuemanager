
const exphbs = require('express-handlebars');

const redis = require('redis');
const express = require('express');
const bodyParser = require('body-parser');
const path = require('path');
const Nexmo = require('nexmo');
const socketio = require('socket.io');
_ = require('lodash');
const Kafka = require("node-rdkafka"); // see: https://github.com/blizzard/node-rdkafka 
const externalConfig = require('./config'); 
const CONSUMER_GROUP_ID = "node-consumer" 
// construct a Kafka Configuration object understood by the node-rdkafka library 
// merge the configuration as defined in config.js with additional properties defined here 
const kafkaConf = {...externalConfig.kafkaConfig ,
 ...{ "group.id": CONSUMER_GROUP_ID,
 "socket.keepalive.enable": true,
 "debug": "generic,broker,security"} 
}; 
const topics = [externalConfig.topic] 
const app = express();
app.set('views', path.join(__dirname, 'views'));
app.engine('handlebars', exphbs({defaultLayout: 'main'}));
app.set('view engine', 'handlebars');
//setup public folder
app.use(express.static('./public'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended:true}));

const nexmo = new Nexmo({
  apiKey: '459ed73c',
  apiSecret: 'CC8KyfR7D2MJmTUB'
}, {debug: true})


//create Redis client
let client = redis.createClient({port:14560,host:'redis-14560.c16.us-east-1-2.ec2.cloud.redislabs.com',no_ready_check: true,
auth_pass: 'redissecurepassword001', });


app.get('/', (req, res) => {
      
  
        
        function reassembleSort() {

            
            //get all the arguments
            var	sortArguments = Array.prototype.slice.call(arguments);
            //get the callback, convert the single element array into a string
            var originalCb = Array.prototype.slice.call(arguments).slice(-1)[0];
            var	fields = [];
        
        //remove the last argument, eg the callback
        sortArguments.splice(-1,1);
    
        //go through each argument
        sortArguments.forEach(function(anArgument,argumentIndex) {
            //if the argument is some form of 'get'
            if (anArgument.toLowerCase() === 'get') {
                //special pattern for getting the key
                if (sortArguments[argumentIndex+1] === '#') {
                    //push it into the fields array
                    fields.push('#');
                } else {
                    //otherwise split the pattern by '->', retrieving the latter part
                    //and push it into the fields object
                    fields.push(sortArguments[argumentIndex+1].split('->')[1]);
                }
            }
        });
        //run the normal sort
       client.sort.apply(
            client, //the `this` argument of the sort should be the client
            [
                sortArguments, //just the sort arguments without the callback
                function(err, values){
                    if (err) { originalCb(err); } else {
                        //we'll use lodash
                        values = _(values)
                            //chunk splits up the returned values by the number of fields
                            //and returns it into a nested array of these chunks
                            .chunk(fields.length)
                            .map(function(aValueChunk) {
                                //we will zip the fields and the chunks
                                return _.zipObject(fields, aValueChunk);
                            })
                            .value();
                        //call the original callback passing in the new values	
                        originalCb(err,values);
                    }
                    
                }
            ]
        );
    }
    
    reassembleSort(
        'queuemembers',
        'ALPHA',
        'BY',
        '*->timestamp',
        'GET',
        '*->name',
        'GET',
        '*->timestamp',
        'GET',
        '*->bank',
        'GET',
        '*->branch',
        'GET',
        '*->service',
        'GET',
        '*->tag',
        'GET',
        '*->phone',
        

         function(err,values) {
            if (err) throw err;
            let arr = []
            
           
           //arr=JSON.stringify(values,null,' ');
           
           arr = values;
           var delayInMilliseconds = 2000; 
           setTimeout(function() {
           console.log(arr)
        
           res.render('index', {
            queue: arr
        });
      }, delayInMilliseconds);
           //setTimeout(function() {
            
          // }, delayInMilliseconds);
           //await res.render('index', {
            //   queue: arr
           //});
            
        }
    );
        
  

  })
  app.get('/next', (req, res) => { 
    function reassembleSort() {

            
      //get all the arguments
      var	sortArguments = Array.prototype.slice.call(arguments);
      //get the callback, convert the single element array into a string
      var originalCb = Array.prototype.slice.call(arguments).slice(-1)[0];
      var	fields = [];
  
  //remove the last argument, eg the callback
  sortArguments.splice(-1,1);

  //go through each argument
  sortArguments.forEach(function(anArgument,argumentIndex) {
      //if the argument is some form of 'get'
      if (anArgument.toLowerCase() === 'get') {
          //special pattern for getting the key
          if (sortArguments[argumentIndex+1] === '#') {
              //push it into the fields array
              fields.push('#');
          } else {
              //otherwise split the pattern by '->', retrieving the latter part
              //and push it into the fields object
              fields.push(sortArguments[argumentIndex+1].split('->')[1]);
          }
      }
  });
  //run the normal sort
 client.sort.apply(
      client, //the `this` argument of the sort should be the client
      [
          sortArguments, //just the sort arguments without the callback
          function(err, values){
              if (err) { originalCb(err); } else {
                  //we'll use lodash
                  values = _(values)
                      //chunk splits up the returned values by the number of fields
                      //and returns it into a nested array of these chunks
                      .chunk(fields.length)
                      .map(function(aValueChunk) {
                          //we will zip the fields and the chunks
                          return _.zipObject(fields, aValueChunk);
                      })
                      .value();
                  //call the original callback passing in the new values	
                  originalCb(err,values);
              }
              
          }
      ]
  );
}

reassembleSort(
  'queuemembers',
  'ALPHA',
  'BY',
  '*->timestamp',
  'GET',
  '*->name',
  'GET',
  '*->timestamp',
  'GET',
  '*->tag',
  'GET',
  '*->phone',
   function(err,values) {
      if (err) throw err;
      let arr = []
      
     
     //arr=JSON.stringify(values,null,' ');
     arr = values;
     
   
    var delayInMilliseconds = 1000;

  
    if (arr[1].phone){
      console.log(arr[1].phone)
    
    const text = 'Hello this is Virtual Queue, please make your way to the bank.'
            nexmo.message.sendSms(
              'Vonage APIs', arr[1].phone, text, { type: 'unicode'},
              (err, responseData) => {
                if(err){
                  console.log(err)
                }else{
                  console.dir(responseData);
                }
              }
            )
    }

     
     setTimeout(function() {
    // client.del(arr[0].tag);
    
    client.del(arr[0].tag);
    
    client.srem("queuemembers", arr[0].tag);
     res.redirect('/');
}, delayInMilliseconds);
     
      
  }
);

  });
  function sendText(num){
  const number = num 
            
  }


  let stream = new Kafka.KafkaConsumer.createReadStream(kafkaConf, { "auto.offset.reset": "earliest" }, { topics: topics })
  stream.on('data', function (message) { 
    //console.log(`Consumed message on Stream: ${message.value.toString()}`);
    // the structure of the messages is as follows: 
    // { 
    // value: Buffer.from('hi'),  // message contents as a Buffer 
    // size: 2, // size of the message, in bytes 
    // topic: 'librdtesting-01', // topic the message comes from 
    // offset: 1337, // offset the message was read from 
    // partition: 1, // partition the message was on 
    // key: 'someKey', // key of the message if present 
    // timestamp: 1510325354780 // timestamp of message creation 
    // } 
    try{
    const jsonObj = JSON.parse(message.value.toString())
        const id = jsonObj.id;
        const name = jsonObj.name;
        const branch = jsonObj.branch;
        const bank = jsonObj.bank;
        const service = jsonObj.service;
        const phone = jsonObj.phone;
        const timestamp = JSON.parse(message.timestamp.toString())
        const offset = JSON.parse(message.offset.toString())
     
        console.log(id,timestamp, offset, name,branch,bank,service,phone);
        client.hmset(id, [
          'timestamp', timestamp,
          'name', name,
          'offset', offset,
          'bank',bank,
          'tag',id,
          'phone',phone,
          'branch',branch,
          'service',service
      ], function(err, reply){
          if(err){
              console.log(err);
          }
          console.log(reply);
          
      });
      client.sadd("queuemembers", id);
    }
    catch (error) {
        console.log('err=', error)
    }
  }); 
  console.log(`Stream consumer created to consume from topic ${topics}`); 
  stream.consumer.on("disconnected", function (arg) {
   console.log(`The stream consumer has been disconnected`)  
   process.exit(); 
  }); 
  // automatically disconnect the consumer after 30 seconds setTimeout(function () { stream.consumer.disconnect(); }, 30000)
const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const jsonObj = JSON.parse(message.value.toString())
        const id = jsonObj.id;
        const name = jsonObj.name;
        const branch = jsonObj.branch;
        const bank = jsonObj.bank;
        const service = jsonObj.service;
        const phone = jsonObj.phone;
        const timestamp = JSON.parse(message.timestamp.toString())
        const offset = JSON.parse(message.offset.toString())
     /*   let passengerInfo = filterPassengerInfo(jsonObj)
        if (passengerInfo) {
          console.log(
            '******* Alert!!!!! passengerInfo *********',
            passengerInfo
          )
        }*/
        console.log(id,timestamp, offset, name,branch,bank,service,phone);
        await client.hmset(id, [
          'timestamp', timestamp,
          'name', name,
          'offset', offset,
          'bank',bank,
          'tag',id,
          'phone',phone,
          'branch',branch,
          'service',service
      ], function(err, reply){
          if(err){
              console.log(err);
          }
          console.log(reply);
          
      });
      await client.sadd("queuemembers", id);
      } catch (error) {
        console.log('err=', error)
      }
    }
  })
}




app.listen(3000, () => console.log("My server is listening on port 3000"))
