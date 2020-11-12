//import http from "http";
//import ws from "websocket"
//import redis from "redis";
const exphbs = require('express-handlebars');
const { Kafka } = require('kafkajs')
const redis = require('redis');
const express = require('express');
const expressLayouts = require('express-ejs-layouts')
const bodyParser = require('body-parser');
const path = require('path');
_ = require('lodash');
const app = express();
app.set('views', path.join(__dirname, 'views'));
app.engine('handlebars', exphbs({defaultLayout: 'main'}));
app.set('view engine', 'handlebars');
let arrr=[]
app.use(bodyParser.urlencoded({extended:false}));



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
   function(err,values) {
      if (err) throw err;
      let arr = []
      
     
     //arr=JSON.stringify(values,null,' ');
     arr = values;
     
    console.log(arr[0])

     var delayInMilliseconds = 1000; 
     setTimeout(function() {
    // client.del(arr[0].tag);
    client.del(arr[0].tag);
    client.srem("queuemembers", arr[0].tag);
     res.redirect('/');
}, delayInMilliseconds);
     
      
  }
);

  });
const kafka = new Kafka({
    'clientId':'myapp',
    'brokers': ['localhost:19092','localhost:29092','localhost:39092']
})
const http = require('http');
const ws = require('websocket');
const APPID = process.env.APPID;
//create Redis client
let client = redis.createClient();
const topic = 'testQueue5'
const consumer = kafka.consumer({
  groupId: 'group2'
})

let connections = [];
const WebSocketServer = ws.server


const subscriber = redis.createClient();

const publisher = redis.createClient();
  
 
subscriber.on("subscribe", function(channel, count) {
  console.log(`Server ${APPID} subscribed successfully to livechat`)
  publisher.publish("livechat", "a message");
});
 
subscriber.on("message", function(channel, message) {
  try{
  //when we receive a message I want t
  console.log(`Server ${APPID} received message in channel ${channel} msg: ${message}`);
  connections.forEach(c => c.send(APPID + ":" + message))
    
  }
  catch(ex){
    console.log("ERR::" + ex)
  }
});


subscriber.subscribe("livechat");


//create a raw http server (this will help us create the TCP which will then pass to the websocket to do the job)
const httpserver = http.createServer()

//pass the httpserver object to the WebSocketServer library to do all the job, this class will override the req/res 
const websocket = new WebSocketServer({
    "httpServer": httpserver
})
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
        const timestamp = JSON.parse(message.timestamp.toString())
        const offset = JSON.parse(message.offset.toString())
     /*   let passengerInfo = filterPassengerInfo(jsonObj)
        if (passengerInfo) {
          console.log(
            '******* Alert!!!!! passengerInfo *********',
            passengerInfo
          )
        }*/
        console.log(id,timestamp, offset, name,branch,bank,service);
        await client.hmset(id, [
          'timestamp', timestamp,
          'name', name,
          'offset', offset,
          'bank',bank,
          'tag',id,
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
run().catch(e => console.error(`[example/consumer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
process.on(type, async e => {
try {
console.log(`process.on ${type}`)
console.error(e)
await consumer.disconnect()
process.exit(0)
} catch (_) {
process.exit(1)
}
})
})

signalTraps.map(type => {
process.once(type, async () => {
try {
await consumer.disconnect()
} finally {
process.kill(process.pid, type)
}
})
})

app.listen(8000, () => console.log("My server is listening on port 8000"))

//when a legit websocket request comes listen to it and get the connection .. once you get a connection thats it! 
websocket.on("request", request=> {

    const con = request.accept(null, request.origin)
    con.on("open", () => console.log("opened"))
    con.on("close", () => console.log("CLOSED!!!"))
    con.on("message", message => {
        //publish the message to redis
        console.log(`${APPID} Received message ${message.utf8Data}`)
        publisher.publish("livechat", message.utf8Data)
    })
    function sendNumber() {
      
      if (con.connected) {
          var number = Math.round(Math.random() * 0xFFFFFF);
          con.sendUTF(number.toString());
          setTimeout(sendNumber, 1000);
      }
  }
   sendNumber();
    setTimeout(() => con.send(`Connected successfully to server ${APPID}`), 5000)
    connections.push(con)
  

})
  
//client code 
//let ws = new WebSocket("ws://localhost:8080");
//ws.onmessage = message => console.log(`Received: ${message.data}`);
//ws.send("Hello! I'm client")


/*
    //code clean up after closing connection
    subscriber.unsubscribe();
    subscriber.quit();
    publisher.quit();
    */
