# HiveMQ MQTT Clients

## Build
mvn clean package
   
## Clients
   
### AsyncClient (MQTT5)
   
The AsyncClient starts two threads.  The first thread subscribes to a topic on HiveMQ and the second publishes to that topic.  This is done via the asynchronous API of the HiveMQ Client.  Two messages are published.  One on the same thread and the other on an Executor thread.  This is done to show that traces still occur even though it jumps threads.  Publishes 300 messages and then shuts down.

Takes two optional arguments
-host=hostname
-messages=##
   
if omitted then default values are used.  Default for host is localhost.   Default messages is 300

#### Run

Using localhost HiveMQ and publish 300 messages  
    
mvn exec:java -DmainClass=com.nr.fit.hivemq.client.examples.AsyncClient 
   
Using localhost HiveMQ and publish 200 messages  
   
mvn exec:java -DmainClass=com.nr.fit.hivemq.client.examples.AsyncClient -Dexec.args=-messages=200   
   
Using myHost HiveMQ and publish 300 messages  
   
mvn exec:java -DmainClass=com.nr.fit.hivemq.client.examples.AsyncClient -Dexec.args=-host=myHost   
   
Using myHost HiveMQ and publish 400 messages  
   
mvn exec:java -DmainClass=com.nr.fit.hivemq.client.examples.AsyncClient -Dexec.args="-host=myHost -messages=400"      
   
### SyncClient (MQTT5)
   
Same as AsyncClient except using synchronous API calls

Takes two optional arguments
-host=hostname
-messages=##
   
if omitted then default values are used.  Default for host is localhost.   Default messages is 300

#### Run

Using localhost HiveMQ and publish 300 messages  
    
mvn exec:java -DmainClass=com.nr.fit.hivemq.client.examples.SyncClient 
   
Using localhost HiveMQ and publish 200 messages  
   
mvn exec:java -DmainClass=com.nr.fit.hivemq.client.examples.SyncClient -Dexec.args=-messages=200   
   
Using myHost HiveMQ and publish 300 messages  
   
mvn exec:java -DmainClass=com.nr.fit.hivemq.client.examples.SyncClient -Dexec.args=-host=myHost   
   
Using myHost HiveMQ and publish 400 messages  
   
mvn exec:java -DmainClass=com.nr.fit.hivemq.client.examples.SyncClient -Dexec.args="-host=myHost -messages=400"      
   
   
### RxClient (MQTT5)
   
This consists of two separate clients, one is the Publisher and the other is the Subscriber.   Both use the RxJava HiveMQ MQTT API calls.  

#### Run
   
Note that you can provide additional arguments similar to the other two client

Always start the Subcriber first   
   
mvn exec:java -DmainClass=com.nr.fit.hivemq.client.examples.RxClient_Sub   
    
Start the Publisher   
   
mvn exec:java -DmainClass=com.nr.fit.hivemq.client.examples.RxClient_Pub   
 
   
### MQTT3 Sync client

Same as Sync MQTT5 client except uses MQTT3.  As a result no distributed tracing is possible.

#### Run

Using localhost HiveMQ and publish 300 messages  
    
mvn exec:java -DmainClass=com.nr.fit.hivemq.client.examples.Mqtt3SyncClient 
