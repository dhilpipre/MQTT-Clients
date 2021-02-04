# Eclipse Paho MQTT5 Clients

## Build
mvn clean package
   
## Running with New Relic Java Agent
   
To run these clients with the Java Agent, set the environment variable MAVEN_OPTS with the java agent string
export MAVEN_OPTS="-javaagent:path-to-newrelic.jar"
   
## Clients
   
### AsyncApplication (MQTT5)
   
The AsyncApplication has both a subscriber flow and publisher flow.  It runs for 15 minutes.

Takes three optional arguments
-host=hostname
-duration=##
-clientName
   
if omitted then default values are used. Value for duration is milliseconds.  Default for host is localhost.   Default duration is 900000 ms (15 minutes). Default clientName is Tester

#### Run

Using defaults  
    
mvn exec:java -DmainClass=com.nr.paho.mqtt5.examples.AsyncApplication 
   
Using myHost HiveMQ and run for 10 minutes  
   
mvn exec:java -DmainClass=com.nr.fit.hivemq.client.examples.AsyncApplication -Dexec.args="-host=myHost -duration=600000
   
   
### SyncApplication (MQTT5)
   
Same as AsyncApplication except using synchronous API calls

Takes three optional arguments
-host=hostname
-duration=##
-clientName
   
if omitted then default values are used. Value for duration is milliseconds.  Default for host is localhost.   Default duration is 900000 ms (15 minutes). Default clientName is Tester

#### Run

Using defaults  
    
mvn exec:java -DmainClass=com.nr.paho.mqtt5.examples.AsyncApplication 
   
Using myHost HiveMQ and run for 10 minutes  
   
mvn exec:java -DmainClass=com.nr.fit.hivemq.client.examples.AsyncApplication -Dexec.args="-host=myHost -duration=600000
   
   
### Synchronous Pub/Sub (MQTT5)
   
This consists of two separate clients, one is the Publisher and the other is the Subscriber.   Both use sync API client calls

#### Run
   
Note that you can provide additional arguments similar to the other two clients

Always start the Subcriber first in one terminal   
   
mvn exec:java -DmainClass=com.nr.paho.mqtt5.examples.SyncSubcriber   
    
Start the Publisher in another terminal   
   
mvn exec:java -DmainClass=com.nr.paho.mqtt5.examples.SyncPublisher  
 
   
