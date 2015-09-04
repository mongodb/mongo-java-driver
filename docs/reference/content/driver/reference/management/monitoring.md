+++
date = "2015-03-19T12:53:26-04:00"
title = "Monitoring"
[menu.main]
  parent = "Sync Management"
 identifier = "Sync Monitoring"
  weight = 100
  pre = "<i class='fa'></i>"
+++

# JMX Monitoring

The driver uses [JMX](http://docs.oracle.com/javase/8/docs/technotes/guides/jmx/) to create
[MXBeans](http://docs.oracle.com/javase/tutorial/jmx/mbeans/mxbeans.html) that allow an
application or end user to monitor various aspects of the driver.

The driver creates MXBean instances of a single type:
[ConnectionPoolStatisticsMBean](http://api.mongodb.org/java/3.0/com/mongodb/management/ConnectionPoolStatisticsMBean.html).
 The driver registers one `ConnectionPoolStatisticsMBean` instance per each server it connects to. For example, in the case of a replica 
 set, the driver creates an instance per each non-hidden member of the replica set.

Each MXBean instance is required to be registered with a unique object name, which consists of a domain and a set of named properties. All 
MXBean instances created by the driver are under the domain `"org.mongodb.driver"`.  Instances of `ConnectionPoolStatisticsMBean` will have 
the following properties:

- `clusterId`: a client-generated unique identifier, required to ensure object name uniqueness in situations where an
application has multiple `MongoClient` instances connected to the same MongoDB server deployment
- `host`: the host name of the server
- `port`: the port on which the server is listening
- `minSize`: the minimum allowed size of the pool, including idle and in-use members
- `maxSize`: the maximum allowed size of the pool, including idle and in-use members
- `size`: the current size of the pool, including idle and and in-use members
- `waitQueueSize`: the current size of the wait queue for a connection from this pool
- `checkedOutCount`: the current count of connections that are currently in use

# Command Monitoring

The driver implements the 
[command monitoring specification](https://github.com/mongodb/specifications/blob/master/source/command-monitoring/command-monitoring.rst), 
which allows an application to attach its own event listeners that are notified when commands are started and when they sucessfully 
completed or fail.
 
Command listeners are registered individually for each instance of `MongoClient` by configuring  `MongoClientOptions` with one or more 
instances of a class that implements the [`CommandListener`]({{< apiref "com/mongodb/event/CommandListener" >}}) interface.   Consider 
the following, obviously simplistic, implementation of the `CommandListener` interface:
 
```java
public class TestCommandListener implements CommandListener {                        
    @Override                                                                                                        
    public void commandStarted(final CommandStartedEvent event) {                                                    
        System.out.println(String.format("Sent command '%s:%s' with id %s to database '%s' "                         
                                         + "on connection '%s' to server '%s'",                                      
                                         event.getCommandName(),                                                     
                                         event.getCommand().get(event.getCommandName()),                             
                                         event.getRequestId(),                                                       
                                         event.getDatabaseName(),                                                    
                                         event.getConnectionDescription()                                            
                                              .getConnectionId(),                                                    
                                         event.getConnectionDescription().getServerAddress()));                      
    }                                                                                                                
                                                                                                                     
    @Override                                                                                                        
    public void commandSucceeded(final CommandSucceededEvent event) {                                                
        System.out.println(String.format("Successfully executed command '%s' with id %s "                            
                                         + "on connection '%s' to server '%s'",                                      
                                         event.getCommandName(),                                                     
                                         event.getRequestId(),                                                       
                                         event.getConnectionDescription()                                            
                                              .getConnectionId(),                                                    
                                         event.getConnectionDescription().getServerAddress()));                      
    }                                                                                                                
                                                                                                                     
    @Override                                                                                                        
    public void commandFailed(final CommandFailedEvent event) {                                                      
        System.out.println(String.format("Failed execution of command '%s' with id %s "                              
                                         + "on connection '%s' to server '%s' with exception '%s'",                  
                                         event.getCommandName(),                                                     
                                         event.getRequestId(),                                                       
                                         event.getConnectionDescription()                                            
                                              .getConnectionId(),                                                    
                                         event.getConnectionDescription().getServerAddress(),                        
                                         event.getThrowable()));                                                     
    }                                                                                                                
}                                                                                                                            
```
     
and an instance of `MongoClientOptions` configured with an instance of `TestCommandListener`:            
            
```java                                                                                   
MongoClientOptions options = MongoClientOptions.builder()                            
                                               .addCommandListener(new TestCommandListener())  
                                               .build();                             
```

A `MongoClient` configured with these options will print a message to `System.out` before sending each command to a MongoDB server, and 
another message upon either successful completion or failure of each command.

