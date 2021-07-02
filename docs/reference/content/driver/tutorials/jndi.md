+++
date = "2015-03-19T12:53:26-04:00"
title = "JNDI"
[menu.main]
  parent = "Connect to MongoDB"
  identifier = "Sync JNDI"
  weight = 30
  pre = "<i class='fa'></i>"
+++

## Java Naming and Directory Interface (JNDI)

The driver includes a [JNDI]({{< javaseref "technotes/guides/jndi/index.html" >}}) ObjectFactory implementation,
[`MongoClientFactory`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/MongoClientFactory" >}}) ([`legacy version`]({{< apiref "mongodb-driver-legacy" "com/mongodb/client/jndi/MongoClientFactory" >}})), that returns `MongoClient` instances based on a
[connection string](http://docs.mongodb.org/manual/reference/connection-string/).

## Examples

The configuration of the `MongoClientFactory` differs depending on the application server. Below are examples of a few popular ones.

### Wildfly (formerly JBoss)

1. In a [Wildfly](http://wildfly.org/) installation, create a new module for MongoDB at `modules/system/layers/base/org/mongodb/main`.

2. Copy the mongo-java-driver jar file into the module.

3. Add the following module.xml file into the module:

        <module xmlns="urn:jboss:module:1.3" name="org.mongodb">
           <resources>
               <resource-root path="mongodb-driver-sync-4.3.0-beta4.jar"/>
           </resources>
           <dependencies>
               <module name="javax.api"/>
               <module name="javax.transaction.api"/>
               <module name="javax.servlet.api" optional="true"/>
           </dependencies>
        </module>


4. Add a binding to JBoss's naming subsystem configuration that references the above module, the `MongoClientFactory` class, and the
connection string for the MongoDB cluster.
                 
        <subsystem xmlns="urn:jboss:domain:naming:2.0">
            <bindings>
                <object-factory name="java:global/MyMongoClient" module="org.mongodb" class="com.mongodb.client.MongoClientFactory">
                    <environment>
                        <property name="connectionString" value="mongodb://localhost:27017"/>
                    </environment>
                 </object-factory>
            </bindings>
            <remote-naming/>
        </subsystem>

A MongoClient instance will be accessible via the JNDI name `java:global/MyMongoClient`.

### Tomcat

1. In a [Tomcat](http://tomcat.apache.org/) installation, copy the mongo-java-driver jar file into the lib directory.

2. In context.xml of a web application, add a resource that references the `MongoClientFactory` class, and the connection string for the
MongoDB cluster:

        <Resource name="mongodb/MyMongoClient"
                  auth="Container"
                  type="com.mongodb.MongoClient"
                  closeMethod="close"
                  factory="com.mongodb.client.MongoClientFactory"
                  singleton="true"
                  connectionString="mongodb://localhost"/>

3. In web.xml of a web application, add a reference to the above resource:

        <resource-ref>
            <res-ref-name>
                mongodb/MyMongoClient
            </res-ref-name>
            <res-type>
                com.mongodb.MongoClient
            </res-type>
            <res-auth>
                Container
            </res-auth>
        </resource-ref>

A MongoClient instance will be accessible via the JNDI name `mongodb/MyMongoClient` in the `java:comp/env` context.
