/**
 * Copyright (c) 2008 - 2011 10gen, Inc. <http://10gen.com>
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.tools;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.util.Set;

/**
 * A simple class that formats Mongo Java driver connection pool statistics in an easily-accessible JSON format.
 * It can be used to get statistics on connection pool in the same VM by using the no-args constructor, or in any
 * VM by using the constructor that takes an MBeanServerConnection.
 * <p>
 * This class also exposes a command line interface modeled after mongostat.  For usage, run:

 * <pre>   java -cp mongo.jar com.mongodb.util.tools.ConnectionPoolStat --help}</pre>
 *
 * @mongodb.driver.manual reference/mongostat  mongostat
 * @deprecated This class will be removed in 3.x versions of the driver,
 *             so please remove it from your compile time dependencies.
 */
@Deprecated
public class ConnectionPoolStat {

    /**
     * Use the given MBean server connection to access statistics for connection pools.
     *
     * @param mBeanConnection the MBean server to connect to
     */
    public ConnectionPoolStat(MBeanServerConnection mBeanConnection) {
        this.mBeanConnection = mBeanConnection;
    }

    /**
     * Use the platform MBean server.  This is useful if you want to access statistics
     * for connection pools in the same virtual machine.
     *
     * @see java.lang.management.ManagementFactory#getPlatformMBeanServer()
     */
    public ConnectionPoolStat() {
        this.mBeanConnection = ManagementFactory.getPlatformMBeanServer();
    }

    /**
     * Gets the statistics for all Mongo connection pools registered with the MBean server used
     * by this instance.  The format will always be JSON, but the specific JSON fields may change in a
     * future release.  An example of the output, which should not be taken as a specification:
     *
     * <pre>
   { pools : [
     { objectName: 'com.mongodb:type=ConnectionPool,host=localhost/127.0.0.1,port=27018,instance=1',
       host: 'localhost', port: 27018, maxSize: 10, total: 10, inUse: 3,
       inUseConnections: [
         { namespace: 'FindContention.test', opCode: 'OP_QUERY', query: { }, numDocuments: 1, threadName: 'pool-2-thread-19', durationMS: 843, localPort: 64062 },
         { namespace: 'FindContention.test', opCode: 'OP_QUERY', query: { }, numDocuments: 1, threadName: 'pool-2-thread-1', durationMS: 4331, localPort: 64095 },
         { namespace: 'FindContention.test', opCode: 'OP_QUERY', query: { }, numDocuments: 1, threadName: 'pool-2-thread-16', durationMS: 4343, localPort: 64087 }
       ]
     },
     { objectName: 'com.mongodb:type=ConnectionPool,host=localhost/127.0.0.1,port=27017,instance=1',
       host: 'localhost', port: 27017, maxSize: 10, total: 10, inUse: 2,
       inUseConnections: [
         { namespace: 'FindContention.test', opCode: 'OP_QUERY', query: { }, numDocuments: 1, threadName: 'pool-2-thread-5', durationMS: 920, localPort: 64093 },
         { namespace: 'FindContention.test', opCode: 'OP_QUERY', query: { }, numDocuments: 1, threadName: 'pool-2-thread-11', durationMS: 1468, localPort: 64068 },
       ]
     }
    ]
   }</pre>
     *
     * @return JSON-formatted stats for all connection pools registered in JMX
     * @throws JMException for any JMX-related exceptions
     * @throws IOException for any I/O exceptions
     */
    public String getStats() throws JMException, IOException {
        CharArrayWriter charArrayWriter = new CharArrayWriter();
        PrintWriter printWriter = new PrintWriter(charArrayWriter);
        print(printWriter);
        return charArrayWriter.toString();
    }

    /**
     * Command line interface for displaying connection pool stats.  In order to connect to a remote JMX server to
     * get these stats, currently you must set com.sun.management.jmxremote.port system property on the remote server
     * and specify that port using the --port argument.
     *
     * @param args program arguments
     * @throws Exception JMX-related exceptions
     * @see ConnectionPoolStat#printUsage()
     */
    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = -1;
        long rowCount = 0;
        int sleepTime = 1000;

        int pos = 0;
        for (; pos < args.length; pos++) {
            if (args[pos].equals("--help")) {
                printUsage();
                System.exit(0);
            } else if (args[pos].equals("--host") || args[pos].equals("-h")) {
                host = args[++pos];
            } else  if (args[pos].equals("--port")) {
                port = getIntegerArg(args[++pos], "--port");
            } else if (args[pos].equals("--rowcount") || args[pos].equals("-n")) {
                rowCount = getIntegerArg(args[++pos], "--rowCount");
            } else if (args[pos].startsWith("-")) {
                printErrorAndUsageAndExit("unknown option " + args[pos]);
            }
            else {
                sleepTime = getIntegerArg(args[pos++], "sleep time") * 1000;
                break;
            }
        }

        if (pos != args.length) {
            printErrorAndUsageAndExit("too many positional options");
        }

        if (port == -1  && !host.contains(":")) {
            printErrorAndUsageAndExit("port is required");
        }

        String hostAndPort = (port != -1) ? host + ":" + port : host;

        if (rowCount == 0) {
            rowCount = Long.MAX_VALUE;
        }

        JMXServiceURL u = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + hostAndPort +  "/jmxrmi");
        JMXConnector connector = JMXConnectorFactory.connect(u);
        MBeanServerConnection mBeanConnection = connector.getMBeanServerConnection();
          try {
            ConnectionPoolStat printer = new ConnectionPoolStat(mBeanConnection);
            for (int i = 0; i < rowCount; i++) {
                System.out.println(printer.getStats());
                if (i != rowCount - 1) {
                    Thread.sleep(sleepTime);
                }
            }
        } finally {
            connector.close();
        }
    }

    private static int getIntegerArg(String arg, String argName) {
        try {
            return Integer.parseInt(arg);
        } catch (NumberFormatException e) {
            printErrorAndUsageAndExit(argName + " arg must be an integer");
        }
        throw new IllegalStateException();
    }

    private static void printErrorAndUsageAndExit(final String error) {
        System.err.println("ERROR: " + error);
        System.out.println();
        printUsage();
        System.exit(1);
    }

    private static void printUsage() {
        System.out.println("View live MongoDB connection pool statistics from a remote JMX server.");
        System.out.println();
        System.out.println("usage: java com.mongodb.tools.ConnectionPoolStat [options] [sleep time");
        System.out.println("sleep time: time to wait (in seconds) between calls. Defaults to 1");
        System.out.println("options:");
        System.out.println("  --help                 produce help message");
        System.out.println("  --port arg             JMX remote port. Required. Can also use --host hostname:port");
        System.out.println("  -h [ --host ] arg      JMX remote host. Defaults to localhost");
        System.out.println("  -n [ --rowcount ] arg  number of times to print stats (0 for indefinite)");
        System.out.println();
        System.out.println("Fields");
        System.out.println("  objectName                     - name of the JMX bean for this connection pool");
        System.out.println("  host                           - host of the mongod/mongos server");
        System.out.println("  port                           - port of the mongod/mongos server");
        System.out.println("  maxSize                        - max # of connections allowed");
        System.out.println("  total                          - # of connections allocated");
        System.out.println("  inUse                          - # of connections in use");
        System.out.println("  inUseConnections               - list of all in use connections");
        System.out.println("  inUseConnections.namespace     - namespace on which connection is operating");
        System.out.println("  inUseConnections.opCode        - operation connection is executing");
        System.out.println("  inUseConnections.query         - query the connection is executing (for query/update/remove)");
        System.out.println("  inUseConnections.numDocuments  - # of documents in the message (mostly relevant for batch inserts)");
        System.out.println("  inUseConnections.threadName    - name of thread on which connection is executing");
        System.out.println("  inUseConnections.durationMS    - duration that the operation has been executing so far");
        System.out.println("  inUseConnections.localPort     - local port of the connection");
    }

    private void print(PrintWriter pw) throws JMException, IOException {
        Set<ObjectName> beanSet = mBeanConnection.queryNames(new ObjectName("com.mongodb:type=ConnectionPool,*"), null);
        pw.println("{ pools : [");
        int i = 0;
        for (ObjectName objectName : beanSet) {
            pw.print("   { ");
            printAttribute("ObjectName", objectName.toString(), pw);
            pw.println();
            pw.print("     ");
            printAttribute("Host", objectName, pw);
            printAttribute("Port", objectName, pw);
            printAttribute("MaxSize", objectName, pw);
            printStatistics(pw, objectName);
            pw.println("   }" + (i == beanSet.size() - 1 ? "" : ","));
            i++;
        }
        pw.println("  ]");
        pw.println("}");
    }

    private void printStatistics(final PrintWriter pw, final ObjectName objectName) throws InstanceNotFoundException, IOException, ReflectionException, AttributeNotFoundException, MBeanException {
        String key = "Statistics";
        CompositeData statistics = (CompositeData) mBeanConnection.getAttribute(objectName, key);
        printSimpleStatistics(pw, statistics);
        printInUseConnections(statistics, pw);
    }

    private void printSimpleStatistics(final PrintWriter pw, final CompositeData statistics) throws InstanceNotFoundException, IOException, ReflectionException, AttributeNotFoundException, MBeanException {
        printCompositeDataAttribute("total", statistics, pw);
        printCompositeDataAttribute("inUse", statistics, pw);
        pw.println();
    }

    private void printInUseConnections(final CompositeData statistics, final PrintWriter pw) throws InstanceNotFoundException, IOException, ReflectionException, AttributeNotFoundException, MBeanException {
        String key = "inUseConnections";
        CompositeData[] compositeDataArray = (CompositeData[]) statistics.get(key);
        pw.println("     " + getKeyString(key) + ": [");
        for (int i = 0; i < compositeDataArray.length; i++) {
            CompositeData compositeData = compositeDataArray[i];
            pw.print("      { ");
            printCompositeDataAttribute("namespace", compositeData, pw);
            printCompositeDataAttribute("opCode", compositeData, pw);
            printCompositeDataAttribute("query", compositeData, pw, StringType.JSON);
            printCompositeDataAttribute("numDocuments", compositeData, pw);
            printCompositeDataAttribute("threadName", compositeData, pw);
            printCompositeDataAttribute("durationMS", compositeData, pw);
            printCompositeDataAttribute("localPort", compositeData, pw, Position.LAST);
            pw.println(" }" + (i == compositeDataArray.length -1 ? "" : ", "));
        }
        pw.println("     ]");
    }

    private void printCompositeDataAttribute(String key, final CompositeData compositeData, final PrintWriter pw) {
        printCompositeDataAttribute(key, compositeData, pw, Position.REGULAR);
    }

    private void printCompositeDataAttribute(String key, final CompositeData compositeData, final PrintWriter pw, Position position) {
        printCompositeDataAttribute(key, compositeData, pw, position, StringType.REGULAR);
    }

    private void printCompositeDataAttribute(final String key, final CompositeData compositeData, final PrintWriter pw, final StringType stringType) {
        printCompositeDataAttribute(key, compositeData, pw, Position.REGULAR, stringType);
    }

    private void printCompositeDataAttribute(String key, final CompositeData compositeData, final PrintWriter pw, Position position, StringType stringType) {
        printAttribute(key, compositeData.get(key), pw, position, stringType);
    }

    private void printAttribute(final String key, final ObjectName objectName, final PrintWriter pw) throws InstanceNotFoundException, IOException, ReflectionException, AttributeNotFoundException, MBeanException {
        printAttribute(key, mBeanConnection.getAttribute(objectName, key), pw);
    }

    private void printAttribute(final String key, final Object value, final PrintWriter pw) {
        printAttribute(key, value, pw, Position.REGULAR, StringType.REGULAR);
    }

    private void printAttribute(final String key, final Object value, final PrintWriter pw, Position position, StringType stringType) {
        if (value != null ) {
           pw.print(getKeyString(key) + ": " + getValueString(value, stringType) + (position == Position.LAST ? "" : ", "));
        }
    }

    private String getKeyString(final String key) {
        return Character.toLowerCase(key.charAt(0)) + key.substring(1);
    }

    private String getValueString(final Object value, final StringType stringType) {
        if (value instanceof String && stringType == StringType.REGULAR) {
            return "" + "'" + value + "'";
        }
        return value.toString();
    }

    enum StringType { REGULAR, JSON }

    enum Position { REGULAR, LAST}

    private final MBeanServerConnection mBeanConnection;
}
