#!/usr/bin/python

import os
import sys
import shutil
import simples3
import subprocess
import settings

if len( sys.argv ) == 1:
    raise Exception( "need version number for now" )

version = sys.argv[1]

p = subprocess.Popen( [ "/usr/bin/ant" , "jar" ] , stdout=subprocess.PIPE ).communicate()

if p[0].find( "SUCCESSFUL" ) < 0:
    print( p[0] )
    print( p[1] )
    raise( "build failed" )

dir = "org/mongodb/mongo-java-driver/" + version
if not os.path.exists( dir ):
    os.makedirs( dir )

root = dir + "/mongo-java-driver-" + version + "."

shutil.copy2( "mongo.jar" , root + "jar" )

pom  = "<project>\n"
pom += "  <modelVersion>4.0.0</modelVersion>\n"
pom += "  <groupId>org.mongodb</groupId>\n"
pom += "  <artifactId>mongo-java-driver</artifactId>\n"
pom += "  <name>MongoDB Java Driver</name>\n"
pom += "  <version>" + version + "</version>\n"
pom += "  <url>http://mongodb.org/</url>\n"
pom += "</project>\n"

out = open( root + "pom" , 'w' )
out.write( pom )
out.close()


s = simples3.S3Bucket( settings.bucket , settings.id , settings.key )
mavenRoot = "maven/" + root
s.put( mavenRoot + "jar" , open( "mongo.jar" , "rb" ).read() , acl="public-read" )
s.put( mavenRoot + "pom" , pom , acl="public-read" )
