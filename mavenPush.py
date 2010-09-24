#!/usr/bin/python

# http://repo1.maven.org/maven2/org/mongodb/mongo-java-driver/

import os
import sys
import shutil
import subprocess

if len( sys.argv ) == 1:
    print "Usage: mavenPush.py VERSION [PUBDIR]"
    print "VERSION - version you want to publish"
    print "PUBDIR - directory to publish to. default = /ebs/maven/"
    sys.exit()

version = sys.argv[1]

if len( sys.argv ) > 2:
    root = os.path.expanduser( sys.argv[2] )
else:
    root = "/ebs/maven/"

p = subprocess.Popen( [ "/usr/bin/ant" , "clean" ] , stdout=subprocess.PIPE ).communicate()
p = subprocess.Popen( [ "/usr/bin/ant" , "alljars" ] , stdout=subprocess.PIPE ).communicate()

if p[0].find( "SUCCESSFUL" ) < 0:
    print( p[0] )
    print( p[1] )
    raise( "build failed" )

def go( shortName , longName ):
    dir = root + "/org/mongodb/" + longName + "/" + version
    if not os.path.exists( dir ):
        os.makedirs( dir )

    fileRoot = dir + "/" + longName + "-" + version

    for x in [ "" , "-sources" , "-javadoc" ]:
        shutil.copy2( shortName + x + ".jar" , fileRoot + x + ".jar" )

    pom = open( "maven-" + shortName + ".xml" , "r" ).read()
    pom = pom.replace( "$VERSION" , version )

    out = open( fileRoot + ".pom" , 'w' )
    out.write( pom )
    out.close()

    p = subprocess.Popen( [ "sha1sum" , fileRoot + ".jar" ] , stdout=subprocess.PIPE ).communicate()
    sha1 = p[0].split( ' ' )[0]
    out = open( fileRoot + ".jar.sha1" , 'w' )
    out.write( sha1 )
    out.close()


go( "mongo" , "mongo-java-driver" )
go( "bson" , "bson" )
