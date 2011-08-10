#!/usr/bin/python

# http://repo1.maven.org/maven2/org/mongodb/mongo-java-driver/

from xml.dom.minidom import parseString
import datetime
import os
import sys
import shutil
import subprocess
import time

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

def build_metadata_xml(path, artifactid):
    groupid = os.path.split(path)[1]
    xml = '<metadata>'
    xml += '<groupId>org.%s</groupId>' % (groupid,)
    xml += '<artifactId>%s</artifactId>' % (artifactid,)
    xml += '<versioning><versions>'

    entries = os.listdir(path)
    for entry in entries:
        if os.path.isdir(os.path.join(path, entry)):
            xml += '<version>%s</version>' % (entry,)
    xml += '</versions>'
    xml += '<lastUpdated>%s</lastUpdated>' % (int(time.time()*1000),)
    xml += '</versioning></metadata>'

    doc = parseString(xml)
    return doc.documentElement.toprettyxml()

def go( pkgName, shortName , longName ):
    dir = root + pkgName + longName + "/" + version
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

    out = open( os.path.join(os.path.split(dir)[0], "maven-metadata.xml") , 'w' )
    out.write( build_metadata_xml(os.path.split(dir)[0], longName) )
    out.close()

go( "/org/mongodb/" , "mongo" , "mongo-java-driver" )
go( "/org/bson/" , "bson" , "bson" )
