package com.mongodb.release

import org.gradle.api.tasks.bundling.Jar

class ReleasePluginExtension {
    List<File> filesToUpdate = []
    String releaseVersion = System.getProperty('releaseVersion')
    Set<File> jarFiles = new HashSet<File>()

    def jarFile(Jar jarFile) {
        return jarFiles.add(jarFile.archivePath)
    }

}
