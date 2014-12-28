package com.mongodb.release

import org.eclipse.jgit.api.Git
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.tasks.TaskAction
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class UpdateToNextVersionTask extends DefaultTask {

    UpdateToNextVersionTask() {
        description = 'Update the version in the build file to the next SNAPSHOT version and commit'
        mustRunAfter: 'draftReleaseNotes'
    }

    @TaskAction
    def updateToNextVersion() {
        def oldVersion = System.getProperty('releaseVersion')
        if (!oldVersion) {
            throw new GradleException('When doing a full release, you need to specify a value for releaseVersion. For example, ' +
                                      './gradlew release -DreleaseVersion=3.0.0')
        }
        def newVersion = incrementToNextVersion(oldVersion)
        def buildFile = project.file('build.gradle')
        project.release.filesToUpdate.each {
            project.ant.replaceregexp(file: it, match: oldVersion, replace: newVersion)
        }

        def git = Git.open(new File('.'))
        git.commit()
           .setAll(true)
           .setMessage("Updated to next development version: ${newVersion}")
           .call()

        getLog().info 'Pushing changes using the credentials stored in ~/.gradle/gradle.properties'
        String username = project.property("github.credentials.username")
        String password = project.property("github.credentials.password")

        git.push()
           .setCredentialsProvider(new UsernamePasswordCredentialsProvider(username, password))
           .call()
    }

    static incrementToNextVersion(String old) {
        String[] split = old.split('\\.')
        def next = (split.last() as int) + 1

        def updated = split[0..-2].join('.')
        updated += ".${next}-SNAPSHOT"
        updated
    }

    private Logger getLog() { project?.logger ?: LoggerFactory.getLogger(this.class) }
}
