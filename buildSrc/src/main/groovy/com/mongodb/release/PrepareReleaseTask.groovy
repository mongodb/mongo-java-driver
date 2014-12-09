package com.mongodb.release

import org.eclipse.jgit.api.Git
import org.eclipse.jgit.api.errors.JGitInternalException
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.tasks.TaskAction
import org.slf4j.Logger
import org.slf4j.LoggerFactory

// requires you to populate github.credentials.username & github.credentials.password in ~/.gradle/gradle.properties
// uses http://wiki.eclipse.org/JGit/ 
class PrepareReleaseTask extends DefaultTask {

    PrepareReleaseTask (){
        description = 'Update release version in the build file, commit to github and tag the release'
        mustRunAfter project.subprojects.clean
    }

    @TaskAction
    def prepareGitForRelease() {
        def releaseVersion = System.getProperty('releaseVersion')
        if (!releaseVersion) {
            throw new GradleException('When doing a full release, you need to specify a value for releaseVersion. For example, ' +
                                      './gradlew release -DreleaseVersion=3.0.0')
        }
        def snapshotVersion = releaseVersion + '-SNAPSHOT'
        getLog().info "Updating ${project.release.filesToUpdate} from ${snapshotVersion} to ${releaseVersion}"

        project.release.filesToUpdate.each {
            project.ant.replaceregexp(file: it, match: snapshotVersion, replace: releaseVersion) 
        }

        getLog().info 'Checking files into git'
        def git = Git.open(new File('.'))
        try {
            git.commit()
               .setAll(true)
               .setMessage("Release ${releaseVersion}")
               .call()
        } catch (JGitInternalException e) {
            if (e.getMessage().equals('No changes')) {
                // we probably already committed these files, we're done
                // this is not an elegant way to make this idempotent, but it does work
                return
            }
        }

        getLog().info "Tagging release with 'r${releaseVersion}'"
        git.tag()
           .setName("r${releaseVersion}")
           .setMessage("Release ${releaseVersion}")
           .setForceUpdate(true)
           .call()

        getLog().info 'Pushing changes using the credentials stored in ~/.gradle/gradle.properties'
        String username = project.property("github.credentials.username")
        String password = project.property("github.credentials.password")

        git.push()
           .setCredentialsProvider(new UsernamePasswordCredentialsProvider(username, password))
           .setPushTags()
           .call()
    }

    private Logger getLog() { project?.logger ?: LoggerFactory.getLogger(this.class) }
}
