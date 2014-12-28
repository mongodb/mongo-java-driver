package com.mongodb.release

import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.tasks.TaskAction
import org.kohsuke.github.GitHub
import org.slf4j.Logger
import org.slf4j.LoggerFactory

//this class needs you to have your credentials in ~/.github
class DraftReleaseNotesTask extends DefaultTask {
    String repositoryName = "mongodb/mongo-java-driver"

    DraftReleaseNotesTask() {
        description = 'Creates the release notes document'
        mustRunAfter 'prepareRelease', project.subprojects.publish
    }

    @TaskAction
    void draftReleaseNotes() {
        def releaseVersion = System.getProperty('releaseVersion')
        if (!releaseVersion) {
            throw new GradleException('When doing a full release, you need to specify a value for releaseVersion. For example, ' +
                                      './gradlew release -DreleaseVersion=3.0.0')
        }

        def notes = createDraftReleaseNotesContent(releaseVersion, new Date())
        def githubRelease = GitHub.connect().getRepository(repositoryName)
                                  .createRelease("r${releaseVersion}")
                                  .name("Java Driver ${releaseVersion} (${new Date().format('MMMMMM dd, yyyy')})")
                                  .body(notes.toString())
                                  .draft(true)
                                  .create()
        attachJarFilesToRelease(githubRelease)
    }

    private attachJarFilesToRelease(ghRelease) {
        def log = getLog()
        project.release.jarFiles.each { jarFile ->
            log.info "Uploading ${jarFile.name}"
            ghRelease.uploadAsset(jarFile, "application/jar")
        }
    }

    static createDraftReleaseNotesContent(releaseVersion, date) {
        """
## Version ${releaseVersion} (${date.format("MMM dd, yyyy")})

### Notes

### Downloads

Below and on maven central

### Docs

http://api.mongodb.org/java/${releaseVersion}/

### Compatibility

### Bug fixes

You can find a full list of bug fixes [here](https://jira.mongodb.org/issues/?jql=project%20%3D%20JAVA%20AND%20issuetype%20%3D%20Bug%20AND%20fixVersion%20%3D%20%22${releaseVersion}%22).

### New Features

You can find a full list of new features [here](https://jira.mongodb.org/issues/?jql=project%20%3D%20JAVA%20AND%20issuetype%20in%20(%22New%20Feature%22%2C%20Task)%20AND%20fixVersion%20%3D%20%22${releaseVersion}%22).

### Improvements

You can find a full list of improvements [here](https://jira.mongodb.org/issues/?jql=project%20%3D%20JAVA%20AND%20issuetype%20%3D%20Improvement%20AND%20fixVersion%20%3D%20%22${releaseVersion}%22).
"""
    }

    private Logger getLog() { project?.logger ?: LoggerFactory.getLogger(this.class) }
}
