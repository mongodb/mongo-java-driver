/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.gradle.api.plugins.clirr

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.Dependency
import org.gradle.api.logging.LogLevel
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.plugins.ReportingBasePlugin
import org.gradle.api.reporting.ReportingExtension

class ClirrPlugin implements Plugin<Project> {

    void apply(Project project) {
        project.apply(plugin: JavaBasePlugin)
        project.apply(plugin: ReportingBasePlugin)

        def reportingExtension = project.extensions.getByType(ReportingExtension)

        def extension = project.extensions.create("clirr", ClirrPluginExtension)

        Configuration clirrConfiguration = project.configurations.add("clirr")
        Dependency clirrDependency = project.dependencies.create('net.sf.clirr:clirr-core:0.6')
        clirrConfiguration.dependencies.add(clirrDependency)

        extension.conventionMapping.with {
            map('reportsDir') { reportingExtension.file("clirr") }
            map('baseline') { "$project.group:$project.name:(,$project.version)".toString() }
            map('formats') { ['plain'] }
            map('failOnBinWarning') { false }
            map('failOnBinError') { true }
            map('failOnSrcWarning') { false }
            map('failOnSrcError') { true }
        }


        def jarTask = project.tasks.getByName("jar");

        def clirrTask = project.task('clirr', dependsOn: jarTask) << {

            Configuration configuration = project.configurations.add("baseline").setTransitive(false)
            configuration.dependencies.add(project.dependencies.create(extension.baseline))

            File jar = configuration.resolve().find { it.name.endsWith('.jar') }

            ant.taskdef(resource: 'clirrtask.properties', classpath: project.configurations.clirr.asPath)

            extension.reportsDir.mkdirs()

            inputs.file jar
            outputs.dir extension.reportsDir

            ant.clirr(
                    failOnBinWarning: extension.failOnBinWarning,
                    failOnBinError: extension.failOnBinError,
                    failOnSrcWarning: extension.failOnSrcWarning,
                    failOnSrcError: extension.failOnSrcError
            ) {
                origfiles(file: jar.getPath())
                newfiles(dir: jarTask.destinationDir, includes: jarTask.archiveName)
                extension.formats.each { format ->
                    formatter(type: format, outfile: "$extension.reportsDir/report.${format == 'xml' ? 'xml' : 'txt'}")
                }
            }
        }

        project.tasks.getByName("check").dependsOn(clirrTask)

    }

}