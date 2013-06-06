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
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.plugins.ReportingBasePlugin
import org.gradle.api.reporting.ReportingExtension

class ClirrPlugin implements Plugin<Project> {

    private ClirrPluginExtension extension

    @Override
    void apply(Project project) {
        project.plugins.apply(ReportingBasePlugin)
        project.plugins.apply(JavaBasePlugin)
        createConfigurations(project)
        createExtension(project)
        addClirrTask(project)
    }

    void createConfigurations(Project project) {
        project.configurations.create('clirr').with {
            visible = false
            transitive = true
            description = "The Clirr libraries to be used for this project."
            return (Configuration) delegate
        }

        project.configurations.create('base').with {
            visible = false
            transitive = false
            return (Configuration) delegate
        }
    }

    void createExtension(Project project) {
        extension = project.extensions.create('clirr', ClirrPluginExtension).with {
            reportsDir = project.extensions.getByType(ReportingExtension).file('clirr')
            baseline = "$project.group:$project.archivesBaseName:+".toString()
            ignoreFailures = false
            return (ClirrPluginExtension) delegate
        }
    }


    void addClirrTask(Project project) {
        ClirrTask task = project.tasks.create('clirr', ClirrTask)

        task.with {
            oldClasspath = project.configurations['base']
            newFiles = project.tasks['jar'].outputs.files
            newClasspath = project.configurations['compile']
        }


        project.tasks.getByName("check").dependsOn(task)
    }

}