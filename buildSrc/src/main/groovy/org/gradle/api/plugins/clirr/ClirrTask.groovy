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

import net.sf.clirr.core.Checker
import net.sf.clirr.core.CheckerException
import net.sf.clirr.core.ClassFilter
import net.sf.clirr.core.XmlDiffListener
import net.sf.clirr.core.spi.DefaultTypeArrayBuilderFactory
import net.sf.clirr.core.spi.JavaType
import net.sf.clirr.core.spi.TypeArrayBuilder
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.FileCollection
import org.gradle.api.plugins.clirr.reporters.CountReporter
import org.gradle.api.plugins.clirr.reporters.HtmlReporter
import org.gradle.api.plugins.clirr.reporters.Reporter
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.TaskAction

import static net.sf.clirr.core.internal.ClassLoaderUtil.createClassLoader

class ClirrTask extends DefaultTask {

    public static final String REPORT_NAME = 'compatibility-report'

    @InputFiles
    FileCollection oldClasspath

    @InputFiles
    FileCollection newClasspath;

    @InputFiles
    FileCollection newFiles

    @TaskAction
    void run() {
        addBaselineDependency()
        project.clirr.reportsDir.mkdirs()

        final Checker checker = new Checker();

        final BufferedListener bufferedListener = new BufferedListener(
                project.clirr.ignoredDifferenceTypes,
                project.clirr.ignoredPackages
        );
        checker.addDiffListener(bufferedListener)

        checker.addDiffListener(new XmlDiffListener("${project.clirr.reportsDir}/${REPORT_NAME}.xml"))

        try {
            final JavaType[] origClasses = createClassSet(oldClasspath as File[]);
            final JavaType[] newClasses = createClassSet((newClasspath + newFiles) as File[])

            checker.reportDiffs(origClasses, newClasses);
        } catch (CheckerException ex) {
            throw new GradleException("Can't execute 'clirr' task", ex);
        }

        final Reporter reporter = new HtmlReporter(new FileWriter("${project.clirr.reportsDir}/${REPORT_NAME}.html"))
        reporter.report(bufferedListener.differences)

        if (!project.clirr.ignoreFailures) {
            final CountReporter counter = new CountReporter();
            counter.report(bufferedListener.differences);
            if (counter.getSrcErrors() > 0) {
                throw new GradleException("There are several compatibility issues. \nPlease check ${project.clirr.reportsDir}/${REPORT_NAME}.html for more information");
            }
        }


    }

    private JavaType[] createClassSet(final File[] files) {
        final ClassLoader classLoader = createClassLoader(files as String[]);

        final DefaultTypeArrayBuilderFactory tabFactory = new DefaultTypeArrayBuilderFactory();
        final TypeArrayBuilder tab = tabFactory.build();

        return tab.createClassSet(files, classLoader, new ClassSelector());
    }

    private void addBaselineDependency() {
        project.dependencies {
            base project.clirr.baseline
        }
    }

    private static class ClassSelector implements ClassFilter {

        @Override
        boolean isSelected(final JavaType javaType) {
            return true;
        }
    }
}