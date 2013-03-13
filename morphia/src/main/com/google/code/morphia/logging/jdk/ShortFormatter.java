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

package com.google.code.morphia.logging.jdk;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class ShortFormatter extends Formatter {
    private static final MessageFormat messageFormat = new MessageFormat("[{1}|{2}|{3,date,h:mm:ss}]{0} :{4}");

    public ShortFormatter() {
        super();
    }

    @Override
    public String format(final LogRecord record) {
        final StringBuilder sb = new StringBuilder();
        String source = record.getSourceClassName() == null ? record.getLoggerName() : record.getSourceClassName();
        source = source.substring(source.length() - 15) + "."
                 + (record.getSourceMethodName() == null ? "" : record.getSourceMethodName());

        final Object[] arguments = new Object[6];
        arguments[0] = source;
        arguments[1] = record.getLevel();
        arguments[2] = Thread.currentThread().getName();
        arguments[3] = new Date(record.getMillis());
        arguments[4] = record.getMessage();
        sb.append(messageFormat.format(arguments));

        if (record.getThrown() != null) {
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            record.getThrown().printStackTrace(pw);
            pw.close();
            sb.append(sw.toString());
        }
        sb.append("\n");
        return sb.toString();
    }

}
