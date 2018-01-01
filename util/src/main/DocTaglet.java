/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.sun.javadoc.Tag;
import com.sun.tools.doclets.Taglet;

public abstract class DocTaglet implements Taglet {

    public boolean inConstructor() {
        return true;
    }

    public boolean inField() {
        return true;
    }

    public boolean inMethod() {
        return true;
    }

    public boolean inOverview() {
        return true;
    }

    public boolean inPackage() {
        return true;
    }

    public boolean inType() {
        return true;
    }

    public boolean isInlineTag() {
        return false;
    }

    public String toString(final Tag[] tags) {
        if (tags.length == 0) {
            return null;
        }

        StringBuilder buf = new StringBuilder(String.format("%n<dl><dt><span class=\"strong\">%s</span></dt>%n", getHeader()));
        for (Tag t : tags) {
            buf.append("   <dd>").append(genLink(t.text())).append("</dd>%n");
        }
        return buf.toString();
    }

    protected abstract String getHeader();

    public String toString(final Tag tag) {
        return toString(new Tag[]{tag});
    }

    protected String genLink(final String text) {
        String relativePath = text;
        String display = text;

        int firstSpace = text.indexOf(' ');
        if (firstSpace != -1) {
            relativePath = text.substring(0, firstSpace);
            display = text.substring(firstSpace, text.length()).trim();
        }

        return String.format("<a href='%s%s'>%s</a>", getBaseDocURI(), relativePath, display);
    }

    protected abstract String getBaseDocURI();
}
