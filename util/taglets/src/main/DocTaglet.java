/*
 * Copyright 2008-present MongoDB, Inc.
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

import com.sun.source.doctree.DocTree;
import com.sun.source.doctree.UnknownBlockTagTree;
import jdk.javadoc.doclet.Taglet;

import javax.lang.model.element.Element;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static jdk.javadoc.doclet.Taglet.Location.CONSTRUCTOR;
import static jdk.javadoc.doclet.Taglet.Location.FIELD;
import static jdk.javadoc.doclet.Taglet.Location.METHOD;
import static jdk.javadoc.doclet.Taglet.Location.OVERVIEW;
import static jdk.javadoc.doclet.Taglet.Location.PACKAGE;
import static jdk.javadoc.doclet.Taglet.Location.TYPE;

public abstract class DocTaglet implements Taglet {

    @Override
    public Set<Location> getAllowedLocations() {
        return new HashSet<>(asList(CONSTRUCTOR, METHOD, FIELD, OVERVIEW, PACKAGE, TYPE));
    }

    @Override
    public boolean isInlineTag() {
        return false;
    }

    @Override
    public String toString(List<? extends DocTree> tags, Element element) {
        if (tags.size() == 0) {
            return null;
        }

        StringBuilder buf = new StringBuilder(String.format("<dl><dt><span class=\"strong\">%s</span></dt>", getHeader()));
        for (DocTree tag : tags) {
            String text = ((UnknownBlockTagTree) tag).getContent().get(0).toString();
            buf.append("<dd>").append(genLink(text)).append("</dd>");
        }
        return buf.toString();
    }

    protected String genLink(final String text) {
        String relativePath = text;
        String display = text;

        int firstSpace = text.indexOf(' ');
        if (firstSpace != -1) {
            relativePath = text.substring(0, firstSpace);
            display = text.substring(firstSpace).trim();
        }

        return String.format("<a href='%s%s'>%s</a>", getBaseDocURI(), relativePath, display);
    }

    protected abstract String getHeader();

    protected abstract String getBaseDocURI();
}
