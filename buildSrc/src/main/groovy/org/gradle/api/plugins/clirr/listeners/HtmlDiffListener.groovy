package groovy.org.gradle.api.plugins.clirr.listeners

import net.sf.clirr.core.ApiDifference
import net.sf.clirr.core.FileDiffListener

class HtmlDiffListener extends FileDiffListener {

    def HtmlDiffListener(final String outFile) throws FileNotFoundException {
        super(outFile)
    }

    public void reportDiff(ApiDifference difference) {
        PrintStream out = getOutputStream();
        out.print("  <" + DIFFERENCE);
        out.print(" binseverity=\"" + difference.getBinaryCompatibilitySeverity() + "\"");
        out.print(" srcseverity=\"" + difference.getSourceCompatibilitySeverity() + "\"");
        out.print(" class=\"" + difference.getAffectedClass() + "\"");
        if (difference.getAffectedMethod() != null) {
            out.print(" method=\"" + difference.getAffectedMethod() + "\"");
        }
        if (difference.getAffectedField() != null) {
            out.print(" field=\"" + difference.getAffectedField() + "\"");
        }
        out.print(">");
        out.print(difference.getReport(translator)); // TODO: XML escapes??
        out.println("</" + DIFFERENCE + '>');
    }

    public void start() {
        PrintStream out = getOutputStream();
        out.println("<?xml version=\"1.0\"?>");
        out.println('<' + DIFFREPORT + '>');
    }

    protected void writeFooter() {
        PrintStream out = getOutputStream();
        out.println("</" + DIFFREPORT + '>');
    }
}
