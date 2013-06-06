package org.gradle.api.plugins.clirr.reporters;

import net.sf.clirr.core.ApiDifference;

import java.util.List;
import java.util.Map;

public interface Reporter {
    void report(Map<String, List<ApiDifference>> differences);
}
