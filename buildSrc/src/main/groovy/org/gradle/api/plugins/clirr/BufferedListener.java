package org.gradle.api.plugins.clirr;

import net.sf.clirr.core.ApiDifference;
import net.sf.clirr.core.DiffListenerAdapter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BufferedListener extends DiffListenerAdapter {
    private final Map<String, List<ApiDifference>> differences;
    private final List<Integer> ignoredDifferenceTypes;
    private final List<String> ignoredPackages;

    public BufferedListener(final List<Integer> ignoredDifferenceTypes, final List<String> ignoredPackages) {
        this.ignoredDifferenceTypes = ignoredDifferenceTypes;
        this.ignoredPackages = ignoredPackages;
        this.differences = new HashMap<String, List<ApiDifference>>();
    }


    @Override
    public void reportDiff(final ApiDifference difference) {
        if (ignoredDifferenceTypes.contains(difference.getMessage().getId())) {
            return;
        }

        final String affectedClass = difference.getAffectedClass();

        for (String pkg : ignoredPackages){
            if (affectedClass.startsWith(pkg)) {
                return;
            }
        }

        if (!differences.containsKey(affectedClass)) {
            differences.put(affectedClass, new ArrayList<ApiDifference>());
        }
        differences.get(affectedClass).add(difference);
    }

    public Map<String, List<ApiDifference>> getDifferences() {
        return differences;
    }
}
