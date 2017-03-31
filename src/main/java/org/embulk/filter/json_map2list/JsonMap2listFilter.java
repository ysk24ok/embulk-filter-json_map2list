package org.embulk.filter.json_map2list;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.ParseContext;

import org.embulk.filter.json_map2list.JsonMap2listFilterPlugin.PluginTask;
import org.embulk.spi.Column;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class JsonMap2listFilter
{
    private PluginTask task;

    JsonMap2listFilter(PluginTask task)
    {
        this.task = task;
    }

    public String doFilter(String json)
    {
        String targetKeyPath = task.getTargetKeyPath();
        String keyName = task.getKeyName();
        String jsonPath = String.format("$.%s", targetKeyPath);
        Map<String, Map> m = JsonPath.parse(json).read(jsonPath);
        List<Map> l = new ArrayList<Map>();
        for (Map.Entry<String, Map> entry: m.entrySet()) {
            entry.getValue().put(keyName, entry.getKey());
            l.add(entry.getValue());
        }
        return JsonPath.parse(json).set(jsonPath, l).jsonString();
    }
}
