package bdtc.lab1.tools;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.net.URI;

public class MetricNameResolver {

    /**
     * Static method that reads mapping
     * @param context hadoop configuration class
     * @return Map id -> name
     * @throws IOException when file can not be opened or file has incorrect internal structure
     */
    public static Map<Integer, String> resolve(TaskInputOutputContext context) throws IOException {
        URI[] paths = context.getCacheFiles();
        Path path = null;
        for(URI u : paths) {
            if (u.getPath().toLowerCase().contains("metric")) {
                path = new Path(u.getPath());
            }
        }
        if (path == null) {
            throw new IOException();
        }
        Map<Integer, String> metricMap = new HashMap<>();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FSDataInputStream(fs.open(path))));
        String line;
        int metricId;
        while ((line = reader.readLine()) != null){
            try {
                String[] words = line.split(",");
                metricId = Integer.parseInt(words[0]);
                metricMap.put(metricId, words[1]);
            }
            catch (NumberFormatException noexcept) {
                throw new IOException();
            }
        }
        return metricMap;
    }
}
