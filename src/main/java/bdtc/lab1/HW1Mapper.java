package bdtc.lab1;
import bdtc.lab1.tools.MetricNameResolver;
import bdtc.lab1.tools.metricIdWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

/**
 * Mapper class
 * Input key {@link LongWritable}
 * Input value {@link Text}
 * Output key {@link metricIdWritable}
 * Output value {@link FloatWritable}
 */
public class HW1Mapper extends Mapper<LongWritable, Text, metricIdWritable, FloatWritable> {
    private int scale;
    private String scaleText;
    private Map<Integer, String> metricName;
    private final metricIdWritable fixedMetric = new metricIdWritable();
    private final FloatWritable fixedFloat = new FloatWritable();


    /**
     * Initial mapper setup
     * @param context mapper context
     * @throws IOException failed to parse metric file or scale
     */
    @Override
    protected void setup(Context context) throws IOException{
//
//        metricName = MetricNameResolver.resolve(context);
//        try {
//            scale = Integer.parseInt(context.getConfiguration().get("metricScale"));
//            scaleText = "";
//            long millis = scale % 1000;
//            long second = (scale / 1000) % 60;
//            long minute = (scale / (1000 * 60)) % 60;
//            long hour = (scale / (1000 * 60 * 60)) % 24;
//
//            if (hour> 0){
//                scaleText += String.format("%dh", hour);
//            }
//            if (minute> 0){
//                scaleText += String.format("%dm", minute);
//            }
//            if (second> 0){
//                scaleText += String.format("%ds", second);
//            }
//            if (millis> 0){
//                scaleText += String.format("%dms", millis);
//            }
//
////            metricName = MetricNameResolver.resolve(context);
//
//        }
//        catch (Exception exception){
//            throw new IOException();
//        }
        metricName = MetricNameResolver.resolve(context);
        scale = Integer.parseInt(context.getConfiguration().get("metricScale"));
        scaleText = "";
        long millis = scale % 1000;
        long second = (scale / 1000) % 60;
        long minute = (scale / (1000 * 60)) % 60;
        long hour = (scale / (1000 * 60 * 60)) % 24;

        if (hour> 0){
            scaleText += String.format("%dh", hour);
        }
        if (minute> 0){
            scaleText += String.format("%dm", minute);
        }
        if (second> 0){
            scaleText += String.format("%ds", second);
        }
        if (millis> 0){
            scaleText += String.format("%dms", millis);
        }
    }

    /**
     * Map function. Truncates timestamp according to interval for aggregating. Checks data for correctness.
     * Uses counters {@link CounterType}
     * @param key input key
     * @param value input value
     * @param context mapper context
     */
    @Override
    protected void map(LongWritable key, Text value, Context context){
        int metricId;
        long metricTimestamp;
        float metricValue;
        try {
            String[] words = value.toString().split(",");
            if (words.length == 3) {
                metricId = Integer.parseInt(words[0]);
                metricTimestamp = Long.parseLong(words[1]);
                metricValue = Float.parseFloat(words[2]);
                metricTimestamp = metricTimestamp - metricTimestamp % scale;
                if (metricName.containsKey(metricId)){
                    fixedMetric.setMetricName(metricName.get(metricId));
                    fixedMetric.setMetricTimestamp(metricTimestamp);
                    fixedMetric.setScaleText(scaleText);
                    fixedFloat.set(metricValue);
                    context.write(fixedMetric, fixedFloat);
                }
                else throw new IOException(String.format("Unrecognized metricId %d at %d", metricId, key.get()));
            }
            else throw new IOException(String.format("Len string params != 3 at %d", key.get()));
        }
        catch (IOException | InterruptedException error){
            context.getCounter(CounterType.MALFORMED).increment(1);
        }
    }
}
