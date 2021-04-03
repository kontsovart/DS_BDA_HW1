package bdtc.lab1;
import bdtc.lab1.tools.MetricNameResolver;
import bdtc.lab1.tools.metricIdWritable;
import bdtc.lab1.CounterType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.events.EventException;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.regex.Pattern;


import java.util.concurrent.TimeUnit ;


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


    /**
     * Initial mapper setup
     * @param context mapper context
     * @throws IOException if input data parsing fails due to wrong format
     */
    @Override
    protected void setup(Context context) throws IOException{
        scale = Integer.parseInt(context.getConfiguration().get("interval"));
        try {
            scaleText = "";
            long millis = scale % 1000;
            long second = (scale / 1000) % 60;
            long minute = (scale / (1000 * 60)) % 60;
            long hour = (scale / (1000 * 60 * 60)) % 24;

            if (hour> 0){
                scaleText += String.format("%dh ", hour);
            }
            if (minute> 0){
                scaleText += String.format("%dm ", minute);
            }
            if (second> 0){
                scaleText += String.format("%ds ", second);
            }
            if (millis> 0){
                scaleText += String.format("%dms ", millis);
            }

            metricName = MetricNameResolver.resolve(context);

        }
        catch (Exception exception){
            throw new IOException();
        }
    }

    /**
     * Map function. Truncates timestamp according to interval for aggregating. Checks data for correctness.
     * Uses counters {@link CounterType}
     * @param key input key
     * @param value input value
     * @param context mapper context
     * @throws IOException from context.write()
     * @throws InterruptedException from context.write()
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
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
                context.write(new metricIdWritable(metricId, metricTimestamp, scaleText), new FloatWritable(metricValue));
                if (metricName.containsKey(metricId)){
                    context.write(new metricIdWritable(metricId, metricTimestamp, scaleText), new FloatWritable(metricValue));
                }
                else throw new Exception(String.format("Unrecognized metricId %d at %d", metricId, key.get()));
            }
            else throw new Exception(String.format("Len string params != 3 at %d", key.get()));
        }
        catch (Exception error){
            context.getCounter(CounterType.MALFORMED).increment(1);
        }
    }
}
