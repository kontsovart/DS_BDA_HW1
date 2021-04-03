//package bdtc.lab1;
//
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer;
//
//import java.io.IOException;
//
///**
// * Редьюсер: суммирует все единицы полученные от {@link HW1Mapper}, выдаёт суммарное количество пользователей по браузерам
// */
//public class HW1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//
//    @Override
//    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//        int sum = 0;
//        while (values.iterator().hasNext()) {
//            sum += values.iterator().next().get();
//        }
//        context.write(key, new IntWritable(sum));
//    }
//}


package bdtc.lab1;

import bdtc.lab1.tools.MetricNameResolver;
import bdtc.lab1.tools.metricIdWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * Reducer class
 * Input key {@link metricIdWritable}
 * Input value {@link FloatWritable}
 * Output key {@link metricIdWritable}
 * Output value {@link FloatWritable}
 */
public class HW1Reducer extends Reducer<metricIdWritable, FloatWritable, metricIdWritable, FloatWritable> {

    /**
     * Mapping device_id -> device_name
     */
    private Map<Integer, String> metricName;

    /**
     * Initial reducer setup
     * @param context reducer context
     * @throws IOException in MappingReader.read()
     */
    @Override
    protected void setup(Context context) throws IOException {
        metricName = MetricNameResolver.resolve(context);
    }

    /**
     * Reduce function. Calculates average value in given interval
     * @param metric key
     * @param values iterable of values
     * @param context reducer context
     * @throws IOException in context.write()
     * @throws InterruptedException in context.write()
     */
    @Override
    protected void reduce(metricIdWritable metric, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        float sum = 0.0F;
        float count = 0.0F;
        while (values.iterator().hasNext()) {
            sum += values.iterator().next().get();
            count += 1;
        }
        context.write(new metricIdWritable(metric.getMetricId(), metric.getMetricTimestamp(), metric.getScaleText()),
                new FloatWritable(sum/count));
    }
}