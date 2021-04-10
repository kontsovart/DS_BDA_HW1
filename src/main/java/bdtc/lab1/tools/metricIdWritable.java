package bdtc.lab1.tools;
import com.google.common.collect.ComparisonChain;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

@AllArgsConstructor
@NoArgsConstructor
public class metricIdWritable implements WritableComparable<metricIdWritable> {

    /**
     * Metric name representation
     */
    @Setter
    @Getter
    private String metricName;
    /**
     * Metric timestamp
     */
    @Setter
    @Getter
    private long metricTimestamp;
    /**
     * Scale value representation
     */
    @Setter
    @Getter
    private String scaleText;

    /**
     * Override of readFields() method
     * @param in output data to write
     * @throws IOException when IO operation fails
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        metricName = in.readUTF();
        metricTimestamp = in.readLong();
        scaleText = in.readUTF();
    }

    /**
     * Override of write() method
     * @param out output data to write
     * @throws IOException when IO operation fails
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(metricName);
        out.writeLong(metricTimestamp);
        out.writeUTF(scaleText);
    }

    /**
     * Override of compareTo() method
     */
    @Override
    public int compareTo(metricIdWritable o) {
        return ComparisonChain.start().compare(metricName, o.metricName)
                .compare(metricTimestamp, o.metricTimestamp).compare(scaleText, o.scaleText).result();
    }

    /**
     * Override of equals() method
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        metricIdWritable that = (metricIdWritable) o;
        return Objects.equals(metricName,that.metricName) && metricTimestamp == that.metricTimestamp && Objects.equals(scaleText, that.scaleText);
    }

    /**
     * Override of hashCode() method
     */
    @Override
    public int hashCode() {
        return Objects.hash(metricName, metricTimestamp, scaleText);
    }

    /**
     * Override of toString() method
     */
//    @Override
//    public String toString() {
//        return "metricIdWritable{" +
//                "metricId=" + metricName +
//                ", metricTimestamp=" + metricTimestamp +
//                ", scaleText='" + scaleText + '\'' +
//                '}';
//    }
    @Override
    public String toString() {
        return
                metricName +','+ metricTimestamp +','+ scaleText + ",";
    }
}
