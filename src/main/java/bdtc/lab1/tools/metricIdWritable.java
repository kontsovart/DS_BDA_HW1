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

@AllArgsConstructor
public class metricIdWritable implements WritableComparable<metricIdWritable> {

    @Setter
    @Getter
    private int metricId;
    @Setter
    @Getter
    private long metricTimestamp;
    @Setter
    @Getter
    private String scaleText;

    @Override
    public void readFields(DataInput in) throws IOException {
        metricId = in.readInt();
        metricTimestamp = in.readLong();
        scaleText = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(metricId);
        out.writeLong(metricTimestamp);
        out.writeUTF(scaleText);
    }

    @Override
    public int compareTo(metricIdWritable o) {
        return ComparisonChain.start().compare(metricId, o.metricId)
                .compare(metricTimestamp, o.metricTimestamp).compare(scaleText, o.scaleText).result();
    }

}