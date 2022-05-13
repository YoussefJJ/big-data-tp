
 import org.apache.hadoop.io.DoubleWritable;
 import org.apache.hadoop.io.IntWritable;
         import org.apache.hadoop.io.Text;
         import org.apache.hadoop.mapreduce.Mapper;

         import java.io.IOException;
         import java.util.StringTokenizer;

public class TokenizerMapper
        extends Mapper<Object, Text, Text, IntWritable>{

    private final static DoubleWritable distance = new DoubleWritable();
    private Text uber = new Text();

    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",",0);
        uber.set(tokens[1]);
        distance.set(Double.parseDouble(tokens[0]));
        context.write(uber, distance);

    }
}