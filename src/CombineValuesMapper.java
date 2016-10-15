import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.*;


public class CombineValuesMapper extends Mapper<Text, TupleWritable, NullWritable, Text> {

    private static final NullWritable nullKey = NullWritable.get();
    private Text outValue = new Text();
    private StringBuilder valueBuilder = new StringBuilder();
    private String separator;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        separator = context.getConfiguration().get("separator");
        //context.write(null,new Text("x1 x2 x3"));
        context.write(null,new Text(context.getConfiguration().get("header")));
    }

    @Override
    protected void map(Text key, TupleWritable value, Context context) throws IOException, InterruptedException {
        valueBuilder.append(key).append(separator);

        //String test0 = key.toString();

        for (Writable writable : value) {
            String test1 = writable.toString();
            if(!test1.equals(""))
                valueBuilder.append(writable.toString()).append(separator);
        }
        valueBuilder.setLength(valueBuilder.length() - 1);
        outValue.set(valueBuilder.toString());
        context.write(nullKey, outValue);
        valueBuilder.setLength(0);
    }
}