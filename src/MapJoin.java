import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.*;

/**
 * Created by hduser on 27/12/2015.
 */
public class MapJoin {

    public static class SortByKeyMapper extends Mapper<LongWritable, Text, TaggedKey, FatValue>{      //<input, input, output, output>
        private String fatKeyIndex;
        private List<String> indexList ;
        private Splitter splitter;
        private Joiner joiner;
        private TaggedKey taggedKey = new TaggedKey();
        private int joinOrder;
        private FatValue fatValue = new FatValue();
        private ArrayList<String> emptyList = new ArrayList<>();
        StringBuilder builder = new StringBuilder();

        private String findMyKeyIndex(Context context){
            StringBuilder builder = new StringBuilder();
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            builder.append("keyIndex/");
            String fileName = fileSplit.getPath().getName() ;
            String firstRep = "yes" ;
            if(firstRep.equals(context.getConfiguration().get("firstRep"))){
                builder.append(fileName);
            }
            else {
                if (fileName.equals(context.getConfiguration().get("simpleFile"))) {
                    builder.append(fileName);
                } else {
                    builder.append("part-m-00000");
                }
            }
            return builder.toString() ;
        }

        private List<String> getIndex(String strIndex){
            List<String> list = Lists.newArrayList(splitter.split(strIndex));
            list.remove(list.get(list.size()-1));
            return list;
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String separator = context.getConfiguration().get("separator");
            splitter = Splitter.on(separator).trimResults();
            joiner = Joiner.on(separator);
            String myIndex = findMyKeyIndex(context);                   //e.g. "keyIndex/1" = "0"
            fatKeyIndex = context.getConfiguration().get(myIndex);
            indexList = getIndex(fatKeyIndex);
            FileSplit fileSplit = (FileSplit)context.getInputSplit();

            String fileName = fileSplit.getPath().getName() ;
            String firstRep = "yes" ;
            if(firstRep.equals(context.getConfiguration().get("firstRep"))){}
            else {
                if (fileName.equals(context.getConfiguration().get("simpleFile"))) {
                } else {
                    fileName = "part-m-00000" ;
                }
            }

            joinOrder = Integer.parseInt(context.getConfiguration().get(fileName));
            emptyList.add("");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            List<String> values = Lists.newArrayList(splitter.split(value.toString()));     //kathe grammi
            String joinKey ;
            builder.setLength(0);
            for(String keyIndex : indexList){
                builder.append(values.get(Integer.parseInt(keyIndex))+"\t");
                values.set(Integer.parseInt(keyIndex),"");      //kanei to keyIndex element keno
            }
            builder.setLength(builder.length() - 1);
            joinKey = builder.toString();
            values.removeAll(emptyList);
            String valuesWithOutKey = joiner.join(values);

            taggedKey.set(joinKey, joinOrder);
            fatValue.set(valuesWithOutKey,joinOrder);
            context.write(taggedKey, fatValue);
        }
    }

    public static class SortByKeyReducer extends Reducer<TaggedKey,FatValue,Text,Text>{     //<input, input, output, output>
        private MultipleOutputs<Text,Text> mos ;
        FatValue fatvalue ;
        private Text combinedText = new Text();
        private Text keyOut = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<Text,Text>(context) ;
            //context.write(null,new Text(context.getConfiguration().get("header/1")));
            //mos.write("file2",null,new Text(context.getConfiguration().get("header/2")));
        }
        @Override
        protected void reduce(TaggedKey key, Iterable<FatValue> values, Context context) throws IOException, InterruptedException {
            Iterator<FatValue> iter = values.iterator() ;
            while(iter.hasNext()){
                fatvalue = iter.next();
                combinedText.set(fatvalue.toString());
                if( fatvalue.getJoinOrder() == 1  ){
                    keyOut.set(key.getJoinKey());
                    context.write(keyOut,combinedText); //sorted by key
                }else{
                    keyOut.set(key.getJoinKey());
                    mos.write("file2",keyOut,combinedText);
                }

            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }
}