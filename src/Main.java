import com.google.common.base.Splitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by hduser on 28/12/2015.
 */

public class Main {

    public static void main(String[] args) throws Exception {
        if(args.length != 2)
        {
            System.out.println("Proper Usage is: java program inputFolder numberOfReducers");
            System.exit(0);
        }

        long tStart = System.currentTimeMillis();

        //My vars
        String input = args[0];
        int no_of_reducers = Integer.parseInt(args[1]);
        boolean firstTime =true;
        String separator = "\t";
        int nextFileIndex = 0;
        String nextFile = null;
        StringBuilder filePaths = new StringBuilder();
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(config);


        //Get number of files
        Path pt = new Path(input);
        ContentSummary cs = fs.getContentSummary(pt);
        long fileCount = cs.getFileCount();             //prosoxi sta hidden files

        //KeyFinder
        MyIndex myIndex ;
        Path path1 ;
        Path path2 ;
        //Optimization
        long num_of_joins = Cascade_setup.number_of_iterations(args[0], fs);
        String[] filenames = Cascade_setup.create_filenames(args[0], num_of_joins + 1);
        int[] max_pair = InputOptimization.Outer(filenames,fs, separator);
        String firstFileName = filenames[max_pair[0]] ;
        filenames[max_pair[0]] = null ;
        nextFileIndex = max_pair[1] ;

        Job job = null;
        for( int i=1 ; i < fileCount ; i++  ) {           //i < 6
            if(firstTime) {
                config.set(String.valueOf(max_pair[0]+1), "1");     //px file 1 -> proteraitotia 1
                config.set(String.valueOf(max_pair[1]+1), "2");
                filePaths.append(firstFileName).append(",");
                filePaths.append(filenames[max_pair[1]]).append(",");
                path1 = new Path(firstFileName);
                path2 = new Path(filenames[max_pair[1]]);
                config.set("firstRep","yes");
                //firstTime = false ;
            }else{
                config.set(String.valueOf(nextFileIndex+1), "1");
                config.set("part-m-00000", "2");
                filePaths.append(nextFile).append(",");
                for(int j =0 ;fs.exists(new Path(String.valueOf(i+fileCount-1)+"m"+"/"+"part-m-" + String.format("%05d", j)));j++){
                    filePaths.append(String.valueOf(i+fileCount-1)+"m"+"/"+"part-m-" + String.format("%05d", j) + ",");
                }
                //filePaths.setLength(filePaths.length() - 1);
                path1 = new Path(nextFile);                             //arxeio apo ta palia
                path2 = new Path(String.valueOf(i+fileCount-1)+"m"+"/"+"part-m-00000");         //arxeio apo ta nea
                config.set("firstRep","no");
            }

            KeyFinder keyFinder = new KeyFinder();
            keyFinder.setPaths(path1,path2);
            myIndex = keyFinder.findKey(config, Splitter.on(separator));      //exw ta key values e.g "01"
            myIndex.packName1(path1.getName());
            myIndex.packName2(path2.getName());                         //key names e.g "keyIndex/1"

            String name1 ,name2 ;
            String index1 ,index2 ;
            name1 = myIndex.getName1();
            name2 = myIndex.getName2();
            index1 = myIndex.getStr_list1();
            index2 = myIndex.getStr_list2();

            config.set(name1,index1);      // "keyIndex/1" ,"0"
            config.set("simpleFile",String.valueOf(nextFileIndex+1));
            config.set(name2,index2);      // "keyIndex/2" , "0"

            myIndex.fixHeaders(path1,path2,config,Splitter.on(separator));
            String filename1 = MyIndex.packName(path1.getName(),"header/");
            String filename2 = MyIndex.packName(path2.getName(),"header/");
            config.set(filename1,MyIndex.getHeaderStr2(myIndex.getHeader1()));
            config.set(filename2,MyIndex.getHeaderStr2(myIndex.getHeader2()));
            //config.set("header",myIndex.getHeaderStr());


            config.set("separator", separator);
            config.set("mapreduce.output.textoutputformat.separator","+");      //separator reducer

            filePaths.setLength(filePaths.length() - 1);
            //job = new Job(config, "ReduceSideJoin");
            job = Job.getInstance(config, "ReduceSideJoin");
            job.setJarByClass(MapJoin.class);

            job.setInputFormatClass(HeaderInputFormat.class);
            FileInputFormat.addInputPaths(job, filePaths.toString());
            FileOutputFormat.setOutputPath(job, new Path(String.valueOf(i+fileCount)));         //Output dir edw
            MultipleOutputs.addNamedOutput(job,"file2", TextOutputFormat.class,Text.class,Text.class);
            job.setMapperClass(MapJoin.SortByKeyMapper.class);
            job.setReducerClass(MapJoin.SortByKeyReducer.class);
            job.setNumReduceTasks(no_of_reducers);
            job.setPartitionerClass(TaggedJoiningPartitioner.class);
            job.setGroupingComparatorClass(TaggedJoiningGroupingComparator.class);
            job.setMapOutputKeyClass(TaggedKey.class);
            job.setMapOutputValueClass(FatValue.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.waitForCompletion(true);            //JOB -> sorting -> 2 sorted files

            //Delete Previous Outuput Folder
            if(!firstTime) {
                fs.delete(new Path(String.valueOf(i + fileCount - 1) + "m"), true);
            }else
                firstTime = false ;

            //Map Side Join
            config = new Configuration();
            config.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "+");          //specifying the character that separates the key and values
            StringBuilder builder1 = new StringBuilder();
            StringBuilder builder2 = new StringBuilder();
            for(int j =0 ;j < no_of_reducers ;j++){
                if(fs.exists(new Path(String.valueOf(i + fileCount) + "/file2-r-" + String.format("%05d", j)))) {
                    builder1.append(String.valueOf(i + fileCount) + "/part-r-" + String.format("%05d", j) + ",");
                    builder2.append(String.valueOf(i + fileCount) + "/file2-r-" + String.format("%05d", j) + ",");
                }
            }
            builder1.setLength(builder1.length() - 1);
            builder2.setLength(builder2.length() - 1);
            String joinExpression = CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class, builder1.toString(), builder2.toString());
            config.set("mapred.join.expr", joinExpression);
            config.set("separator", separator);
            config.set("header",myIndex.getHeaderStr());
            Job mapJoin = Job.getInstance(config);
            configureJob(mapJoin, "mapJoin", no_of_reducers,fs, String.valueOf(i+fileCount),String.valueOf(i+fileCount), String.valueOf(i+fileCount)+"m", CombineValuesMapper.class, Reducer.class);
            mapJoin.setInputFormatClass(CompositeInputFormat.class);
            mapJoin.waitForCompletion(true);

            //Delete Sorted Folder
            fs.delete(new Path(String.valueOf(i+fileCount)),true);

            filenames[nextFileIndex] = null ;
            nextFileIndex = InputOptimization.Inner(filenames, String.valueOf(i+fileCount)+"m",fs,separator);
            nextFile = filenames[nextFileIndex];

            //Clear Variables
            config = new Configuration();
            filePaths = new StringBuilder();
            job = null;
        }
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        long tEnd = System.currentTimeMillis();
        long tDelta = tEnd - tStart;
        double elapsedSeconds = tDelta / 1000.0;
        System.out.println("elapsed time in seconds : "+elapsedSeconds);
    }

    private static void configureJob(Job job,
                                     String jobName,
                                     int numReducers,
                                     FileSystem fs,
                                     String inPath1,
                                     String inPath2,
                                     String outPath,
                                     Class<? extends Mapper> mapper,
                                     Class<? extends Reducer> reducer) throws java.io.IOException {
        job.setNumReduceTasks(0);
        job.setJobName(jobName);
        job.setMapperClass(mapper);
        job.setReducerClass(reducer);
        job.setMapOutputKeyClass(Text.class);
        job.setJarByClass(Main.class);
        //+"/part-r-00000"+"/file2-r-00000"

        String totalPath = null;
        for(int i =0 ;i < numReducers ;i++){
            Path path1 = new Path(inPath1+"/part-r-"+String.format("%05d",i));
            Path path2 = new Path(inPath2+"/file2-r-"+String.format("%05d",i));
            if(fs.exists(path2)){
                totalPath = path1.toString()+","+ path2.toString();
                FileInputFormat.addInputPath(job, new Path(totalPath));
            }
        }
        FileOutputFormat.setOutputPath(job, new Path(outPath));
    }
}