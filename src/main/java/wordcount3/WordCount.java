package wordcount3;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by Shaoluting on 2021/10/17.
 */

// do the main job
public class WordCount {

    public static void main(String[] args) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 4) {
            System.err.println("Usage: wordcount <in> <out> <stop> <punctuation>");
            System.exit(2);
        }
//        args[0]="input/source";
//        args[1]="output";
//        args[2]="input/stop-word-list.txt";
//        args[3]="input/punctuation.txt";
        if(otherArgs.length>4)
            conf.set("max",args[4]);
        else
            conf.set("max","100");
        //store stop and punctuation
        DistributedCache.addCacheFile(new URI(
                args[2]+"#stop"), conf);
        DistributedCache.addCacheFile(new URI(
                args[3]+"#punc"), conf);

        try {
            // job1: the wordcount2.0
            Job job = Job.getInstance(conf, "word count");
            job.setJarByClass(WordCount.class);
            job.setMapperClass(WordCombiner.WordCountMapper.class);
            job.setCombinerClass(WordCombiner.WordCountCombiner.class);
            job.setReducerClass(WordCombiner.WordCountReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            //the temp path of wordcount result
            Path tempath = new Path(args[1]+"/tmp");
            tempath.getFileSystem(conf).deleteOnExit(tempath);
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, tempath);

            //commit job
            boolean b = job.waitForCompletion(true);
            if (!b) {
                System.out.println("wordcount task fail!");
            }


        } catch (Exception e) {
            e.printStackTrace();
        }

        try{
            // job1: the wordcount3.0
            // do the sort work
            Job job2 = Job.getInstance(conf, "sort");
            job2.setJarByClass(WordCount.class);
            job2.setMapperClass(WordSort.SortMapper.class);
            job2.setReducerClass(WordSort.SortReducer.class);
            job2.setMapOutputKeyClass(WordSort.myclass.class);
            job2.setMapOutputValueClass(IntWritable.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(args[1]+"/tmp"));

            // to resolve the output existed, set the output path be args[1]+'/result'
            FileOutputFormat.setOutputPath(job2,
                    new Path(args[1]+"/result"));
            boolean b = job2.waitForCompletion(true);

           //delete the temp path
            Path tempath = new Path(args[1]+"/tmp");
            tempath.getFileSystem(conf).deleteOnExit(tempath);
            if(!b){
                System.out.println("sort task fail!");
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}



