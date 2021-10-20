package wordcount3;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

//do the wordcount2.0 job
public class WordCombiner {
    public static class WordCountMapper
            extends Mapper<Object, Text,Text, IntWritable> {
        private  final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text word_all = new Text();
        private String filename;
        static String punctuation;
        static List<String> ls = new ArrayList<String>();

        //set up to store stopword and punctuation
        protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            //get the name of the file
            InputSplit inputSplit = context.getInputSplit();
            FileSplit split=(FileSplit)inputSplit;
            filename=split.getPath().getName();

            //get the stop-words and punctuation
            FileReader fr = new FileReader("stop");
            BufferedReader buf = new BufferedReader(fr);
            String stopword;
            while((stopword=buf.readLine())!=null){
                ls.add(stopword);
            }
            buf.close();

            FileReader frr = new FileReader("punc");
            StringBuilder buffer = new StringBuilder();
            BufferedReader bf= new BufferedReader(frr);
            String s =null;
            while((s = bf.readLine())!=null){//使用readLine方法，一次读一行
                buffer.append(s.trim());
            }
            punctuation = buffer.toString();
            bf.close();

        }

        // do the basic map
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //remove the punctuation
            StringTokenizer itr = new StringTokenizer(value.toString(),punctuation+" \t\n\r");
            while (itr.hasMoreTokens()) {
                String tmpword = itr.nextToken();
                //remove the number and check the length and remove the stop-word
                if((!ls.contains(tmpword.toLowerCase()))&&(!StringUtils.isNumeric(tmpword))&&(tmpword.length()>=3)){
                    //write with the type <word_file,1> and <word_all,1>
                    word.set(tmpword.toLowerCase()+"_"+filename);
                    word_all.set(tmpword.toLowerCase()+"_all");
                    context.write(word, one);
                    context.write(word_all, one);
                }
            }
        }
    }

   // do the basic reduce job
    public static class WordCountReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    //the combinet
    // do the basic reduce job
    public static class WordCountCombiner
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
