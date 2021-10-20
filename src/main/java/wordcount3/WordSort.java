package wordcount3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

// do the word sort job
public class WordSort {
    // define myclass <name:file,y:word,x:value>
    public static class myclass implements WritableComparable<myclass> {
        public int x;
        public String y;
        public String name;

        public int getX() {
            return x;
        }

        public String getY() {
            return y;
        }
        public String getName() {
            return name;
        }

        public void readFields(DataInput in) throws IOException {
            x = in.readInt();
            y = in.readUTF();
            name= in.readUTF();

        }

        public void write(DataOutput out) throws IOException {
            out.writeInt(x);
            out.writeUTF(y);
            out.writeUTF(name);

        }

        //set the compare rule
        //sort the value as file-value-word and the 'all' file at last
        public int compareTo(myclass p) {
            //deal with name 'all'
            if(this.getName().compareTo("all")==0 && p.getName().compareTo("all")!=0)
                return 1;
            else if (this.getName().compareTo("all")!=0 && p.getName().compareTo("all")==0)
                return -1;
            //deal with filename
            else if (this.getName().compareTo(p.getName()) < 0) {
                return -1;
            } else if (this.getName().compareTo(p.getName()) > 0) {
                return 1;}
            else{
                //deal with value
                if (this.x > p.x) {
                    return -1;
                } else if (this.x < p.x) {
                    return 1;
                } else {
                    //deal with word
                    if (this.getY().compareTo(p.getY()) < 0) {
                        return -1;
                    } else if (this.getY().compareTo(p.getY()) > 0) {
                        return 1;
                    } else {
                        return 0;
                    }

                }}
        }

    }

    //do the sort map
    public static class SortMapper
            extends Mapper<Object, Text, myclass, IntWritable> {
        private IntWritable valueInfo = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String[] word = value.toString().split("\\s+");
            myclass keyInfo = new myclass();

            //get myclass value separately
            keyInfo.x=Integer.parseInt(word[word.length-1]);
            keyInfo.y=word[word.length-2].split("_")[0];
            keyInfo.name=word[word.length-2].split("_")[1];

            valueInfo.set(Integer.parseInt(word[word.length-1]));
            context.write(keyInfo, valueInfo);
        }
    }

    public static class SortReducer
            extends Reducer<myclass,IntWritable,Text,Text> {
        //set the max number 100
        private IntWritable valueInfo = new IntWritable();
        private Text keyInfo = new Text();

        //set the rank and file
        private int rank = 1;
        private String prv = new String("");
        private String now = new String("");

        public void reduce(myclass key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int num = Integer.valueOf(conf.get("max"));
            now = key.name;
            //if the first file
            if (prv.equals("")){
                prv = key.name;
                context.write(new Text(prv),new Text(""));
                context.write(new Text(String.valueOf(rank) + ":" + key.y+","+String.valueOf(key.x)),new Text(""));
                rank++;
            }
            //if the file is already read
            else if (prv.equals(now)){
                if(rank <= num){
                context.write(new Text(String.valueOf(rank) + ":" + key.y+","+String.valueOf(key.x)),new Text(""));
                rank++;}
            }
            //if the file comes for the first time
            else{
                rank=1;
                context.write(new Text("---------------"),new Text(""));
                context.write(new Text(now),new Text(""));
                context.write(new Text(String.valueOf(rank) + ":" + key.y+","+String.valueOf(key.x)),new Text(""));
                prv = now;
                rank++;
            }

        }}
}
