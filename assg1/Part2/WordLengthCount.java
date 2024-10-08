import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordLengthCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{
	    HashMap<Integer, Integer> hMap = new HashMap<Integer, Integer>();

            private final static IntWritable one = new IntWritable(1);
            private Text word = new Text();

            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                StringTokenizer itr = new StringTokenizer(value.toString());
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken());
		    // get the length of the word
		    int len = word.getLength();
		    if(hMap.containsKey(len)) {
			int sum = (int) hMap.get(len) + 1;
			hMap.put(len, sum);
	   	    } else {
			hMap.put(len, 1);
		    }
		    // convert int to string
		    // String mapStr = String.valueOf(len);
                    // context.write(new Text(mapStr), one);
                }
           }
	   public void cleanup(Context context) throws IOException, InterruptedException {
		Iterator<Map.Entry<Integer, Integer>> tmp = hMap.entrySet().iterator();

		while(tmp.hasNext()) {
		    Map.Entry<Integer, Integer> entry = tmp.next();
		    String key = String.valueOf(entry.getKey());
		    Integer count = entry.getValue();

		    context.write(new Text(key), new IntWritable(count));
		}
	   }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text, IntWritable> {
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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word length count");
        job.setJarByClass(WordLengthCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
	// Formatting the output by changing the separator
	job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}