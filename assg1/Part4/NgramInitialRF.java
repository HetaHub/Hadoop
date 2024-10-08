import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NgramInitialRF {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        HashMap<String, Integer> hMap = new HashMap<String, Integer>();
        HashMap<String, Integer> coMap = new HashMap<String, Integer>(); // map for co-ocurrence
        HashMap<String, Double> resultMap = new HashMap<String, Double>();

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private String associativeArray = new String("");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            // String associativeArray= new String("");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                int isFound = 0; // set isFound=0 for every token
                for (int a = 0; a < word.getLength(); a++) {
                    if (!Character.isLetter((char) word.charAt(a))) {
                        isFound = 0; // if is not character, reset isFound=0 to get next character.
                        continue;
                    } else {
                        if (isFound == 0) { // is letter and is first character
                            associativeArray = associativeArray.concat(Character.toString((char) (word.charAt(a)))); // put
                                                                                                                     // everything
                            // (every 1st character) in a string.
                            isFound = 1; // not head anymore, so isFound=1
                        } else { // is letter but not first character
                            continue;
                        }
                    }
                }
                // associativeArray=associativeArray.concat(Character.toString((char)(word.charAt(0))));
                // //put everything in a string.

            }
            // System.out.println("AssoArray: " + associativeArray);

        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int N = Integer.parseInt(conf.get("N"));

            for (int i = 0; i < associativeArray.length() - 1; i++) { // reduced size of loop (dan)
                String keyString = new String("");
                String key = new String(""); // save the first word of n-gram only
                // if (i + N < associativeArray.length() - 1) {
                key = key.concat(Character.toString(associativeArray.charAt(i)));
                // }
                for (int x = 0; x < N; x++) { // N-gram concat
                    if ((i + x) < associativeArray.length()) { // prevent out of range
                        if (N - x - 1 != 0) {
                            keyString = keyString.concat(Character.toString(associativeArray.charAt(i + x)) + " ");
                        } else {
                            keyString = keyString.concat(Character.toString(associativeArray.charAt(i + x)));
                        }
                    } else {
                        // int temp=i+x-associativeArray.length();
                        // keyString=keyString.concat(Character.toString(associativeArray.charAt(temp)));
                        keyString = new String("");
                        break;
                    }
                }

                if (coMap.containsKey(key)) {
                    int sum = coMap.get(key) + 1;
                    coMap.put(key, sum);
                } else {
                    coMap.put(key, 1);
                    // System.out.println("comap get value: "+coMap.get(key));
                }

                if (hMap.containsKey(keyString)) {
                    int sum = hMap.get(keyString) + 1;
                    hMap.put(keyString, sum);
                } else {
                    hMap.put(keyString, 1);
                }
                // System.out.println("keyString: "+keyString);
                // System.out.println("key: "+key);

            }

            // calculate the frequency
            for (String x : hMap.keySet()) {
                Double xyz = Double.valueOf(hMap.get(x)); // the partial N-gram
                // System.out.println("x: "+x);
                // System.out.println("x.charAt(0): "+x.charAt(0));
                // System.out.println("coMap q: "+coMap.get("q"));
                Double divider = Double.valueOf(coMap.get(Character.toString(x.charAt(0))));
                Double result = xyz / divider;
                resultMap.put(x, result);
            }
            Iterator<Map.Entry<String, Double>> tmp = resultMap.entrySet().iterator();

            while (tmp.hasNext()) {
                Map.Entry<String, Double> entry = tmp.next();
                String key = String.valueOf(entry.getKey());
                Double frequency = entry.getValue();
                System.out.println(key + frequency);
                context.write(new Text(key), new DoubleWritable(frequency));
            }

        }
    }

    public static class IntSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Double sum = Double.valueOf(0);
            Double Theta = Double.parseDouble(conf.get("Theta"));

            for (DoubleWritable val : values) {
                sum += val.get();
            }
            // only frequency >= theta can be shown in output
            if (sum >= Theta) {
                result.set(sum);
                context.write(key, result);
            }
        }
    }

    // public static int N;
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args[2] != null) {
            // N=Integer.parseInt(args[2]);
            conf.set("N", args[2]);
        }
        if (args[3] != null) {
            conf.set("Theta", args[3]);
        }
        Job job = Job.getInstance(conf, "word length count");
        job.setJarByClass(NgramInitialRF.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class); // changed the output type (dan)
        // Formatting the output by changing the separator
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}