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


public class NgramInitialCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{
	    HashMap<String, Integer> hMap = new HashMap<String, Integer>();

            private final static IntWritable one = new IntWritable(1);
            private Text word = new Text();
			
			private String associativeArray= new String("");
			
            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                StringTokenizer itr = new StringTokenizer(value.toString());
				//String associativeArray= new String("");
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken());
					int isFound=0; //set isFound=0 for every token
					for(int a=0;a<word.getLength();a++){
						if(!Character.isLetter((char)word.charAt(a))){
							isFound=0; //if is not character, reset isFound=0 to get next character.
							continue;
						}
						else{
							if(isFound==0){ //is letter and is first character
								associativeArray=associativeArray.concat(Character.toString((char)(word.charAt(a)))); //put everything
																	//(every 1st character) in a string.
								isFound=1; //not head anymore, so isFound=1
							}
							else{ //is letter but not first character
								continue;
							}
						}
					}
					//associativeArray=associativeArray.concat(Character.toString((char)(word.charAt(0))));		//put everything in a string.			
				
                }
                
            }
            public void cleanup(Context context) throws IOException, InterruptedException {
				Configuration conf = context.getConfiguration();
                int N = Integer.parseInt(conf.get("N"));
				for(int i=0;i<associativeArray.length();i++){
					String keyString=new String("");
					for(int x=0;x<N;x++){ //N-gram concat
						if((i+x)<associativeArray.length()){ //prevent out of range
							keyString=keyString.concat(Character.toString(associativeArray.charAt(i+x)));
						}
						else{
							//int temp=i+x-associativeArray.length(); 
							//keyString=keyString.concat(Character.toString(associativeArray.charAt(temp)));
							keyString=new String("");
							continue;
						}
					}
					
					if(hMap.containsKey(keyString)){
						int sum=hMap.get(keyString)+1;
						hMap.put(keyString,sum);
					}
					else {
						hMap.put(keyString,1);
					}
				}
				Iterator<Map.Entry<String, Integer>> tmp = hMap.entrySet().iterator();
				
                while (tmp.hasNext()) {
                    Map.Entry<String, Integer> entry = tmp.next();
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
// public static int N;
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
		if(args[2]!=null){
			// N=Integer.parseInt(args[2]);
            conf.set("N", args[2]);
		}
        Job job = Job.getInstance(conf, "word length count");
        job.setJarByClass(NgramInitialCount.class);
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