import java.io.IOException;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TrendAnalyzer extends Configured implements Tool
{
	
	public static class TrendMapper extends Mapper<Object, Text, Text, Text>
	{
	    private Text dateKey = new Text();
	    private Text songsValue = new Text();
	
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	    {
	    	String line = value.toString();
	    	
	    	for (String entry : line.split("\\t"))
	    	{
	    	   String[] token = entry.split(",");
	    	   
	    	   if(token != null && token.length >= 5 )
	    	   {	
	    		   dateKey.set(token[4]);
	    		   songsValue.set(token[0]);
	    		   
	    		   if(token[4] != null && token[4].length() >= 10 )
		    	   {
		    		   if(Integer.parseInt(token[4].substring(8)) >= 25 && Integer.parseInt(token[4].substring(8)) <= 31)
		    		   {	   
			              context.write(dateKey, songsValue);
		    		   }
		    	   }
	    	   }
	        }
	    }
    }
	
	public static class TrendCombiner extends Reducer<Text, Text, Text, Text>
	{
	    public void reduce(Text dateKey, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	    {
	    	for (Text value : values)
	    	{	   
			    context.write(dateKey, value);
	        }
	    }
    }

    public static class TrendReducer extends Reducer<Text,Text,Text,Text> 
    {
    	private Text result = new Text();
    	 
    	private MultipleOutputs<Text,Text> out;

    	public void setup(Context context) 
    	{
    	   out = new MultipleOutputs<Text,Text>(context);   
    	}
    	 
	    public void reduce(Text dateKey, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	    {
	    	 System.out.println("Reducer - dateKey : "+dateKey);
	    	
	    	 Map<String, Integer> keyMap = new TreeMap<>();
	    	 
	    	 for (Text v : values) 
	    	 { 
	    		 String song = v.toString();
	    		 
	    		 if (!keyMap.containsKey(song)) 
	    		 {
	                    keyMap.put(song, 0);
	             }
	    		 
	             keyMap.put(song, 1 + keyMap.get(song));  
	    	 }
	    	 
	    	 String outputString = "";
	    	 
	    	 int count = 0;
	    	 int maxLimit = 100;
	    	 
	    	 for (Entry<String, Integer> entry  : entriesSortedByValues(keyMap)) 
	    	 {	    		 
	    		 if (count == maxLimit) 
	    		 { 
	    			 break; 
	    		 }
	    		 
	    		 if(!outputString.equals(""))
	    		 {		 
	    			 outputString = outputString + "\n"+entry.getKey();
	    		 }
	    		 else
	    		 {
	    			 outputString = entry.getKey(); 
	    		 }
	    		 
	    		 count = count + 1;
	    	 }
	    	 
	    	 result.set(outputString);
		      
		     out.write(result, new Text(""), dateKey.toString().substring(8));
	    }
	    
	    public void cleanup(Context context) throws IOException,InterruptedException 
	    {
	        out.close();        
	    }
	    
	    private <K,V extends Comparable<? super V>> SortedSet<Map.Entry<K,V>> entriesSortedByValues(Map<K,V> map) 
	    {
	        SortedSet<Map.Entry<K,V>> sortedEntries = new TreeSet<Map.Entry<K,V>>(
	            new Comparator<Map.Entry<K,V>>() {
	                @Override public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2) {
	                    int res = e2.getValue().compareTo(e1.getValue());
	                    if (e1.getKey().equals(e2.getKey())) {
	                        return res; // Code will now handle equality properly
	                    } else {
	                        return res != 0 ? res : 1; // While still adding all entries
	                    }
	                }
	            }
	        );
	        
	        sortedEntries.addAll(map.entrySet());
	        return sortedEntries;
	    }
	    
  }

  public static void main(String[] args) throws Exception 
  {
	  System.out.println("Job Start Time : "+new Date());
	  
	  int returnStatus = ToolRunner.run(new Configuration(), new TrendAnalyzer(), args);

	  System.out.println("Job End Time : "+new Date());
	  
      System.exit(returnStatus); 
  }	 
  
  public int run(String[] args) throws IOException
  {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Trend_Analyzer");
	    job.setJarByClass(TrendAnalyzer.class);
	    
	    job.setMapperClass(TrendMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.getConfiguration().set("mapred.max.map.failures.percent","2");
	    
	    job.setCombinerClass(TrendCombiner.class);
	    
	    job.setReducerClass(TrendReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(1);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
	    
	    job.getConfiguration().set("fs.s3n.awsAccessKeyId", "AKIAJMYYKPC4HDHXUNRQ");
        job.getConfiguration().set("fs.s3n.awsSecretAccessKey","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");
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    try 
	    {
	    	boolean isSuccess = job.waitForCompletion(true);
	    	
	    	if (isSuccess)
	    	{
	    	    FileSystem hdfs = FileSystem.get(getConf());
	    	    FileStatus fs[] = hdfs.listStatus(new Path(args[1]));
	    	    
	    	    if (fs != null)
	    	    { 
	    	        for (FileStatus aFile : fs) 
	    	        {
	    	            if (!aFile.isDirectory()) 
	    	            {
	    	                hdfs.rename(aFile.getPath(), new Path(aFile.getPath().toString().replace("-r-00000", "")+".txt"));
	    	            }
	    	        }
	    	    }
	    	}
	    	
			return isSuccess ? 0 : 1;
		} 
	    catch (ClassNotFoundException e) 
	    {
			e.printStackTrace();
		}
	    catch (InterruptedException e) 
	    {
			e.printStackTrace();	
		}
	    catch (Exception e) 
	    {
			e.printStackTrace();	
		}
	    
		return 0;
  }
}