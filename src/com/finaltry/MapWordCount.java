package com.finaltry;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.tools.ant.taskdefs.Available;

public class MapWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	int count = 0;
        double sum = 0;
        double average = 0;
        
    	StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            	String token = itr.nextToken().toLowerCase().replaceAll("[^A-Za-z ]","");;
                word.set(token);
                String userInput =token;
                double charNum = userInput.length();
                sum = charNum + sum;
                count++;
                
                context.write(word, one);
        }
        if (count > 0) {
            average = sum / count;
        }
        Main lm=new Main();
        lm.readFile(new Path("output2")+"/part-r-00000", "The average number of letters per word is:"+average,false);
    }
}