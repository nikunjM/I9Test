package com.finaltry;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapReverse extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), ".");
        
        while (itr.hasMoreTokens()) {
        	word.set(new StringBuilder(itr.nextToken()).reverse().toString());
        	//word.set(reverse(itr.nextToken()));
        	context.write(word, one);
        }
    }
    String reverse(String str)
    {
        char[] charArray = str.toCharArray();
        for(int i=0,j=charArray.length-1; i<j; i++,j--) {
            char temp=charArray[i];
            charArray[i]= charArray[j];
            charArray[j]= temp;
        }
        return String.valueOf(charArray);
    }
    
}