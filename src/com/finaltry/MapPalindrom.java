package com.finaltry;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapPalindrom extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while(itr.hasMoreTokens()) {
            String token = itr.nextToken();
            boolean isPalindrome = true;
            for(int i = 0; i < token.length(); i++)
                if(token.charAt(i) != token.charAt(token.length()-i-1))
                {
                    isPalindrome = false;
                    break;
                }

	        if(isPalindrome)
	        {
		        word.set(token);
	            context.write(word, one);
            }
        }
        }
}