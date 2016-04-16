package com.finaltry;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

	public long getInputGroups(Job job) {
		try {
			org.apache.hadoop.mapreduce.Counters counters = job.getCounters();
			Counter counter = counters.findCounter(
					"org.apache.hadoop.mapred.Task$Counter",
					"REDUCE_INPUT_GROUPS");
			System.out.println(counter.getValue());
			return counter.getValue();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return 0;
	}

	public void main(String input) throws Exception {
		
		String zero=input;
		String one="output"; 
	    String two=	"output1";	
	    String three="output2";
	    
		
	    Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ",");// its not used but
															// we can change key
															// value style

		Job job = new Job(conf, "wordcount");
		Job jobnew = new Job(conf, "Sentence Count");
		Job jobpalindrom = new Job(conf, "Palindrom check");

		FileSystem fs = FileSystem.get(conf);

		job.setOutputKeyClass(Text.class);
		jobnew.setOutputKeyClass(Text.class);
		jobpalindrom.setOutputKeyClass(Text.class);

		job.setOutputValueClass(IntWritable.class);
		jobnew.setOutputValueClass(IntWritable.class);
		jobpalindrom.setOutputValueClass(IntWritable.class);

		// // Set only 1 reduce task
		// job.setNumReduceTasks(1);//to get all files in single output file,
		// but then we cant use parrelalisum

		if (fs.exists(new Path(one)))
			fs.delete(new Path(one), true);

		if (fs.exists(new Path(two)))
			fs.delete(new Path(two), true);

		if (fs.exists(new Path(three)))
			fs.delete(new Path(three), true);

		job.setMapperClass(MapWordCount.class);
		jobnew.setMapperClass(MapReverse.class);
		jobpalindrom.setMapperClass(MapPalindrom.class);

		job.setReducerClass(Reduce.class);
		jobnew.setReducerClass(Reduce.class);
		jobpalindrom.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		jobnew.setInputFormatClass(TextInputFormat.class);
		jobnew.setOutputFormatClass(TextOutputFormat.class);

		jobpalindrom.setInputFormatClass(TextInputFormat.class);
		jobpalindrom.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(zero));
		FileOutputFormat.setOutputPath(job, new Path(one));

		FileInputFormat.addInputPath(jobnew, new Path(zero));
		FileOutputFormat.setOutputPath(jobnew, new Path(two));

		FileInputFormat.addInputPath(jobpalindrom, new Path(zero));
		FileOutputFormat.setOutputPath(jobpalindrom, new Path(three));

		job.setJarByClass(Main.class);
		job.waitForCompletion(true);
		long valWordCount = getInputGroups(job);
	
		jobnew.setJarByClass(Main.class);
		jobnew.waitForCompletion(true);
		long valSentence = getInputGroups(jobnew);

		jobpalindrom.setJarByClass(Main.class);
		jobpalindrom.waitForCompletion(true);

		long valplanindrom = getInputGroups(jobpalindrom);
		System.out.println("palindrone Count  Reduce input records"
				+ valplanindrom + new Path(one));

		readFile(new Path(one) + "/" + "part-r-00000", "There are "
				+ valWordCount + " unique words:  ", true);
		readFile(new Path(two) + "/" + "part-r-00000", "There are "
				+ valSentence + " sentences which are reversed: ", true);
		readFile(new Path(three) + "/" + "part-r-00000", "There are "
				+ valplanindrom + " planidroms: ", true);

	}

	void readFile(String path, String msg, boolean overwrite)throws IOException {
		File file = new File("final.txt");
		if (!file.exists()) {
			file.createNewFile();
		}
		FileWriter fileWritter = new FileWriter(file.getName(), overwrite);

		BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
		bufferWritter.write(msg + "[");
		int count = 0;

		File lf = new File(path);
		if (lf.isFile()) {
			System.out
					.println("Tes its a file ------------------------------------------>");
		} else {

			System.out
					.println("Errroroorroororororrrr------------------------------------------>");
		}

//		if (lf.isFile()) {
			try (BufferedReader br = new BufferedReader(new FileReader(path))) {
				String sCurrentLine;
				while ((sCurrentLine = br.readLine()) != null) {
					bufferWritter.write(sCurrentLine + ",");
					count++;
					if (count == 15) {
						count = 0;
						bufferWritter.newLine();
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			bufferWritter.write("]");
			bufferWritter.newLine();
			bufferWritter.newLine();
			bufferWritter.newLine();
			bufferWritter.close();
		}

//	}
}