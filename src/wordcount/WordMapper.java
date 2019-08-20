package wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	
	private final static LongWritable one = new LongWritable(1);
	private Text word = new Text();
	
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
		String sens = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(sens, "\t\r\n\f|,.()<>");
		while(tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken().toLowerCase());
			context.write(word, one);
		}
	}

}
