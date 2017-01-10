//OBJECTIVES

//MAPPING
//AGE Group who files taxes ,Education ,Martial status,residence
//5 columns
//REDUCING
//No of individuals who pay tax and are from US/Foreign Origin

package showvalues;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Showvalues {

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) {

			try {
				String[] str = value.toString().split(",");
				String extendsoutput = str[1] + str[4] + str[8];
				if (str[2].contains("Divorced")
						&& (str[8]
								.contains("Native- Born in the United States"))
						&& (str[4].contains("Nonfiler"))) {
					context.write(new Text(str[2]), new Text(extendsoutput));
				}
			}

			catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, " census");
		job.setJarByClass(Showvalues.class);
		job.setMapperClass(MapClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
