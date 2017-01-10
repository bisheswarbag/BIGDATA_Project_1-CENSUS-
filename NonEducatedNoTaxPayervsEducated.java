package nonEducatedTaxPayervsEducated;

//UnEducated are who lower than 10th grade

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NonEducatedNoTaxPayervsEducated {

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) {

			try {
				String[] str = value.toString().split(",");
				if (str[4].contains("Nonfiler")) {
					context.write(new Text(str[4]), new Text(str[1]));
				}
			}

			catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}

	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int Educated = 0;
			int Uneducated = 0;
			int total = 0;
			for (Text t : values) {
				total++;
				String parts[] = t.toString().split(",");

				if (!(parts[0].contains("7th and 8th grade") || (!parts[0]
						.contains("10th grade") || (!parts[0]
						.contains("5th or 6th grade") || (!parts[0]
						.contains("9th grade") || (!parts[0]
						.contains(" Some college but no degree"))))))) {
					Uneducated++;

				} else {
					Educated++;
				}
			}

			String countingDivorce = "\n"
					+ "People who are Uneducated and Donot File Tax :-  "
					+ Uneducated
					+ "\n"
					+ "percentage of People Who donot file Taxes and Uneducated :-  "
					+ (Uneducated * 100) / total;
			String countingDivorce2 = "\n"
					+ "Individual who are Educated and still dont Pay Taxes :-  "
					+ Educated
					+ "\n"
					+ "Percentage of People Who are Educated and Still Doesnot File Taxes :-  "
					+ (Educated * 100) / total;

			context.write(key, new Text(countingDivorce));
			context.write(key, new Text(countingDivorce2));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(NonEducatedNoTaxPayervsEducated.class);
		job.setJobName("NON EDUCATED TAXPAYER VS EDUCATED TAXPAYER ");
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
