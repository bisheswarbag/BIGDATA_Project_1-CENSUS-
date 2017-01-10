//have income and donot pay taxes vs other daispora
//Income >1500

package incomeNoTax;

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

public class IncomeNoTaxes {

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) {

			try {
				String[] str = value.toString().split(",");
				if (str[4].contains("Nonfiler"))
					context.write(new Text(str[4]), new Text(str[8]));
			}

			catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}

	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int nonamerican = 0;
			int american = 0;
			int total = 0;
			for (Text t : values) {
				total++;
				String parts[] = t.toString().split(",");

				if (!(parts[0].contains("Native- Born in the United States"))) {
					nonamerican++;

				} else {
					american++;
				}
			}

			String countingDivorce = "\n"
					+ "People who have Income and donot File Tax :-  "
					+ american + "\n"
					+ "percentage of People Who donot file Taxes :-  "
					+ (american * 100) / total;
			String countingDivorce2 = "\n"
					+ "People who donot belong to US and donot Pay Taxes :-  "
					+ nonamerican + "\n"
					+ "Percentage of People Who Doesnot File Taxes :-  "
					+ (nonamerican * 100) / total;

			context.write(key, new Text(countingDivorce));
			context.write(key, new Text(countingDivorce2));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(IncomeNoTaxes.class);
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
