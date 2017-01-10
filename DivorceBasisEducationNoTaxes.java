//Grape of Award to Responsible Faction of the Society/Country.
//Government S
//Teacher /Job in Tax or Government Agency/.
//effect of correlation between education divorce
package perDivoreEducation;

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

public class DivorceBasisEducationNoTaxes {

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) {

			try {
				String[] str = value.toString().split(",");
				if (str[2].contains("Divorced")
						&& (str[4].contains("Nonfiler"))) {

					context.write(new Text(str[2]), new Text(str[1] + ","
							+ str[8] + "," + str[4]));
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
			int educated = 0;
			int uneducated = 0;
			int countphd = 0;
			int countEducatedHighschool = 0;
			int countbachelors = 0;
			int count10thgrade = 0;
			int countless1st = 0;
			int countcollgepass = 0;
			int total = 0;
			for (Text t : values) {
				total++;
				String parts[] = t.toString().split(",");

				if ((parts[0].contains("Bachelors degree(BA AB BS)") && (parts[1]
						.contains("Native- Born in the United States")))) {
					countEducatedHighschool++;

				}
				if ((parts[0].contains("High school graduate") && (parts[1]
						.contains("Native- Born in the United States")))) {
					countbachelors++;

				}
				if ((parts[0].contains("10th grade") && (parts[1]
						.contains("Native- Born in the United States")))) {

					count10thgrade++;

				}
				if ((parts[0].contains("Less than 1st grade") && (parts[1]
						.contains("Native- Born in the United States")))) {
					countless1st++;
				}
				if ((parts[0].contains(" Doctorate degree(PhD EdD)") && (parts[1]
						.contains("Native- Born in the United States")))) {
					countphd++;
				}

				if ((parts[0].contains(" Some college but no degree") && (parts[1]
						.contains("Native- Born in the United States")))) {
					countcollgepass++;
				}

			}
			educated = count10thgrade + countbachelors
					+ countEducatedHighschool + countless1st + countcollgepass;
			uneducated = total - educated;

			String countingDivorce = "\n"
					+ "Individual who are divorced and donot pay taxes and are Native and are high school graduates"
					+ "\n" + countEducatedHighschool + "\n"
					+ (countEducatedHighschool * 100) / total + "%";
			String countingDivorce2 = "\n"
					+ "Individual who are divorced and donot pay taxes and are Native and are bachlors "
					+ countbachelors + "     percentage "
					+ (countbachelors * 100) / total + "%";
			String countingDivorce3 = "\n"
					+ "Individual who are divorced and donot pay taxes and are Native and are 10 pass "
					+ count10thgrade + "\n" + total;
			String countingDivorce4 = "\n"
					+ "Individual who are divorced and donot pay taxes and are Native and are 10 pass "
					+ countless1st + "\n" + total;
			String countingDivorce5 = "\n"
					+ "Individual who are divorced and donot pay taxes and are Native and are Doctorate degree(PhD EdD) "
					+ countphd + "\n" + total;
			String countingDivorce6 = "\n"
					+ "Individual who are divorced and donot pay taxes and are Native and have done some collage "
					+ countcollgepass + "\n" + total + "\n" + "\n" + "\t"
					+ "Uneducated Divorce NonTax Payer  " + uneducated + "\t"
					+ "Percentage :-" + (uneducated * 100) / total + "%" + "\n"
					+ "\n" + "\t" + "Educated Divorce NonTax Payer  "
					+ educated + "\t" + "Percentage :- " + (educated * 100)
					/ total + "%";

			context.write(key, new Text(countingDivorce));
			context.write(key, new Text(countingDivorce2));
			context.write(key, new Text(countingDivorce3));
			context.write(key, new Text(countingDivorce4));
			context.write(key, new Text(countingDivorce5));
			context.write(key, new Text(countingDivorce6));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(DivorceBasisEducationNoTaxes.class);
		job.setJobName("Reduce Side Join");
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
