package encourageCountryRelations;

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

public class EncourageCountryRealtions {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) {

			try {
				String[] str = value.toString().split(",");

				if (str[4].contains("Nonfiler")) {
					context.write(new Text(str[4]), new Text(str[8] + ","
							+ str[7]));

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

			int natives = 0;
			int philip = 0;
			float peru = 0;
			int india = 0;
			float poland = 0;
			int pr = 0;
			float vietnam = 0;
			int germany = 0;
			int dominican = 0;
			int ng = 0;
			int mexico = 0;
			float total = 0;
			int forignborn = 0;

			for (Text t : values) {
				total++;
				String parts[] = t.toString().split(",");

				if (!parts[0].contains("Native- Born in the United States")) {
					forignborn++;
					if ((parts[1].contains("Philippines"))) {
						philip++;
					} else if (parts[1].contains("Germany")) {
						germany++;
					} else if (parts[1].contains("Vietnam")) {
						vietnam++;
					} else if (parts[1].contains("peru")) {
						peru++;
					} else if (parts[1].contains("India")) {
						india++;
					} else if (parts[1].contains(" Mexico")) {
						mexico++;
					} else if (parts[1].contains("Dominican-Republic")) {
						dominican++;
					} else if (parts[1].contains("Poland")) {
						poland++;
					} else if (parts[1].contains(("Nicaragua"))) {
						ng++;
					} else if (parts[1].contains("Puerto-Rico")) {
						pr++;
					}
				} else {
					natives++;
				}
			}

			String countingnatives = "\n"
					+ "People who are Native born and Donot File Tax :-  "
					+ natives
					+ "\n"
					+ "percentage of People Who donot file Taxes and natioves :-  "
					+ (natives * 100) / total + "%";
			String countingforengrs = "\n"
					+ "Individual who are forign born and Donot Pay Taxes :-  "
					+ forignborn
					+ "\n"
					+ "Percentage of People Who are forigners and Doesnot File Taxes :-  "
					+ (forignborn * 100) / total + "%";

			// Country wise

			String countingDominian = "\n"
					+ "Individual who are Domnican and Donot Pay Taxes :-  "
					+ dominican
					+ "\n"
					+ "Percentage of People Who are Domnican and Doesnot File Taxes :-  "
					+ (dominican * 100) / total + "%";

			String countingpoland = "\n"
					+ "Individual who are poland and Donot Pay Taxes :-  "
					+ poland
					+ "\n"
					+ "Percentage of People Who are poland and Doesnot File Taxes :-  "
					+ (poland * 100) / total + "%";

			String countingnigaruga = "\n"
					+ "Individual who are Nigaruga and Donot Pay Taxes :-  "
					+ ng
					+ "\n"
					+ "Percentage of People Who are Nigaruga and Doesnot File Taxes :-  "
					+ (ng * 100) / total + "%";

			String countingpr = "\n"
					+ "Individual who are Puretorico and Donot Pay Taxes :-  "
					+ pr
					+ "\n"
					+ "Percentage of People Who are Peruto-Rico  and Doesnot File Taxes :-  "
					+ (pr * 100) / total + "%";

			String countingindians = "\n"
					+ "Individual who are indians and Donot Pay Taxes :-  "
					+ india
					+ "\n"
					+ "Percentage of People Who are indians and Doesnot File Taxes :-  "
					+ (india * 100) / total + "%";

			String countingMexico = "\n"
					+ "Individual who are mexico and Donot Pay Taxes :-  "
					+ mexico
					+ "\n"
					+ "Percentage of People Who are mexico and Doesnot File Taxes :-  "
					+ (mexico * 100) / total + "%";

			String countinggermans = "\n"
					+ "Individual who are germans and Donot Pay Taxes :-  "
					+ germany
					+ "\n"
					+ "Percentage of People Who are germans and Doesnot File Taxes :-  "
					+ (germany * 100) / total + "%";

			String countingvietnam = "\n"
					+ "Individual who are vieatnamese and Donot Pay Taxes :-  "
					+ vietnam
					+ "\n"
					+ "Percentage of People Who are vietnaman and Doesnot File Taxes :-  "
					+ (vietnam * 100) / total + "%";

			String countingphilipines = "\n"
					+ "Individual who are philipines and Donot Pay Taxes :-  "
					+ philip
					+ "\n"
					+ "Percentage of People Who are philipines and Doesnot File Taxes :-  "
					+ (philip * 100) / total + "%";

			String countingperus = "\n"
					+ "Individual who are peruians and Donot Pay Taxes :-  "
					+ peru
					+ "\n"
					+ "Percentage of People Who are peruians and Doesnot File Taxes :-  "
					+ (peru * 100) / total + "%";

			context.write(key, new Text(countingnatives));
			context.write(key, new Text(countingforengrs));
			context.write(key, new Text(countingpoland));
			context.write(key, new Text(countingnigaruga));
			context.write(key, new Text(countingpr));
			context.write(key, new Text(countingindians));
			context.write(key, new Text(countingMexico));
			context.write(key, new Text(countinggermans));
			context.write(key, new Text(countingvietnam));
			context.write(key, new Text(countingphilipines));
			context.write(key, new Text(countingperus));
			context.write(key, new Text(countingDominian));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(EncourageCountryRealtions.class);
		job.setJobName(" Which country's people are more responsible ");
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
