//OBJECTIVES

//MAPPING
//AGE Group who files taxes ,Education ,Martial status,residence
//5 columns
//REDUCING
//No of individuals who pay tax and are from US/Foreign Origin

package showvalues;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageSalaryAgeGroups {


		public static class MapClass extends Mapper <LongWritable,Text,Text,Text>{
				
				public void map(LongWritable Key,Text value,Context context)throws IOException, InterruptedException
				{
				try{	String record=value.toString();
					String[] str=record.split(",");
					//0 to 12
					//13 to 17
					//18 to 40
					//41 to 60
					//61 to 80
					//81 to 100
					String[] sp=str[5].split(":");
					String[] sp1=str[0].split(":");
					 String[] p1=sp1[1].split(" ");
					 
					
					
							context.write(new Text("null"), new Text(sp[1]+","+p1[1]));
				}
					
					
							catch (Exception e) {
								System.out.println(e.getMessage());
			     }
				}
				}
		

		public static class ReduceClass extends Reducer<Text, Text, NullWritable, Text>	
		{
			public void reduce (Text key,Iterable<Text> values,Context context)throws IOException, InterruptedException 
			{
				long age;
				double sal1=0,sal2=0,sal3=0,sal4=0,sal5=0,sal6=0;
				int inf=0,teen=0,adult=0,middle=0,senior=0,old=0;
				for (Text val : values) 
				 {
					String parts=val.toString();
					String[] p=parts.split(",");
					age=Long.parseLong(p[1]);
					
					
				 
					if((age>=0)&&(age<=12))
					{
						inf++;
						sal1=sal1+Double.parseDouble(p[0]);
					}
					else if((age>=13)&&(age<=17))
					{
						teen++;
						sal2=sal2+Double.parseDouble(p[0]);
					}
					else if((age>=18)&&(age<=40))
					{
						adult++;
						sal3=sal3+Double.parseDouble(p[0]);
					}
					else if((age>=41)&&(age<=60))
					{
						middle++;
						sal4=sal4+Double.parseDouble(p[0]);
					}
					else if((age>=61)&&(age<=80))
					{
						senior++;
						sal5=sal5+Double.parseDouble(p[0]);
					}
					else if((age>=81)&&(age<=100))
					{
						old++;
						sal6=sal6+Double.parseDouble(p[0]);
					}
							
				 } 
				double d=sal2/teen;
				String s1="Average salary of Infants (0 to 12)     :   "+sal1/inf;
				String s2="Average salary of Teenegers (13 to 17)  :   "+d;
				String s3="Average salary of Adults (18 to 40)     :   "+sal3/adult;
				String s4="Average salary of Middle Aged (41 to 60):   "+sal4/middle;
				String s5="Average salary of Seniors (61 to 80)    :   "+sal5/senior;
				String s6="Average salary of Elderly (81 to 100)   :   "+sal6/old;
				
				context.write(null, new Text(s1));
				context.write(null, new Text(s2));
				context.write(null, new Text(s3));
				context.write(null, new Text(s4));
				context.write(null, new Text(s5));
				context.write(null, new Text(s6));
			}
		}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, " census");
		job.setJarByClass(AverageSalaryAgeGroups.class);
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
