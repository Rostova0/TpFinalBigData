package org.epf.hadoop.colfil1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ColFilJob1 {
    public static void main(String[] args) throws Exception {
        System.out.println("Arguments provided:");
        for (int i = 0; i < args.length; i++) {
            System.out.println("Arg[" + i + "]: " + args[i]);
        }

        if (args.length != 2) {
            System.err.println("Usage: ColFilJob1 <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Collaborative Filtering Job 1");

        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(args[1]);
        if (fs.exists(outputPath)) {
            System.out.println("Output path exists. Deleting: " + outputPath);
            fs.delete(outputPath, true);
        }

        job.setJarByClass(ColFilJob1.class);


        job.setMapperClass(Job1Mapper.class);
        job.setReducerClass(Job1Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(RelationshipInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setNumReduceTasks(2);

        System.out.println("Job name: " + job.getJobName());
        System.out.println("Number of reducers: " + job.getNumReduceTasks());

        boolean success = job.waitForCompletion(true);

        System.out.println("Job execution completed. Success: " + success);
        System.exit(success ? 0 : 1);
    }
}
