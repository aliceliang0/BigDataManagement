import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query1 {

    public static class SelectCustomerMapper
            extends Mapper<Object, Text, Text, NullWritable>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] strs = line.split(",");
            int age = Integer.parseInt(strs[2]);
            if(age >= 20 && age <= 50) {
                Text name = new Text(strs[1]);
                context.write(name, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: query1 <HDFS input file> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "query1");
        job.setJarByClass(Query1.class);
        job.setMapperClass(SelectCustomerMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(0);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

