import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query2 {
    static class MyMapper extends Mapper<Object, Text, Text, Text> {

        private HashMap<String, String> idandName = new HashMap<String, String>();


        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader br = null;
            Path[] distributePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            String customer = null;

            for (Path p : distributePaths) {
                if (p.toString().contains("customer")) {
                    br = new BufferedReader(new FileReader(p.toString()));
                    while (null != (customer = br.readLine())) {
                        String[] customer_info = customer.split(",");
                        idandName.put(customer_info[0], customer_info[1]);
                    }
                }
            }
        }

        Text outPutKey = new Text();
        Text outPutValue = new Text();
        String name = "";

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] l = value.toString().split(",");
            name = idandName.get(l[1]);
            outPutKey.set(l[1]); //id
            outPutValue.set(name + "," + "1"+ "," + l[2]); //the output is name transtotal
            context.write(outPutKey, outPutValue); // id name 1 transtotal
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            float totalSum = 0.0f;
            String name = "";
            int sum = 0;

            for (Text v : values) {
                String[] l = v.toString().split(",");
                name = l[0];
                totalSum += Float.parseFloat(l[2]);
                sum += Integer.parseInt(l[1]);
            }

            context.write(key, new Text(name + "," + sum + "," + totalSum));
        }
    }
    public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "query2");
            job.setJarByClass(Query2.class);
            job.setMapperClass(MyMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            DistributedCache.addCacheFile(new URI("/Users/daojun/Desktop/mapreduce/input/customer.txt"), job.getConfiguration());
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
    }

}


