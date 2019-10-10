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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query4 {
    static class MyMapper extends Mapper<Object, Text, Text, Text> {

        HashMap<String, String> idandcode = new HashMap<String,String>();

        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader br = null;
            Path[] distributePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            String customer = null;

            for (Path p : distributePaths) {
                if (p.toString().contains("customer")) {
                    br = new BufferedReader(new FileReader(p.toString()));
                    while (null != (customer = br.readLine())) {
                        String[] customer_info = customer.split(",");
                        idandcode.put(customer_info[0], customer_info[4]); // key is id, value is CountryCode
                    }
                }
            }
        }

        Text outPutKey = new Text();
        Text outPutValue = new Text();
        String countryCode = "";

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] l = value.toString().split(",");
            countryCode = idandcode.get(l[1]);
            outPutKey.set(countryCode);
            outPutValue.set(l[2]); //the output is name transtotal
            context.write(outPutKey, outPutValue); // id name transtotal
        }
    }

    public static class SecondMapper
            extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] l = value.toString().split(",");
            String ctycode = l[4];
            context.write(new Text(ctycode), new Text("customer"));
        }
    }


    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            float minTransTotal = 100000.0f;
            float maxTransTotal = 0.0f;
            int sum = 0; // calculating the NumberOfCustomers

            for (Text v : values) {
                String l = v.toString();
                if (l.equals("customer")){
                    sum ++;
                }
                else {
                    if (Float.parseFloat(l) < minTransTotal)   minTransTotal = Float.parseFloat(l);
                    if (Float.parseFloat(l) > maxTransTotal)   maxTransTotal = Float.parseFloat(l);
                }
            }

            context.write(key, new Text(sum + "," + minTransTotal + "," + maxTransTotal));

        }
    }

    public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "query4");
        job.setJarByClass(Query2.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        DistributedCache.addCacheFile(new URI("/Users/daojun/Desktop/mapreduce/input/customer.txt"), job.getConfiguration());
        MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, MyMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, SecondMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}