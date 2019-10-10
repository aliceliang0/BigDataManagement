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

public class Query5 {
    static class MyMapper extends Mapper<Object, Text, Text, Text> {

        private HashMap<String, String> idandage = new HashMap<String, String>();


        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader br = null;
            Path[] distributePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            String customer = null;

            for (Path p : distributePaths) {
                if (p.toString().contains("customer")) {
                    br = new BufferedReader(new FileReader(p.toString()));
                    while (null != (customer = br.readLine())) {
                        String[] customer_info = customer.split(",");
                        idandage.put(customer_info[0], customer_info[2] +  "," + customer_info[3]); //id age gender
                    }
                }
            }
        }

        Text outPutKey = new Text();
        Text outPutValue = new Text();
        String age = "";
        String gender = "";
        String ageTrans = "";

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] l = value.toString().split(",");
            String[] s = idandage.get(l[1]).split(",");
            age = s[0];
            gender = s[1];
            if(Integer.parseInt(s[0]) >= 10 && Integer.parseInt(s[0]) < 20) ageTrans = "[10,20)";
            if(Integer.parseInt(s[0]) >= 20 && Integer.parseInt(s[0]) < 30) ageTrans = "[20,30)";
            if(Integer.parseInt(s[0]) >= 30 && Integer.parseInt(s[0]) < 40) ageTrans = "[30,40)";
            if(Integer.parseInt(s[0]) >= 40 && Integer.parseInt(s[0]) < 50) ageTrans = "[40,50)";
            if(Integer.parseInt(s[0]) >= 50 && Integer.parseInt(s[0]) < 60) ageTrans = "[50,60)";
            if(Integer.parseInt(s[0]) >= 60 && Integer.parseInt(s[0]) <= 70) ageTrans = "[60,70]";
            outPutKey.set(ageTrans + "," + gender); // e.g.,  1,male
            outPutValue.set(l[2]); //the output value is transTotal
            context.write(outPutKey, outPutValue); // Key: age range + "," + gender   Value: transTotal
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            float minTransTotal = 100000.0f;
            float maxTransTotal = 0.0f;
            float sumTrans = 0.0f;
            float avgTransTotal = 0.0f;
            int length = 0;

            for (Text v : values) {
                String l = v.toString();
                if(Float.parseFloat(l) < minTransTotal) minTransTotal = Float.parseFloat(l);
                if(Float.parseFloat(l) > maxTransTotal) maxTransTotal = Float.parseFloat(l);
                length ++;
                sumTrans += Float.parseFloat(l);
            }

            avgTransTotal = sumTrans / length;

            context.write(key, new Text(minTransTotal + "," + maxTransTotal + "," + avgTransTotal));

        }
    }

    public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "query5");
        job.setJarByClass(Query5.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        DistributedCache.addCacheFile(new URI("/Users/daojun/Desktop/mapreduce/input/customer.txt"), job.getConfiguration());
        //job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

}