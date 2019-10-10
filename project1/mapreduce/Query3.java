import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;


public class Query3 {

    public static class customerMapper
            extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value.toString();
            String[] l = line.split(",");
            context.write(new Text(l[0]), new Text("customer" + "," + l[1] + "," + l[5]));
            //ID  name salary
        }
    }


    public static class transactionMapper
            extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value.toString();
            String[] l = line.split(",");
            context.write(new Text(l[1]), new Text("transaction"+ "," + l[2] + "," +l[3]));
            // ID  transTotal  transNumItems
        }
    }

    public static class custransReducer
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> value, Context context
        ) throws IOException, InterruptedException {

            String name = ""; // record the name
            float salary = 0.0f; // record the salary
            int sum = 0; // calculating the sum of transactions
            float totalSum = 0.0f; // record the TotalSum
            int minItems = 100; // record the minItems

            for (Text v:value) {
                String[] l = v.toString().split(",");

                if (l[0].equals("customer")){
                    name = l[1];
                    salary = Float.parseFloat(l[2]);
                }
                else if (l[0].equals("transaction")) {
                    sum = sum + 1;
                    totalSum += Float.parseFloat(l[1]);
                    if(Integer.parseInt(l[2]) < minItems){
                        minItems = Integer.parseInt(l[2]);
                    }
                }

            }

            context.write(key, new Text(name + "," + salary + "," + sum + "," + totalSum + ","  + minItems));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "query3");
        job.setJarByClass(Query3.class);
        job.setReducerClass(Query3.custransReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, customerMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, transactionMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
