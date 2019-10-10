import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class CustomInputFormat {
    public static class JSONInputFormat extends FileInputFormat<Text, Text> {

        public RecordReader<Text, Text> createRecordReader(InputSplit InputSplit, TaskAttemptContext TaskAttemptContext) throws
                IOException, InterruptedException {

            JSONRecordReader reader = new JSONRecordReader();
            reader.initialize(InputSplit, TaskAttemptContext);
            return reader;
        }
        public static void setMaxInputSplitSize(Job job, long size) {

            job.getConfiguration().setLong("mapred.max.split.size", size);
        }


        @Override
        public List<InputSplit> getSplits(JobContext job) throws IOException {

            List<InputSplit> splits = new ArrayList();
            List<FileStatus> files = this.listStatus(job);
            Iterator i$ = files.iterator();
            while(i$.hasNext()) {
                FileStatus file = (FileStatus) i$.next();
                Path path = file.getPath();
                FileSystem fs = path.getFileSystem(job.getConfiguration());
                LineReader lr = null;
                FSDataInputStream in = fs.open(path);
                lr = new LineReader(in, job.getConfiguration());
                int lineCount = 0;
                while (lr.readLine(new Text()) > 0) {
                    lineCount++;
                }
                int blockCount = lineCount / (13+2) / 5 + 1;
                int length = (13+2) * blockCount;
                splits.addAll(NLineInputFormat.getSplitsForFile(file, job.getConfiguration(), length));
            }
            return splits;
        }

    }

    public static class JSONRecordReader extends RecordReader<Text, Text> {
        int CurrentkeyCount = 0;
        LineRecordReader LDReader = null;
        Text key;
        Text value;

        @Override
        public void initialize(InputSplit InputSplit, TaskAttemptContext TaskAttemptContext) throws IOException, InterruptedException {
            //   close();
            LDReader = new LineRecordReader();
            LDReader.initialize(InputSplit, TaskAttemptContext);
        }
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            String currentVal = "";
            return nextKeyValue(CurrentkeyCount, currentVal);
        }

        public boolean nextKeyValue(int CurrentkeyCount, String currentVal)throws IOException{
            if (!LDReader.nextKeyValue()) {
                key = null;
                value = null;
                return false;
            }
            String nextLine = LDReader.getCurrentValue().toString().replaceAll("\\s+", "");
            if (!nextLine.equals("{")) {currentVal += nextLine;}
            if (LDReader.getCurrentValue().toString().contains("},") || LDReader.getCurrentValue().toString().contains("}")) {
                key = new Text(Integer.toString(CurrentkeyCount));
                value = new Text(currentVal.replaceAll("\"", ""));
                CurrentkeyCount++;
                return true;
            }
            return nextKeyValue(CurrentkeyCount, currentVal);
        }
        @Override
        public void close() throws IOException {
            LDReader = null;
            key = null;
            value = null;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return LDReader.getProgress();
        }
    }

    public static class MyMapper extends Mapper<Text, Text, Text, Text> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] l = value.toString().split(",");
            context.write(new Text(l[5].split(":")[1]), new Text(l[8].split(":")[1]));
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException,
                InterruptedException {
            int maxElevation = 0;
            int minElevation = 100000;

            for (Text v : value) {
                String l = v.toString();
                if(Integer.parseInt(l) > maxElevation){
                    maxElevation = Integer.parseInt(l);
                }
                if(Integer.parseInt(l) < minElevation){
                    minElevation = Integer.parseInt(l);
                }
            }

            context.write(key, new Text( maxElevation + "," + minElevation));
            // key is "Flags", value is " maximum elevation values in each flag, miminum elevation values in each flag
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CustomInput");
        JSONInputFormat JS = new JSONInputFormat();
        JS.setMaxInputSplitSize(job, 5);
        job.setJarByClass(CustomInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(JSONInputFormat.class); // use the class we create
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}