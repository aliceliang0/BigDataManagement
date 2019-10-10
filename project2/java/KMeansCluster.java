import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.net.URI;
import java.util.HashMap;

public class KMeansCluster {

    public static class MyMapper
            extends Mapper<Object, Text, Text, Text> {

        HashMap<Integer, String> xandY = new HashMap<Integer, String>();

        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader br;
            Path[] distributePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            String kCentroids = null;
            Integer i = 0;

            if(distributePaths[distributePaths.length - 1].toString().contains("kCentroids")){
                br = new BufferedReader(new FileReader(distributePaths[distributePaths.length - 1].toString()));
            }
            else{
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataInputStream fsOld = fs.open(distributePaths[distributePaths.length - 1]);
                br = new BufferedReader(new InputStreamReader(fsOld));
            }

            while (null != (kCentroids = br.readLine())) {
                String[] singleCentroids = kCentroids.split(",");
                xandY.put(i, singleCentroids[0] + "," + singleCentroids[1]); // key is the order of centroids, value is x-axis and y-axis of centroids
                i++;
            }


        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            double xCentroid;
            double yCentroid;

            int k = xandY.size(); // get how many centroids we have
            double[] distance = new double[k];

            String[] l = value.toString().split(",");
            int xPoint = Integer.parseInt(l[0]);
            int yPoint = Integer.parseInt(l[1]);

            for(int i = 0; i < k; i++){
                String[] xy = xandY.get(i).split(","); // get the x-axis and y-axis of ith centroids
                xCentroid = Double.parseDouble(xy[0]);
                yCentroid =  Double.parseDouble(xy[1]);
                distance[i] = Math.sqrt((xPoint-xCentroid)*(xPoint-xCentroid)+(yPoint-yCentroid)*(yPoint-yCentroid));
            }

            // find the index of centroid which is closest to the current point
            int minIndex = 0;
            for(int i = 1; i < k; i++){
                if(distance[minIndex] > distance[i]){
                    minIndex = i;
                }
            }

            context.write(new Text(xandY.get(minIndex)), value);

        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, NullWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int count = 0;
            long xSum = 0;
            long ySum = 0;
            float xAvg;
            float yAvg;

            for(Text v: values){
                String[] l = v.toString().split(",");
                xSum += Integer.parseInt(l[0]);
                ySum += Integer.parseInt(l[1]);
                count++;
            }

            xAvg = xSum / (float)count;
            yAvg = ySum / (float)count;

            context.write(new Text(xAvg+","+yAvg), NullWritable.get());
        }
    }

    public boolean isSameCentroids(String initlCentroids, String newCentroids) throws IOException {
        // if return false, then the centroids in two files are not in the same position
        // if return true, then the centroids in two files are in the same position
        // this method compare the result of first iteration
        String kCentroidsinit = null;
        String kCentroidsNew = null;
        BufferedReader brOld = new BufferedReader(new FileReader(initlCentroids));
        FileSystem hdfs = FileSystem.get(new Configuration());
        FSDataInputStream fsNew = hdfs.open(new Path(newCentroids));
        BufferedReader brNew = new BufferedReader(new InputStreamReader(fsNew));
        boolean flag = true;

        while(null!=(kCentroidsinit = brOld.readLine())){
            kCentroidsNew = brNew.readLine();
            if(!kCentroidsinit.equals(kCentroidsNew)){
                flag = false;
            }
        }
        return flag;
    }

    public boolean isSameCentroidsFS(String oldCentroids, String newCentroids) throws IOException {
        FileSystem hdfs = FileSystem.get(new Configuration());
        FSDataInputStream fsOld = hdfs.open(new Path(oldCentroids));
        FSDataInputStream fsNew = hdfs.open(new Path(newCentroids));

        BufferedReader brOld = new BufferedReader(new InputStreamReader(fsOld));
        BufferedReader brNew = new BufferedReader(new InputStreamReader(fsNew));

        String kCentroidsOld = null;
        String kCentroidsNew = null;

        boolean flag = true;

        while(null!=(kCentroidsOld = brOld.readLine())){
            kCentroidsNew = brNew.readLine();
            if(!kCentroidsOld.equals(kCentroidsNew)){
                flag = false;
            }
        }
        return flag;
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Job job = new Job(conf, "KMeansCluster");
        job.setJarByClass(KMeansCluster.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        DistributedCache.addCacheFile(new URI("/Users/daojun/Desktop/mapreduce/input_centroids/kCentroids.txt"), job.getConfiguration());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("outputKMeans" + "1"));
        job.waitForCompletion(true);

        int iter = 1;
        int index;
        KMeansCluster a = new KMeansCluster();
        boolean b = a.isSameCentroids("/Users/daojun/Desktop/mapreduce/input_centroids/kCentroids.txt", "/Users/daojun/Desktop/mapreduce/outputKMeans" + 1 + "/part-r-00000");

        while(!b){
            index = iter + 1;
            Configuration conf1 = new Configuration();
            Job job1 = new Job(conf1, "KMeansCluster");
            job1.setJarByClass(KMeansCluster.class);
            job1.setMapperClass(MyMapper.class);
            job1.setReducerClass(MyReducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            DistributedCache.addCacheFile(new URI("/Users/daojun/Desktop/mapreduce/outputKMeans"+ iter +"/part-r-00000"), job1.getConfiguration());
            FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path("/Users/daojun/Desktop/mapreduce/outputKMeans" + index));
            job1.waitForCompletion(true);
            b = a.isSameCentroidsFS("/Users/daojun/Desktop/mapreduce/outputKMeans"+ iter +"/part-r-00000","/Users/daojun/Desktop/mapreduce/outputKMeans"+ index +"/part-r-00000");
            iter ++;
            if(iter == 6) break;
        }

    }

}
