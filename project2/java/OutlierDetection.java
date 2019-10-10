import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OutlierDetection {

    public static class pointMapper
            extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            String radius = conf.get("radius");

            int r = Integer.parseInt(radius);
            int band = r + 1;

            String line = value.toString();
            String[] lines = value.toString().split(",");
            int x = Integer.parseInt(lines[0]);
            int y = Integer.parseInt(lines[1]);

            int xBy100 = x / 100;
            int yBy100 = y / 100;

            int xPlusRBy100 = (x + band) / 100;
            int xDecRBy100 = (x - band) / 100;

            int yPlusRBy100 = (y + band) / 100;
            int yDecRBy100 = (y - band) / 100;

            context.write(new Text(xBy100 + "," + yBy100), new Text("c," + line));

            if(xPlusRBy100 - xBy100 == 1 && yBy100 - yDecRBy100 == 1){
                context.write(new Text(xPlusRBy100 + "," + yDecRBy100), new Text("s," + line));
            }

            if(yBy100 - yDecRBy100 == 1 && xBy100 - xDecRBy100 == 1){
                context.write(new Text(xDecRBy100 + "," + yDecRBy100), new Text("s," + line));
            }

            if(xPlusRBy100 - xBy100 == 1 && yPlusRBy100 - yBy100 == 1){
                context.write(new Text(xPlusRBy100 + "," + yPlusRBy100), new Text("s," + line));
            }

            if(yPlusRBy100 - yBy100 == 1 && xBy100 - xDecRBy100 == 1){
                context.write(new Text(xDecRBy100 + "," + yPlusRBy100), new Text("s," + line));
            }

            if(xPlusRBy100 - xBy100 == 1 && yBy100 - yDecRBy100 == 0 &&  yPlusRBy100 - yBy100 == 0){
                context.write(new Text(xPlusRBy100 + "," +yBy100), new Text("s," + line));
            }

            if(xBy100 - xDecRBy100 == 1 && yBy100 - yDecRBy100 == 0 &&  yPlusRBy100 - yBy100 == 0){
                context.write(new Text(xDecRBy100 + "," + yBy100), new Text("s," + line));
            }

            if(yBy100 - yDecRBy100 == 1 && xBy100 - xDecRBy100 == 0 && xPlusRBy100 - xBy100 == 0){
                context.write(new Text(xBy100 + "," + yDecRBy100), new Text("s," + line));
            }

            if(yPlusRBy100 - yBy100 == 1 && xBy100 - xDecRBy100 == 0 && xPlusRBy100 - xBy100 == 0){
                context.write(new Text(xBy100 + "," +yPlusRBy100), new Text("s," + line));
            }

        }


    }


    public static class MyReducer
            extends Reducer<Text,Text,NullWritable,Text> {

        public void reduce(Text key, Iterable<Text> value, Context context
        ) throws IOException, InterruptedException {

            String[] lines;
            String tag;
            List<String> corePoints = new ArrayList<String>();
            List<String> suppPoints = new ArrayList<String>();

            Configuration conf = context.getConfiguration();
            String rStr = conf.get("radius");
            String kStr = conf.get("k");

            int r = Integer.parseInt(rStr);
            int k = Integer.parseInt(kStr);

            int cX; // the X of core points
            int cY; // the Y of core points
            int count; // count how many centroids are in the circle
            int sX; // the X of supporting points
            int sY; // the Y of supporting points
            String[] cXAndY;
            String[] sXAndY;

            for(Text v:value){
                lines = v.toString().split(",");
                tag = lines[0];
                if(tag.equals("c")){
                    corePoints.add(lines[1] + "," + lines[2]);
                }else if(tag.equals("s")){
                    suppPoints.add(lines[1] + "," + lines[2]);
                }
            }

            for(String c:corePoints){
                cXAndY = c.split(",");
                cX = Integer.parseInt(cXAndY[0]);
                cY = Integer.parseInt(cXAndY[1]);
                count = 0;
                for(String s:suppPoints){
                    sXAndY = s.split(",");
                    sX = Integer.parseInt(sXAndY[0]);
                    sY = Integer.parseInt(sXAndY[1]);
                    if(((sX - cX) * (sX - cX) + (sY - cY) * (sY - cY)) <= (r * r)){
                        count ++;
                    }
                }
                if(count < k){
                    // report the outlier points
                    context.write(NullWritable.get(), new Text(c));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("radius", args[2]); // In our test, we set radius to be 10
        conf.set("k", args[3]); // In our test, we set the number of k as 50
        Job job = new Job(conf, "OutlierDetection");
        job.setJarByClass(OutlierDetection.class);
        job.setMapperClass(pointMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
