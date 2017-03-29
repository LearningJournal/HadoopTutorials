import java.util.Calendar;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class PlaytimeAVGByVideo{

  public static class PlaytimeAVGByVideoMapper
       extends Mapper<Object, Text, Text, LongWritable>{

        private Logger logger = Logger.getLogger(PlaytimeAVGByVideoMapper.class);

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {      
      try{
           String[] fields = value.toString().split(",");
           DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
           Calendar startTime = Calendar.getInstance();
           startTime.setTime(df.parse(fields[4]));
           Calendar endTime = Calendar.getInstance();
           endTime.setTime(df.parse(fields[5]));
           long playtime = (endTime.getTimeInMillis()- startTime.getTimeInMillis())/1000;
           logger.info("Video ID= "+fields[1]+ " start="+fields[4]+" end="+fields[5]+" diff="+playtime);

           context.write(new Text(fields[1]), new LongWritable(playtime));
      }catch(Exception e){
         logger.error(e.getMessage());
       }
    }
  }

  public static class PlaytimeAVGByVideoReducer
       extends Reducer<Text,LongWritable,Text,LongWritable> {

    public void reduce(Text key, Iterable<LongWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

       long avg = 0;
       long sum = 0;
       long cnt = 0;
      
       for (LongWritable val : values) {
           sum += val.get();
           cnt +=1;
       }
      avg = sum/cnt;

      context.write(key, new LongWritable(avg));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Playtime AVG By Video");
    job.setJarByClass(PlaytimeAVGByVideo.class);
    job.setMapperClass(PlaytimeAVGByVideoMapper.class);
    job.setReducerClass(PlaytimeAVGByVideoReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
