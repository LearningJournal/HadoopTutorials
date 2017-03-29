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

public class PlaytimeAVG {

  public static class PlaytimeAVGMapper
       extends Mapper<Object, Text, Text, LongWritable>{

        private Logger logger = Logger.getLogger(PlaytimeAVGMapper.class);

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {      
      try{
           Calendar startTime = Calendar.getInstance();
           Calendar endTime = Calendar.getInstance();
           DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

           String[] fields = value.toString().split(",");
           
           startTime.setTime(df.parse(fields[4]));           
           endTime.setTime(df.parse(fields[5]));

           long playtime = (endTime.getTimeInMillis()- startTime.getTimeInMillis())/1000;
           logger.info("start="+fields[4]+" end="+fields[5]+" diff="+playtime);

           context.write(new Text("Playtime(sec)"), new LongWritable(playtime));

      }catch(Exception e){
         logger.error(e.getMessage());
       }
    }
  }

  public static class PlaytimeAVGReducer
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
    Job job = Job.getInstance(conf, "PLaytime AVG");
    job.setJarByClass(PlaytimeAVG.class);
    job.setMapperClass(PlaytimeAVGMapper.class);
    job.setReducerClass(PlaytimeAVGReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
