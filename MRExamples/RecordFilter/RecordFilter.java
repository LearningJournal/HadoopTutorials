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

public class RecordFilter {

  public static class RecordFilterMapper
       extends Mapper<Object, Text, Text, Text>{

        private Logger logger = Logger.getLogger(RecordFilterMapper.class);

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
           if (playtime<0){
               logger.info("Invalid-> "+ " start="+fields[4]+" end="+fields[5]+" diff="+playtime);
               context.write(new Text("Invalid"), value);
           }else{
               logger.info("Valid-> "+ " start="+fields[4]+" end="+fields[5]+" diff="+playtime);
               context.write(new Text("Valid"), value);
           }

      }catch(Exception e){
         logger.error(e.getMessage());
       }
    }
  }

  public static class RecordFilterReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

       for(Text val : values) {
          context.write(null, val);
       }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Record Filter");
    job.setJarByClass(RecordFilter.class);
    job.setMapperClass(RecordFilterMapper.class);
    job.setReducerClass(RecordFilterReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(2);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


