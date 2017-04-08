import java.util.Calendar;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JsonTrans extends Configured implements Tool{

  public static class JsonTransMapper
       extends Mapper<Object, Text, Text, Text>{

        private Logger logger = Logger.getLogger(JsonTransMapper.class);

        public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      try{
           JSONObject jobj = new JSONObject();
           String[] fields = value.toString().split(",");
           DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
           Calendar startTime = Calendar.getInstance();
           startTime.setTime(df.parse(fields[4]));
           Calendar endTime = Calendar.getInstance();
           endTime.setTime(df.parse(fields[5]));
           long playtime = (endTime.getTimeInMillis()- startTime.getTimeInMillis())/1000;
           if (playtime>0){
               jobj.put("User ID",fields[0]);
               jobj.put("Video ID",fields[1]);
               jobj.put("Session ID",fields[2]);
               jobj.put("IP",fields[3]);
               jobj.put("Start Time",startTime.getTime());
               jobj.put("End Time",endTime.getTime());
               jobj.put("Player",fields[6]);
               jobj.put("Play Mode",fields[7]);
               logger.info(jobj.toString());
               context.write(null, new Text(jobj.toString()));
           }
      }catch(Exception e){
         logger.error(e.getMessage());
       }
    }
  }


  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new JsonTrans(),args);
    System.exit(exitCode);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
        System.err.println("Usage: JsonTrans [Generic Options] <in> <out>");
        return -1;
     }
    Configuration conf = getConf();
    Job job = Job.getInstance(conf, "Json Trans");
    job.setJarByClass(JsonTrans.class);
    job.setMapperClass(JsonTransMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(0);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }
}

