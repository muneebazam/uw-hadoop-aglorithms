/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfFloats;
import tl.lin.data.map.HMapStFW;
import tl.lin.data.map.HashMapWritable;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.lang.Math;

/**
 * Simple word count demo.
 */
public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);
  
  public static final class OccurrenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD = new Text();
    private static final int MAX_WORDS = 40;

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(value.toString());
      Set<String> WORD_SET = new HashSet();
      for (int i = 0; i < Math.min(MAX_WORDS, tokens.size()); i++) {
        if (WORD_SET.add(tokens.get(i))) {
          WORD.set(tokens.get(i));
          context.write(WORD, ONE);
        } 
      }
    }
  }

  private static final class CoOccurrenceMapper extends Mapper<LongWritable, Text, Text, HMapStFW> {
    private static final Text WORD = new Text();
    private static final int MAX_WORDS = 40;

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(value.toString());
      Set<String> word_map = new HashSet<>();
      for (int i = 0; i < Math.min(MAX_WORDS, tokens.size()); i++) {
        word_map.add(tokens.get(i));
      }
      for (int i = 0; i < tokens.size(); i++) {
        HMapStFW MAP = new HMapStFW();
        WORD.set(tokens.get(i));
        for (int j = 0; j < tokens.size(); j++) {
          if ((tokens.get(i).equals(tokens.get(j)) || (i == j))) continue;
          MAP.put(tokens.get(j), 1.0f);
          context.write(WORD, MAP);
        }
      }
    }
  }

  private static final class CoOccurrenceCombiner extends Reducer<Text, HMapStFW, Text, HMapStFW> {

    @Override
    public void reduce(Text key, Iterable<HMapStFW> values, Context context)
            throws IOException, InterruptedException {
      Iterator<HMapStFW> iter = values.iterator();
      HMapStFW SUM = new HMapStFW();
      while (iter.hasNext()) {
        SUM.plus(iter.next());
      }
      context.write(key, SUM);
    }
  }

  public static final class OccurrenceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  private static final class CoOccurrenceReducer extends Reducer<Text, HMapStFW, Text, HashMapWritable> {
    private static Map<String, Integer> COUNT_MAP = new HashMap<>();
    private static final Text KEY = new Text();
    private static final HashMapWritable WORD_MAP = new HashMapWritable();
    private static long THRESHOLD = 1;
    private static long NUM_LINES;
    
    @Override
    public void setup(Context context) {
      NUM_LINES = context.getConfiguration().getLong("NUM_LINES", 0);
      THRESHOLD = context.getConfiguration().getLong("threshold", 1);
      try {
        FileSystem file_system = FileSystem.get(context.getConfiguration());
        FileStatus[] part_files = file_system.globStatus(new Path("temp_part_files/part-r-*"));

        for (FileStatus part_file : part_files) {
          FSDataInputStream input = file_system.open(part_file.getPath());
          InputStreamReader input_reader = new InputStreamReader(input, "UTF-8");
          BufferedReader buffered_reader = new BufferedReader(input_reader);

          // begin reading lines
          String line = buffered_reader.readLine();
          while (line != null) {
            String[] data = line.split("\\s+");
            if (data.length == 2) {
              COUNT_MAP.put(data[0], Integer.parseInt(data[1]));
            }
            line = buffered_reader.readLine();
          }
          buffered_reader.close();
        }
      } catch (java.io.IOException e) {
        LOG.error("Exception while trying to read part files");
      }
    }

    @Override
    public void reduce(Text key, Iterable<HMapStFW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStFW> iter = values.iterator();
      HMapStFW map = new HMapStFW();
      while (iter.hasNext()) {
        map.plus(iter.next());
      }
      String word_key = key.toString();
      KEY.set(word_key);
      WORD_MAP.clear();
      for (String word : map.keySet()) {
        float count = map.get(word);
        if (count >= THRESHOLD) {
          float coOccurFreq = (float) count * NUM_LINES;
          float wordOneFreq = COUNT_MAP.get(word_key);
          float wordTwoFreq = COUNT_MAP.get(word);
          PairOfFloats PMI = new PairOfFloats();
          PMI.set((float) Math.log10((coOccurFreq / (wordOneFreq * wordTwoFreq))), count);
          WORD_MAP.put(word, PMI);
        }
      }
      if (WORD_MAP.size() > 0) {
        context.write(KEY, WORD_MAP);
      }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private StripesPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-threshold", metaVar = "[path]", required = false, usage = "threshold amount")
    int threshold = 1;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-imc", usage = "use in-mapper combining")
    boolean imc = false;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);
    LOG.info(" - use in-mapper combining: " + args.imc);

    Configuration conf = getConf();
    Job occurrenceJob = Job.getInstance(conf);
    occurrenceJob.setJobName(StripesPMI.class.getSimpleName());
    occurrenceJob.setJarByClass(StripesPMI.class);
    occurrenceJob.setNumReduceTasks(args.numReducers);
    occurrenceJob.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    occurrenceJob.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    occurrenceJob.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    occurrenceJob.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    occurrenceJob.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    FileInputFormat.setInputPaths(occurrenceJob, new Path(args.input));
    FileOutputFormat.setOutputPath(occurrenceJob, new Path("temp_part_files"));

    occurrenceJob.setMapOutputKeyClass(Text.class);
    occurrenceJob.setMapOutputValueClass(IntWritable.class);
    occurrenceJob.setOutputKeyClass(Text.class);
    occurrenceJob.setOutputValueClass(IntWritable.class);
    occurrenceJob.setOutputFormatClass(TextOutputFormat.class);

    occurrenceJob.setMapperClass(OccurrenceMapper.class);
    occurrenceJob.setCombinerClass(OccurrenceReducer.class);
    occurrenceJob.setReducerClass(OccurrenceReducer.class);

    // Delete the output directory if it exists already.
    Path tempOutputDir = new Path("temp_part_files");
    FileSystem.get(conf).delete(tempOutputDir, true);

    long startTime = System.currentTimeMillis();
    occurrenceJob.waitForCompletion(true);
    LOG.info("occurrenceJob Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    long LINE_COUNT = occurrenceJob.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();

  /*******************************************************************************************************/

    Configuration conf2 = getConf();
    Job coOccurrenceJob = Job.getInstance(conf);
    coOccurrenceJob.setJobName(StripesPMI.class.getSimpleName());
    coOccurrenceJob.setJarByClass(StripesPMI.class);
    coOccurrenceJob.setNumReduceTasks(args.numReducers);
    coOccurrenceJob.getConfiguration().setInt("threshold", args.threshold);
    coOccurrenceJob.getConfiguration().setLong("NUM_LINES", LINE_COUNT);
    coOccurrenceJob.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    coOccurrenceJob.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    coOccurrenceJob.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    coOccurrenceJob.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    coOccurrenceJob.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    FileInputFormat.setInputPaths(coOccurrenceJob, new Path(args.input));
    FileOutputFormat.setOutputPath(coOccurrenceJob, new Path(args.output));

    coOccurrenceJob.setMapOutputKeyClass(Text.class);
    coOccurrenceJob.setMapOutputValueClass(HMapStFW.class);
    coOccurrenceJob.setOutputKeyClass(Text.class);
    coOccurrenceJob.setOutputValueClass(HashMapWritable.class);
    coOccurrenceJob.setOutputFormatClass(TextOutputFormat.class);

    coOccurrenceJob.setMapperClass(CoOccurrenceMapper.class);
    coOccurrenceJob.setCombinerClass(CoOccurrenceCombiner.class);
    coOccurrenceJob.setReducerClass(CoOccurrenceReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime2 = System.currentTimeMillis();
    coOccurrenceJob.waitForCompletion(true);
    LOG.info("coOccurrenceJob Finished in " + (System.currentTimeMillis() - startTime2) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}