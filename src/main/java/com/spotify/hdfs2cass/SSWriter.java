package com.spotify.hdfs2cass;

import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import com.spotify.hadoop.mapred.OpenPGPInputFormat;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.AvroTextOutputFormat;
import org.apache.avro.mapred.AvroAsTextInputFormat;
import org.apache.commons.cli.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.NullOutputFormat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Iterator;

public class SSWriter
{
    private static final String USAGE = "[-c] [-d <date param>] -i <input path> -o <output path>";
    private static final String HEADER = "APFilter - Split AP messages";
    private static final String FOOTER = "";
    private static final int NBR_OF_REDUCERS = 25;

    public static void main(String[] args) throws Exception {
        CommandLine line = parseOptions(args);

        Path inputPath = new Path(line.getOptionValue('i'));
        Path outputPath = new Path(line.getOptionValue('o'));

        Class reducerClass;
        Class outputFormat = ColumnFamilyOutputFormat.class;

        Class mapKey;
        Class inputFormat;
        JobConf conf;

        reducerClass = PlainTextReducer.class;
        inputFormat = AvroAsTextInputFormat.class;
        mapKey = Text.class;
        System.out.println("Running in plain text mode..");

        String dateName = "unknown";
        if(line.hasOption('d')) {
            dateName = line.getOptionValue('d');
        }

        conf = prepareMOJob(inputPath, outputPath, inputFormat, PlainTextMapper.class, reducerClass, mapKey, NullWritable.class,
                Text.class, Text.class, outputFormat, new String("AP splitter (" + dateName + ")"), new String("APMessages"));


        JobClient.runJob(conf);
    }

    private static CommandLine parseOptions(String[] args) throws ParseException {
        CommandLineParser parser = new GnuParser();
        Options options = new Options();
        options.addOption("i", "input", true, "Input path");
        options.addOption("o", "output", true, "Output path");
        options.addOption("c", "crypto", false, "Input path is encrypted");
        options.addOption("d", "date", true, "Date used in the job name");

        CommandLine line = parser.parse(options, args);

        if(!line.hasOption('i')){
            printUsage(options);
            System.exit(1);
        }
        if(!line.hasOption('o')){
            printUsage(options);
            System.exit(1);
        }

        return line;
    }
    
    private static void printUsage(Options options){
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setWidth(80);
        helpFormatter.printHelp(USAGE, HEADER, options, FOOTER);
    }

    public static JobConf prepareMOJob(Path inputPath, Path outputPath, Class<? extends InputFormat> inputFormat,
                                       Class mapper, Class reducer, Class<? extends Writable> mapKey,
                                       Class<? extends Writable> mapValue, Class<? extends Writable> outputKey,
                                       Class<? extends Writable> outputValue, Class<? extends OutputFormat> outputFormat,
                                       String jobName, String outputName)
    {
        JobConf conf = new JobConf(SSWriter.class);
        conf.setJobName(jobName);
        conf.set("mapred.fairscheduler.pool", "production");

        FileInputFormat.addInputPath(conf, inputPath);
        FileOutputFormat.setOutputPath(conf, outputPath);
        FileOutputFormat.setCompressOutput(conf, true);

        conf.setInputFormat(inputFormat);
        conf.setOutputFormat(outputFormat);

        conf.setMapperClass(mapper);
        conf.setReducerClass(reducer);

        conf.setOutputKeyClass(outputKey);
        conf.setOutputValueClass(outputValue);

        conf.setMapOutputKeyClass(mapKey);
        conf.setMapOutputValueClass(mapValue);

        conf.setNumReduceTasks(NBR_OF_REDUCERS);

        AvroJob.setOutputCodec(conf, DataFileConstants.DEFLATE_CODEC);
        AvroJob.setReducerClass(conf, reducer);

        ConfigHelper.setColumnFamily(conf, "apa", "beta");

        return conf;
    }

    public static class PlainTextMapper
            implements Mapper<Text, Text, Text, NullWritable>
    {
        public void configure(JobConf conf)
        {
        }
        public void close() throws IOException
        {
        }
        public void map(Text key, Text value, OutputCollector<Text, NullWritable> output, Reporter reporter)
                throws IOException
        {
            output.collect(key, NullWritable.get());
        }
    }

    public static class PlainTextReducer extends AvroReducer
            implements Reducer<Text, NullWritable, NullWritable, Text>
    {
        public void reduce(Text key, Iterator<NullWritable> values, OutputCollector<NullWritable, Text> output, Reporter reporter)
                throws IOException
        {
            Text value = key;
            output.collect(NullWritable.get(), value);
        }
    }
}
