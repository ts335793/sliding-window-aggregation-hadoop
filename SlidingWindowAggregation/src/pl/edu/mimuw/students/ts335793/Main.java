package pl.edu.mimuw.students.ts335793;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

public class Main {
    public static class ProcessIdWritable extends LongWritable {
        ProcessIdWritable() {
            super();
        }

        ProcessIdWritable(long id) {
            super(id);
        }
    }

    public static class OrderedPairWritable implements WritableComparable {
        private LongWritable first;
        private LongWritable second;

        OrderedPairWritable() {
            super();
            this.first = new LongWritable();
            this.second = new LongWritable();
        }

        OrderedPairWritable(OrderedPairWritable other) {
            this.first = new LongWritable(other.first.get());
            this.second = new LongWritable(other.second.get());
        }

        OrderedPairWritable(LongWritable first, LongWritable second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            first.write(dataOutput);
            second.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            first.readFields(dataInput);
            second.readFields(dataInput);
        }

        @Override
        public int compareTo(Object o) {
            OrderedPairWritable other = (OrderedPairWritable) o;

            if (this.getFirst().get() < other.getFirst().get()) return -1;
            else if (this.getFirst().get() > other.getFirst().get()) return 1;
            else if (this.getSecond().get() < other.getSecond().get()) return -1;
            else if (this.getSecond().get() > other.getSecond().get()) return 1;
            else return 0;
        }

        public LongWritable getFirst() {
            return first;
        }

        public LongWritable getSecond() {
            return second;
        }

        @Override
        public String toString() {
            return first.toString() + "_" + second.toString();
        }
    }

    public static class OrderedPairArrayWritable extends ArrayWritable {
        OrderedPairArrayWritable() {
            super(OrderedPairWritable.class);
        }

        OrderedPairArrayWritable(OrderedPairWritable[] array) {
            super(OrderedPairWritable.class, array);
        }

        OrderedPairArrayWritable(ArrayList<OrderedPairWritable> orderedPairList) {
            super(OrderedPairWritable.class);
            set(orderedPairList.toArray(new Writable[orderedPairList.size()]));
        }

        @Override
        public String toString() {
            String elements = "";
            for (int i = 0; i < get().length; i++) {
                elements += get()[i].toString() + ", ";
            }
            return "[" + elements + "]";
        }
    }

    public static class LongArrayWithProcessIdWritable implements Writable {
        private ProcessIdWritable processId;
        private ArrayWritable longArray;

        LongArrayWithProcessIdWritable() {
            processId = new ProcessIdWritable();
            longArray = new ArrayWritable(LongWritable.class);
        }

        LongArrayWithProcessIdWritable(ProcessIdWritable processId, ArrayWritable longArray) {
            this.processId = processId;
            this.longArray = longArray;
        }


        @Override
        public void write(DataOutput dataOutput) throws IOException {
            processId.write(dataOutput);
            longArray.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            processId.readFields(dataInput);
            longArray.readFields(dataInput);
        }

        public ProcessIdWritable getProcessId() {
            return processId;
        }

        public ArrayWritable getLongArray() {
            return longArray;
        }

        @Override
        public String toString() {
            String output = "";
            for (String string : longArray.toStrings()) {
                output += string + ", ";
            }
            return "{" + processId.toString() + " -> [" + output + "] }";
        }
    }

    public static class SomeWritable extends GenericWritable {
        private static Class[] CLASSES = {
                LongWritable.class,
                OrderedPairWritable.class,
                LongArrayWithProcessIdWritable.class
        };

        SomeWritable() {
            super();
        }

        SomeWritable(SomeWritable other) {
            set(other.get());
        }

        SomeWritable(Writable writable) {
            set(writable);
        }

        @Override
        protected Class<? extends Writable>[] getTypes() {
            return CLASSES;
        }
    }

    public static class ContainsFileFilter implements PathFilter {
        private String pattern;

        ContainsFileFilter(String pattern) {
            this.pattern = pattern;
        }

        @Override
        public boolean accept(Path path) {
            return path.getName().contains(pattern);
        }
    }

    public static ArrayList<OrderedPairWritable> orderedPairIterableToArrayList(Iterable<OrderedPairWritable> orderedPairIterable) {
        ArrayList<OrderedPairWritable> orderedPairList = new ArrayList<OrderedPairWritable>();
        for (OrderedPairWritable orderedPair : orderedPairIterable) {
            orderedPairList.add(new OrderedPairWritable(orderedPair));
        }
        return orderedPairList;
    }

    public static ArrayList<SomeWritable> eitherLongOrOrderedPairIterableToArrayList(Iterable<SomeWritable> eitherLongOrOrderedPairIterable) {
        ArrayList<SomeWritable> eitherLongOrOrderedPairList = new ArrayList<SomeWritable>();
        for (SomeWritable eitherLongOrOrderedPair : eitherLongOrOrderedPairIterable) {
            eitherLongOrOrderedPairList.add(new SomeWritable(eitherLongOrOrderedPair));
        }
        return eitherLongOrOrderedPairList;
    }

    public static SequenceFile.Reader createSequenceFileReader(Configuration configuration, Path directory, String pattern) throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        FileStatus[] fileStatuses = fileSystem.listStatus(directory, new ContainsFileFilter(pattern));
        assert fileStatuses.length == 1;
        Path path = fileStatuses[0].getPath();
        return new SequenceFile.Reader(configuration, SequenceFile.Reader.file(path));
    }

    private static final ProcessIdWritable SPECIAL_PROCESS_ID = new ProcessIdWritable(0);

    private static final String PREFIX = "pl.edu.mimuw.students.ts335793";
    private static final String L = PREFIX + ".L";
    private static final String M = PREFIX + ".M";
    private static final String N = PREFIX + ".N";
    private static final String P = PREFIX + ".P";
    private static final String T = PREFIX + ".T";
    private static final String INPUT = PREFIX + ".input";
    private static final String PHASE_0 = PREFIX + ".phase_0";
    private static final String PHASE_1 = PREFIX + ".phase_1";
    private static final String PHASE_2 = PREFIX + ".phase_2";
    private static final String PHASE_3 = PREFIX + ".phase_3";
    private static final String PHASE_4 = PREFIX + ".phase_4";
    private static final String PHASE_5 = PREFIX + ".phase_5";

    private static final String SET_SIZE_FILE_NAME_PREFIX = "size";
    private static final String SEPARATORS_FILE_NAME_PREFIX = "part";
    private static final String RANK_FILE_NAME_PREFIX = "part";
    private static final String NUMBERS_FILENAME_PREFIX = "part";

    // Phase 0
    // - convert strings to writables
    // - calculate set size

    public static class Phase0Mapper extends Mapper<LongWritable, Text, ProcessIdWritable, SomeWritable> {
        @Override
        public void map(LongWritable lineNumber, Text line, Context context) throws IOException, InterruptedException {
            LongWritable number = new LongWritable(Long.parseLong(line.toString()));
            context.write(SPECIAL_PROCESS_ID, new SomeWritable(new OrderedPairWritable(number, lineNumber)));
        }
    }

    public static class Phase0Combiner extends Reducer<ProcessIdWritable, SomeWritable, ProcessIdWritable, SomeWritable> {
        @Override
        public void reduce(ProcessIdWritable processId, Iterable<SomeWritable> longOrOrderedPairIterable, Context context) throws IOException, InterruptedException {
            ArrayList<SomeWritable> eitherLongOrOrderedPairList = eitherLongOrOrderedPairIterableToArrayList(longOrOrderedPairIterable);
            context.write(SPECIAL_PROCESS_ID, new SomeWritable(new LongWritable(eitherLongOrOrderedPairList.size())));
            int t = context.getConfiguration().getInt(T, 1);
            int m = eitherLongOrOrderedPairList.size() / t;
            long counter = 0;
            for (SomeWritable longOrOrderedPair : eitherLongOrOrderedPairList) {
                context.write(new ProcessIdWritable(counter / m), longOrOrderedPair);
                counter++;
            }
        }
    }

    public static class Phase0Reducer extends Reducer<ProcessIdWritable, SomeWritable, OrderedPairWritable, NullWritable> {
        private MultipleOutputs multipleOutputs;

        @Override
        public void setup(Context context) {
            multipleOutputs = new MultipleOutputs(context);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }

        @Override
        public void reduce(ProcessIdWritable processId, Iterable<SomeWritable> longOrOrderedPairIterable, Context context) throws IOException, InterruptedException {
            long counter = 0;
            for (SomeWritable longOrOrderedPair : longOrOrderedPairIterable) {
                Writable writable = longOrOrderedPair.get();
                if (writable instanceof OrderedPairWritable) {
                    context.write((OrderedPairWritable) writable, NullWritable.get());
                } else {
                    counter += ((LongWritable) writable).get();
                }
            }
            if (processId.get() == SPECIAL_PROCESS_ID.get()) {
                multipleOutputs.write(SET_SIZE_FILE_NAME_PREFIX, new LongWritable(counter), NullWritable.get());
            }
        }
    }

    // Phase 1
    // - create separators

    public static class Phase1Mapper extends Mapper<OrderedPairWritable, NullWritable, ProcessIdWritable, OrderedPairWritable> {
        @Override
        public void map(OrderedPairWritable orderedPair, NullWritable empty, Context context) throws IOException, InterruptedException {
            context.write(SPECIAL_PROCESS_ID, orderedPair);
        }
    }

    public static class Phase1Combiner extends Reducer<ProcessIdWritable, OrderedPairWritable, ProcessIdWritable, OrderedPairWritable> {
        @Override
        public void reduce(ProcessIdWritable processId, Iterable<OrderedPairWritable> orderedPairIterable, Context context) throws IOException, InterruptedException {
            double p = context.getConfiguration().getDouble(P, 1.0);
            Random random = new Random();
            for (OrderedPairWritable orderedPair : orderedPairIterable) {
                if (random.nextDouble() <= p) {
                    context.write(processId, orderedPair);
                }
            }
        }
    }

    public static class Phase1Reducer extends Reducer<ProcessIdWritable, OrderedPairWritable, OrderedPairArrayWritable, NullWritable> {
        @Override
        public void reduce(ProcessIdWritable processId, Iterable<OrderedPairWritable> orderedPairIterable, Context context) throws IOException, InterruptedException {
            ArrayList<OrderedPairWritable> orderedPairList = orderedPairIterableToArrayList(orderedPairIterable);
            Collections.sort(orderedPairList);

            ArrayList<OrderedPairWritable> filteredOrderedPairList = new ArrayList<OrderedPairWritable>();
            int t = context.getConfiguration().getInt(T, 1);
            int s = orderedPairList.size();
            int coeff = ((s / t) + (s % t > 0 ? 1 : 0));
            for (int i = 1; i <= t - 1; i++) {
                int idx = i * coeff;
                if (idx < s) {
                    filteredOrderedPairList.add(orderedPairList.get(idx));
                }
            }

            OrderedPairArrayWritable separators = new OrderedPairArrayWritable(filteredOrderedPairList);
            context.write(separators, NullWritable.get());
        }
    }

    // Phase 2
    // - order numbers in partitions

    public static class Phase2Mapper extends Mapper<OrderedPairWritable, NullWritable, ProcessIdWritable, OrderedPairWritable> {
        private OrderedPairArrayWritable separators = new OrderedPairArrayWritable();

        @Override
        public void setup(Context context) throws IOException {
            Path phase_1_path = new Path(context.getConfiguration().get(PHASE_1));
            SequenceFile.Reader reader = createSequenceFileReader(context.getConfiguration(), phase_1_path, SEPARATORS_FILE_NAME_PREFIX);
            reader.next(separators, NullWritable.get());
        }

        @Override
        public void map(OrderedPairWritable orderedPair, NullWritable empty, Context context) throws IOException, InterruptedException {
            int i = 0;
            for (; i < separators.get().length; i++) {
                if (orderedPair.compareTo(separators.get()[i]) < 0) {
                    break;
                }
            }
            context.write(new ProcessIdWritable(i), orderedPair);
        }
    }

    public static class Phase2Reducer extends Reducer<ProcessIdWritable, OrderedPairWritable, ProcessIdWritable, OrderedPairArrayWritable> {

        @Override
        public void reduce(ProcessIdWritable processId, Iterable<OrderedPairWritable> orderedPairIterable, Context context) throws IOException, InterruptedException {
            ArrayList<OrderedPairWritable> orderedPairList = orderedPairIterableToArrayList(orderedPairIterable);
            Collections.sort(orderedPairList);
            context.write(processId, new OrderedPairArrayWritable(orderedPairList));
        }
    }

    // Phase 3
    // - calculate rank

    public static class Phase3Mapper extends Mapper<ProcessIdWritable, OrderedPairArrayWritable, ProcessIdWritable, OrderedPairWritable> {

        @Override
        public void map(ProcessIdWritable processId, OrderedPairArrayWritable orderedPairArray, Context context) throws IOException, InterruptedException {
            context.write(SPECIAL_PROCESS_ID, new OrderedPairWritable(processId, new LongWritable(orderedPairArray.get().length)));
        }
    }

    public static class Phase3Reducer extends Reducer<ProcessIdWritable, OrderedPairWritable, ArrayWritable, NullWritable> {

        @Override
        public void reduce(ProcessIdWritable processId, Iterable<OrderedPairWritable> orderedPairIterable, Context context) throws IOException, InterruptedException {
            ArrayList<OrderedPairWritable> orderedPairList = orderedPairIterableToArrayList(orderedPairIterable);
            Collections.sort(orderedPairList);

            LongWritable[] rank = new LongWritable[orderedPairList.size()];
            rank[0] = new LongWritable(0);
            for (int i = 1; i < orderedPairList.size(); i++) {
                rank[i] = new LongWritable(rank[i - 1].get() + orderedPairList.get(i - 1).getSecond().get());
            }
            context.write(new ArrayWritable(LongWritable.class, rank), NullWritable.get());
        }
    }

    // Phase 4
    // - do perfect balancing

    public static class Phase4Mapper extends Mapper<ProcessIdWritable, OrderedPairArrayWritable, ProcessIdWritable, OrderedPairWritable> {
        private ArrayWritable rank = new ArrayWritable(LongWritable.class);

        @Override
        public void setup(Context context) throws IOException {
            Path phase_3_path = new Path(context.getConfiguration().get(PHASE_3));
            SequenceFile.Reader reader = createSequenceFileReader(context.getConfiguration(), phase_3_path, RANK_FILE_NAME_PREFIX);
            reader.next(rank, NullWritable.get());
        }

        @Override
        public void map(ProcessIdWritable processId, OrderedPairArrayWritable orderedPairArray, Context context) throws IOException, InterruptedException {
            int m = context.getConfiguration().getInt(M, 1);
            long rank_0 = ((LongWritable) rank.get()[(int) processId.get()]).get();
            for (int i = 0; i < orderedPairArray.get().length; i++) {
                context.write(new ProcessIdWritable((rank_0 + i) / m), (OrderedPairWritable) orderedPairArray.get()[i]);
            }
        }
    }

    public static class Phase4Reducer extends Reducer<ProcessIdWritable, OrderedPairWritable, ProcessIdWritable, OrderedPairArrayWritable> {
        @Override
        public void reduce(ProcessIdWritable processId, Iterable<OrderedPairWritable> orderedPairIterable, Context context) throws IOException, InterruptedException {
            ArrayList<OrderedPairWritable> orderedPairList = orderedPairIterableToArrayList(orderedPairIterable);
            Collections.sort(orderedPairList);
            context.write(processId, new OrderedPairArrayWritable(orderedPairList));
        }
    }

    // Aggregation
    // - sum numbers on the interval

    public static class AggregationMapper extends Mapper<ProcessIdWritable, OrderedPairArrayWritable, ProcessIdWritable, SomeWritable> {
        @Override
        public void map(ProcessIdWritable processId, OrderedPairArrayWritable orderedPairArray, Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            int t = configuration.getInt(T, 1);
            int l = configuration.getInt(L, 1);
            int m = configuration.getInt(M, 1);
            long sum = 0;
            for (Writable writable : orderedPairArray.get()) {
                sum += ((OrderedPairWritable) writable).getFirst().get();
            }
            for (int i = 0; i < t; i++) {
                context.write(new ProcessIdWritable(i), new SomeWritable(new OrderedPairWritable(processId, new LongWritable(sum))));
            }
            LongWritable[] prefixSum = new LongWritable[orderedPairArray.get().length + 1];
            prefixSum[0] = new LongWritable(0);
            for (int i = 0; i < orderedPairArray.get().length; i++) {
                prefixSum[i + 1] = new LongWritable(prefixSum[i].get() + ((OrderedPairWritable) (orderedPairArray.get())[i]).getFirst().get());
            }
            SomeWritable labeledArray = new SomeWritable(new LongArrayWithProcessIdWritable(processId, new ArrayWritable(LongWritable.class, prefixSum)));
            context.write(processId, labeledArray);
            if (l <= m) {
                if (processId.get() + 1 < t) {
                    context.write(new ProcessIdWritable(processId.get() + 1), labeledArray);
                }
            } else {
                if (processId.get() + (l - 1) / m < t) {
                    context.write(new ProcessIdWritable(processId.get() + (l - 1) / m), labeledArray);
                }
                if (processId.get() + (l - 1) / m + 1 < t) {
                    context.write(new ProcessIdWritable(processId.get() + (l - 1) / m + 1), labeledArray);
                }
            }
        }
    }

    public static class AggregationReducer extends Reducer<ProcessIdWritable, SomeWritable, ProcessIdWritable, OrderedPairArrayWritable> {
        @Override
        public void reduce(ProcessIdWritable processId, Iterable<SomeWritable> eitherOrderedPairOrOrderedPairArrayIterable, Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            int t = configuration.getInt(T, 1);
            int m = configuration.getInt(M, 1);
            int l = configuration.getInt(L, 1);
            LongWritable[] w = new LongWritable[t];
            ArrayWritable[] prefixSums = new ArrayWritable[t];
            for (SomeWritable eitherOrderedPairOrOrderedPairArray : eitherOrderedPairOrOrderedPairArrayIterable) {
                if (eitherOrderedPairOrOrderedPairArray.get() instanceof OrderedPairWritable) {
                    OrderedPairWritable orderedPair = (OrderedPairWritable) eitherOrderedPairOrOrderedPairArray.get();
                    w[(int) orderedPair.getFirst().get()] = new LongWritable(orderedPair.getSecond().get());
                } else {
                    LongArrayWithProcessIdWritable longArrayWithProcessId = (LongArrayWithProcessIdWritable) eitherOrderedPairOrOrderedPairArray.get();
                    prefixSums[(int) longArrayWithProcessId.getProcessId().get()] = longArrayWithProcessId.getLongArray();
                }
            }
            OrderedPairWritable[] aggregatedValues = new OrderedPairWritable[prefixSums[(int) processId.get()].get().length - 1];
            for (int i = 0; i < prefixSums[(int) processId.get()].get().length - 1; i++) {
                long w1 = 0;
                long w2 = 0;
                long w3 = 0;
                int iRank = (int) processId.get() * m + i;
                int firstRank = iRank - l + 1;
                int alpha = (firstRank < 0) ? -1 : firstRank / m;
                if (firstRank >= 0) {
                    int firstIndex = firstRank - alpha * m;
                    int lastIndex;
                    if (alpha < processId.get()) {
                        lastIndex = m - 1;
                    } else {
                        lastIndex = i;
                    }
                    w1 = ((LongWritable) prefixSums[alpha].get()[lastIndex + 1]).get() - ((LongWritable) prefixSums[alpha].get()[firstIndex]).get();
                }
                for (int j = alpha + 1; j < processId.get(); j++) {
                    w2 += w[j].get();
                }
                if (alpha < processId.get()) {
                    w3 = ((LongWritable) prefixSums[(int) processId.get()].get()[i + 1]).get();
                }
                aggregatedValues[i] = new OrderedPairWritable(new LongWritable(((LongWritable) prefixSums[(int) processId.get()].get()[i + 1]).get() - ((LongWritable) prefixSums[(int) processId.get()].get()[i]).get()), new LongWritable(w1 + w2 + w3));
            }
            context.write(processId, new OrderedPairArrayWritable(aggregatedValues));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);

        configuration.set(INPUT, "/input");
        configuration.set(PHASE_0, "/phase_0");
        configuration.set(PHASE_1, "/phase_1");
        configuration.set(PHASE_2, "/phase_2");
        configuration.set(PHASE_3, "/phase_3");
        configuration.set(PHASE_4, "/phase_4");
        configuration.set(PHASE_5, "/phase_5");

        configuration.setInt(T, 5);
        //configuration.setInt(L, 3);
        configuration.setInt(L, 20);

        {
            Job job0 = Job.getInstance(configuration, "size");
            job0.setNumReduceTasks(configuration.getInt(T, 1));

            job0.setJarByClass(Main.class);

            job0.setMapperClass(Phase0Mapper.class);
            job0.setCombinerClass(Phase0Combiner.class);
            job0.setReducerClass(Phase0Reducer.class);

            job0.setMapOutputKeyClass(ProcessIdWritable.class);
            job0.setMapOutputValueClass(SomeWritable.class);

            job0.setOutputKeyClass(OrderedPairWritable.class);
            job0.setOutputValueClass(NullWritable.class);

            job0.setInputFormatClass(TextInputFormat.class);
            job0.setOutputFormatClass(SequenceFileOutputFormat.class);
            //job0.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job0, new Path(configuration.get(INPUT)));
            FileOutputFormat.setOutputPath(job0, new Path(configuration.get(PHASE_0)));

            MultipleOutputs.addNamedOutput(job0, SET_SIZE_FILE_NAME_PREFIX, SequenceFileOutputFormat.class, LongWritable.class, NullWritable.class);
            //MultipleOutputs.addNamedOutput(job0, SET_SIZE_FILE_NAME_PREFIX, TextOutputFormat.class, LongWritable.class, NullWritable.class);

            job0.waitForCompletion(true);
        }

        {
            LongWritable size = new LongWritable();
            SequenceFile.Reader reader = createSequenceFileReader(configuration, new Path(configuration.get(PHASE_0)), SET_SIZE_FILE_NAME_PREFIX);
            reader.next(size);
            configuration.setInt(N, (int) size.get());
            configuration.setInt(M, configuration.getInt(N, 1) / configuration.getInt(T, 1));
            configuration.setDouble(P, (1.0 / configuration.getInt(M, 1)) * Math.log(configuration.getInt(N, 1) * configuration.getInt(T, 1)));
        }

        {
            Job job1 = Job.getInstance(configuration, "separators");
            job1.setNumReduceTasks(1);

            job1.setJarByClass(Main.class);

            job1.setMapperClass(Phase1Mapper.class);
            job1.setCombinerClass(Phase1Combiner.class);
            job1.setReducerClass(Phase1Reducer.class);

            job1.setMapOutputKeyClass(ProcessIdWritable.class);
            job1.setMapOutputValueClass(OrderedPairWritable.class);

            job1.setOutputKeyClass(OrderedPairArrayWritable.class);
            job1.setOutputValueClass(NullWritable.class);

            job1.setInputFormatClass(SequenceFileInputFormat.class);
            job1.setOutputFormatClass(SequenceFileOutputFormat.class);

            for (FileStatus fileStatus : fileSystem.listStatus(new Path(configuration.get(PHASE_0)), new ContainsFileFilter(NUMBERS_FILENAME_PREFIX))) {
                FileInputFormat.addInputPath(job1, fileStatus.getPath());
            }
            FileOutputFormat.setOutputPath(job1, new Path(configuration.get(PHASE_1)));

            job1.waitForCompletion(true);
        }

        {
            Job job2 = Job.getInstance(configuration, "sort");
            job2.setNumReduceTasks(configuration.getInt(T, 1));

            job2.setJarByClass(Main.class);

            job2.setMapperClass(Phase2Mapper.class);
            job2.setReducerClass(Phase2Reducer.class);

            job2.setMapOutputKeyClass(ProcessIdWritable.class);
            job2.setMapOutputValueClass(OrderedPairWritable.class);

            job2.setOutputKeyClass(ProcessIdWritable.class);
            job2.setOutputValueClass(OrderedPairArrayWritable.class);

            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);

            for (FileStatus fileStatus : fileSystem.listStatus(new Path(configuration.get(PHASE_0)), new ContainsFileFilter(NUMBERS_FILENAME_PREFIX))) {
                FileInputFormat.addInputPath(job2, fileStatus.getPath());
            }
            FileOutputFormat.setOutputPath(job2, new Path(configuration.get(PHASE_2)));

            job2.waitForCompletion(true);
        }

        {
            Job job3 = Job.getInstance(configuration, "rank");
            job3.setNumReduceTasks(1);

            job3.setJarByClass(Main.class);

            job3.setMapperClass(Phase3Mapper.class);
            job3.setReducerClass(Phase3Reducer.class);

            job3.setMapOutputKeyClass(ProcessIdWritable.class);
            job3.setMapOutputValueClass(OrderedPairWritable.class);

            job3.setOutputKeyClass(ArrayWritable.class);
            job3.setOutputValueClass(NullWritable.class);

            job3.setInputFormatClass(SequenceFileInputFormat.class);
            job3.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.addInputPath(job3, new Path(configuration.get(PHASE_2)));
            FileOutputFormat.setOutputPath(job3, new Path(configuration.get(PHASE_3)));

            job3.waitForCompletion(true);
        }

        {
            Job job4 = Job.getInstance(configuration, "balance");
            job4.setNumReduceTasks(configuration.getInt(T, 1));

            job4.setJarByClass(Main.class);

            job4.setMapperClass(Phase4Mapper.class);
            job4.setReducerClass(Phase4Reducer.class);

            job4.setMapOutputKeyClass(ProcessIdWritable.class);
            job4.setMapOutputValueClass(OrderedPairWritable.class);

            job4.setOutputKeyClass(ProcessIdWritable.class);
            job4.setOutputValueClass(OrderedPairArrayWritable.class);

            job4.setInputFormatClass(SequenceFileInputFormat.class);
            job4.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.addInputPath(job4, new Path(configuration.get(PHASE_2)));
            FileOutputFormat.setOutputPath(job4, new Path(configuration.get(PHASE_4)));

            job4.waitForCompletion(true);
        }

        {
            Job job5 = Job.getInstance(configuration, "aggregate");
            job5.setNumReduceTasks(configuration.getInt(T, 1));

            job5.setJarByClass(Main.class);

            job5.setMapperClass(AggregationMapper.class);
            job5.setReducerClass(AggregationReducer.class);

            job5.setMapOutputKeyClass(ProcessIdWritable.class);
            job5.setMapOutputValueClass(SomeWritable.class);

            job5.setOutputKeyClass(ProcessIdWritable.class);
            job5.setOutputValueClass(OrderedPairArrayWritable.class);

            job5.setInputFormatClass(SequenceFileInputFormat.class);
            job5.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job5, new Path(configuration.get(PHASE_4)));
            FileOutputFormat.setOutputPath(job5, new Path(configuration.get(PHASE_5)));

            job5.waitForCompletion(true);
        }
    }
}
