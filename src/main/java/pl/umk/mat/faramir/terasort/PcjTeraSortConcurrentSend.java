package pl.umk.mat.faramir.terasort;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.pcj.PCJ;
import org.pcj.PcjFuture;
import org.pcj.RegisterStorage;
import org.pcj.StartPoint;
import org.pcj.Storage;

/**
 * Seventh version of PcjTeraSort benchmark based on {@link PcjTeraSortOnePivotMultipleFiles}
 * and {@link PcjTeraSortHdfsConcurrentSend}.
 * <p>
 * Each thread has  one pivot (totalNumberOfPivots = threadCount),
 * with exception when number of threads is greater than number of elements in input.
 * <p>
 * The data is sent to other threads while reading input (overlapping).
 * <p>
 * Each thread writes output to separate files.
 */
@RegisterStorage(PcjTeraSortConcurrentSend.Vars.class)
public class PcjTeraSortConcurrentSend implements StartPoint {

    private static final long MEMORY_MAP_ELEMENT_COUNT = Long.parseLong(System.getProperty("memoryMap.elementCount", "1000000"));

    @Storage(PcjTeraSortConcurrentSend.class)
    enum Vars {
        pivots, buckets, finishedSending
    }

    @SuppressWarnings("serializable")
    private List<Element> pivots = new ArrayList<>();
    @SuppressWarnings("serializable")
    private List<Element> buckets = new LinkedList<>();
    private boolean finishedSending;

    public static void main(String[] args) throws IOException {
        if (args.length < 4) {
            System.err.println("Parameters: <input-file> <output-file-prefix> <total-pivots> <nodes-file>");
            return;
        }
        PCJ.executionBuilder(PcjTeraSortConcurrentSend.class)
                .addProperty("inputFile", args[0])
                .addProperty("outputDir", args[1])
                .addProperty("sampleSize", args[2])
                .addNodes(new File(args[3]))
                .start();
    }

    @Override
    public void main() throws Throwable {
        String inputFile = PCJ.getProperty("inputFile");
        String outputFilePrefix = PCJ.getProperty("outputDir");
        String outputFileSuffix = "-part-";
        String outputFileName = String.format("%s%s%05d", outputFilePrefix, outputFileSuffix, PCJ.myId());
        int sampleSize = Integer.parseInt(PCJ.getProperty("sampleSize"));
        int concurSendBucketSize = Integer.parseInt(System.getProperty("concurSendBucketSize", "100000"));

        if (PCJ.myId() == 0) {
            System.out.printf(Locale.ENGLISH, "Input file: %s%n", inputFile);
            System.out.printf(Locale.ENGLISH, "Output file prefix: %s%n", outputFilePrefix);
            System.out.printf(Locale.ENGLISH, "Sample size is: %d%n", sampleSize);
            System.out.printf(Locale.ENGLISH, "ConcurSend bucket size: %d%n", concurSendBucketSize);

            String namePrefix = new File(outputFilePrefix).getName() + outputFileSuffix;

            File parentDir = new File(outputFileName).getParentFile();
            if (parentDir != null) {
                parentDir.mkdirs();
            } else {
                parentDir = new File(".");
            }

            File[] files = parentDir.listFiles((dir, name) -> name.startsWith(namePrefix));
            if (files != null) {
                Arrays.stream(files).forEach(File::delete);
            }
        }

        long startTime = System.nanoTime();
        long readingStart = 0;
        long sendingStart = 0;

        try (TeraFileInput input = new TeraFileInput(inputFile)) {
            long totalElements = input.length();

            long localElementsCount = totalElements / PCJ.threadCount();
            long reminderElements = totalElements - localElementsCount * PCJ.threadCount();
            if (PCJ.myId() < reminderElements) {
                ++localElementsCount;
            }

            if (PCJ.myId() == 0) {
                System.out.printf(Locale.ENGLISH, "Total elements to sort: %d%n", totalElements);
                System.out.printf(Locale.ENGLISH, "Each thread reads about: %d%n", localElementsCount);
            }

            // every thread read own portion of input file
            long startElement = PCJ.myId() * (totalElements / PCJ.threadCount()) + Math.min(PCJ.myId(), reminderElements);
            long endElement = startElement + localElementsCount;

            // generate pivots (a unique set of keys at random positions: k0<k1<k2<...<k(n-1))
            int samplesByThread = (sampleSize + PCJ.threadCount() - (PCJ.myId() + 1)) / PCJ.threadCount();

            input.seek(startElement);
            for (int i = 0; i < samplesByThread; ++i) {
                Element pivot = input.readElement();
                pivots.add(pivot);
            }
            System.out.println("TL:" + PCJ.myId() + "\tread_samples\t" + (System.nanoTime() - startTime) / 1e9);
            PCJ.barrier();
            if (PCJ.myId() == 0) {
                pivots = PCJ.reduce((left, right) -> {
                    left.addAll(right);
                    return left;
                }, Vars.pivots);

                pivots = pivots.stream().distinct().sorted().collect(Collectors.toList()); // unique, sort
                int pivotsSize = pivots.size();
                int seekValue = Math.max(pivotsSize / PCJ.threadCount(), 1);
                pivots = IntStream.range(1, Math.min(PCJ.threadCount(), pivotsSize))
                        .map(i -> i * seekValue)
                        .mapToObj(pivots::get)
                        .collect(Collectors.toList());


                System.out.printf(Locale.ENGLISH, "Number of pivots: %d%n", pivots.size());
                PCJ.broadcast(pivots, Vars.pivots);
            }

            PCJ.waitFor(Vars.pivots);
            System.out.println("TL:" + PCJ.myId() + "\tget_pivots\t" + (System.nanoTime() - startTime) / 1e9);
            readingStart = System.nanoTime();

            @SuppressWarnings("unchecked")
            List<Element>[] localBuckets = (List<Element>[]) new List[pivots.size() + 1];
            for (int i = 0; i < localBuckets.length; ++i) {
                localBuckets[i] = new LinkedList<>();
            }

            System.out.printf(Locale.ENGLISH, "Thread %d started reading data%n", PCJ.myId());

            List<PcjFuture<Void>> sendFutures = new LinkedList<>();
            BiConsumer<List<Element>, Integer> sender = (bucket, bucketNo) -> {
                if (bucketNo != PCJ.myId()) {
                    PcjFuture<Void> future = PCJ.asyncAt(bucketNo, () -> {
                        LinkedList<Element> local = PCJ.getLocal(Vars.buckets);
                        synchronized (local) {
                            local.addAll(bucket);
                        }
                    });
                    sendFutures.add(future);
                } else {
                    synchronized (buckets) {
                        buckets.addAll(bucket);
                    }
                }
            };
            // for each element in own data: put element in proper bucket
            input.seek(startElement);
            for (long i = startElement; i < endElement; ++i) {
                Element e = input.readElement();
                int bucketNo = Collections.binarySearch(pivots, e);
                if (bucketNo < 0) {
                    bucketNo = -(bucketNo + 1);
                }
                List<Element> bucket = localBuckets[bucketNo];
                bucket.add(e);
                if (bucket.size() == concurSendBucketSize) {
                    sender.accept(bucket, bucketNo);
                    bucket.clear();
                }
            }
            System.out.printf(Locale.ENGLISH, "Thread %d finished reading data in %.7f seconds%n",
                    PCJ.myId(), (System.nanoTime() - readingStart) / 1e9);

            System.out.println("TL:" + PCJ.myId() + "\tread_data\t" + (System.nanoTime() - startTime) / 1e9);
            sendingStart = System.nanoTime();

            System.out.printf(Locale.ENGLISH, "Thread %d started sending buckets data%n", PCJ.myId());
            for (int i = 0; i < localBuckets.length; i++) {
                sender.accept(localBuckets[i], i);
                localBuckets[i].clear();
            }
            sendFutures.forEach(PcjFuture::get);
            PCJ.asyncBroadcast(true, Vars.finishedSending);
            System.out.printf(Locale.ENGLISH, "Thread %d finished sending data in %.7f seconds%n",
                    PCJ.myId(),
                    (System.nanoTime() - sendingStart) / 1e9);
            System.out.println("TL:" + PCJ.myId() + "\tsent_data\t" + (System.nanoTime() - startTime) / 1e9);
        }

        // sort buckets
        PCJ.waitFor(Vars.finishedSending, PCJ.threadCount());
        System.out.println("TL:" + PCJ.myId() + "\twaitfor_data\t" + (System.nanoTime() - startTime) / 1e9);
        long sortingStart = System.nanoTime();

        System.out.printf(Locale.ENGLISH, "Thread %d started sorting bucket%n", PCJ.myId());
        Element[] sortedBuckets = buckets.toArray(new Element[0]);
        Arrays.sort(sortedBuckets);

        System.out.printf(Locale.ENGLISH, "Thread %d finished sorting %d elements in %.7f seconds%n",
                PCJ.myId(),
                sortedBuckets.length,
                (System.nanoTime() - sortingStart) / 1e9);
        System.out.println("TL:" + PCJ.myId() + "\tsorted_data\t" + (System.nanoTime() - startTime) / 1e9);

        // save into file
        System.out.println("TL:" + PCJ.myId() + "\twaitfor_saving\t" + (System.nanoTime() - startTime) / 1e9);
        long savingStart = System.nanoTime();

        System.out.printf(Locale.ENGLISH, "Thread %d started saving buckets to file%n", PCJ.myId());
        try (TeraFileOutput output = new TeraFileOutput(outputFileName)) {
            output.writeElements(sortedBuckets);
        }

        System.out.printf(Locale.ENGLISH, "Thread %d finished saving %d elements in %.7f seconds%n",
                PCJ.myId(),
                sortedBuckets.length,
                (System.nanoTime() - savingStart) / 1e9);
        System.out.println("TL:" + PCJ.myId() + "\tsaved_data\t" + (System.nanoTime() - startTime) / 1e9);

        PCJ.barrier();
        // display execution time
        if (PCJ.myId() == 0) {
            long stopTime = System.nanoTime();
            System.out.printf(Locale.ENGLISH, "Start to Pivots completed:  %17.9f%n", (readingStart - startTime) / 1e9);
            System.out.printf(Locale.ENGLISH, "Start to Reading completed: %17.9f%n", (sendingStart - startTime) / 1e9);
            System.out.printf(Locale.ENGLISH, "Start to Sending completed: %17.9f%n", (sortingStart - startTime) / 1e9);
            System.out.printf(Locale.ENGLISH, "Start to Sorting completed: %17.9f%n", (savingStart - startTime) / 1e9);
            System.out.printf(Locale.ENGLISH, "Start to Saving completed:  %17.9f%n", (stopTime - startTime) / 1e9);
            System.out.printf(Locale.ENGLISH, "Total saving time: %.7f seconds%n", (stopTime - savingStart) / 1e9);
            System.out.printf(Locale.ENGLISH, "Total execution time: %.7f seconds%n", (stopTime - startTime) / 1e9);
        }
    }

    public static class TeraFileInput implements AutoCloseable {
        private static final int recordLength = 100;

        private static final int keyLength = 10;
        private static final int valueLength = recordLength - keyLength;
        private final FileChannel input;
        private final byte[] tempKeyBytes;
        private final byte[] tempValueBytes;
        private MappedByteBuffer mappedByteBuffer;
        private long minElementPos;
        private long maxElementPos;

        public TeraFileInput(String inputFile) throws FileNotFoundException {
            RandomAccessFile raf = new RandomAccessFile(inputFile, "r");
            input = raf.getChannel();
            tempKeyBytes = new byte[keyLength];
            tempValueBytes = new byte[valueLength];

            mappedByteBuffer = null;
            minElementPos = -1;
            maxElementPos = -1;
        }

        @Override
        public void close() throws Exception {
            input.close();
        }

        public long length() throws IOException {
            return input.size() / recordLength;
        }

        public void seek(long pos) throws IOException {
            if (minElementPos <= pos && pos < maxElementPos) {
                mappedByteBuffer.position((int) ((pos - minElementPos) * recordLength));
            } else {
                mappedByteBuffer = null;
            }
            input.position(pos * recordLength);
        }

        public Element readElement() throws IOException {
            if (mappedByteBuffer == null || !mappedByteBuffer.hasRemaining()) {
                long size = Math.min(input.size() - input.position(), MEMORY_MAP_ELEMENT_COUNT * recordLength);
                mappedByteBuffer = input.map(FileChannel.MapMode.READ_ONLY, input.position(), size);
                minElementPos = input.position() / recordLength;
                maxElementPos = minElementPos + size / recordLength;
            }
            mappedByteBuffer.get(tempKeyBytes);
            mappedByteBuffer.get(tempValueBytes);

            input.position(input.position() + recordLength);

            return new Element(new Text(tempKeyBytes), new Text(tempValueBytes));
        }
    }

    public static class TeraFileOutput implements AutoCloseable {
        private final BufferedOutputStream output;

        public TeraFileOutput(String outputFile) throws FileNotFoundException {
            output = new BufferedOutputStream(new FileOutputStream(outputFile, false));
        }

        public void writeElement(Element element) throws IOException {
            output.write(element.getKey().value);
            output.write(element.getValue().value);
        }

        public void writeElements(Element[] elements) throws UncheckedIOException {
            try {
                for (Element element : elements) {
                    writeElement(element);
                }
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }

        @Override
        public void close() throws Exception {
            output.close();
        }
    }

    public static class Text implements Comparable<Text>, Serializable {

        private byte[] value;

        public Text(byte[] value) {
            this.value = value.clone();
        }

        @Override
        public int compareTo(Text other) {
            byte[] buffer1 = this.value;
            byte[] buffer2 = other.value;
            if (buffer1 == buffer2) {
                return 0;
            }

            for (int i = 0, j = 0; i < buffer1.length && j < buffer2.length; ++i, ++j) {
                int a = (buffer1[i] & 0xFF);
                int b = (buffer2[i] & 0xFF);
                if (a != b) {
                    return a - b;
                }
            }
            return buffer1.length - buffer2.length;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Text)) {
                return false;
            }
            Text that = (Text) obj;
            return this.compareTo(that) == 0;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("Text{");
            for (byte b : value) {
                if ((b & 0xF0) == 0) sb.append('0');
                sb.append(Integer.toHexString(b & 0xFF));
            }
            sb.append('}');
            return sb.toString();
        }
    }

    public static class Element implements Comparable<Element>, Serializable {
        private Text key;
        private Text value;

        public Element(Text key, Text value) {
            this.key = key;
            this.value = value;
        }

        public Text getKey() {
            return key;
        }

        public Text getValue() {
            return value;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Element)) {
                return false;
            }
            Element element = (Element) obj;
            return key.equals(element.key) &&
                    value.equals(element.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }

        @Override
        public int compareTo(Element other) {
            int r = key.compareTo(other.key);
            if (r != 0) {
                return r;
            }
            return value.compareTo(other.value);
        }

        @Override
        public String toString() {
            return "Element{" +
                    "key=" + key +
                    ", value=" + value +
                    '}';
        }
    }
}
