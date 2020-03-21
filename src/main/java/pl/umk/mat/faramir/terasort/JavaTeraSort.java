package pl.umk.mat.faramir.terasort;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class JavaTeraSort {

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Parameters: <input-file> <output-file>");
            return;
        }
        String inputFile = args[0];
        String outputFile = args[1];

        new File(outputFile).delete();

        long startTime = System.nanoTime();
        List<Element> list = null;

        try (TeraFileInput input = new TeraFileInput(inputFile)) {
            long totalElements = input.length();
            list = new ArrayList<Element>((int) totalElements);

            System.out.printf(Locale.ENGLISH, "Total elements to sort: %d%n", totalElements);

            long readingStart = System.nanoTime();

            System.out.printf(Locale.ENGLISH, "Started reading data%n");

            // for each element in own data: put element in proper bucket
            input.seek(0);
            for (long i = 0; i < totalElements; ++i) {
                Element e = input.readElement();
                list.add(e);
            }
            System.out.printf(Locale.ENGLISH, "Finished reading data in %.7f seconds%n",
                    (System.nanoTime() - readingStart) / 1e9);
        }

        // sort buckets
        long sortingStart = System.nanoTime();

        System.out.printf(Locale.ENGLISH, "Started sorting buckets%n");
        list.sort(null);
        System.out.printf(Locale.ENGLISH, "Finished sorting %d elements in %.7f seconds%n",
                list.size(),
                (System.nanoTime() - sortingStart) / 1e9);

        // save into file
        long savingStart = System.nanoTime();

        System.out.printf(Locale.ENGLISH, "Started saving buckets to file%n");
        try (TeraFileOutput output = new TeraFileOutput(outputFile)) {
            for (Element element : list) {
                output.writeElement(element);
            }
        }
        System.out.printf(Locale.ENGLISH, "Finished saving %d elements in %.7f seconds%n",
                list.size(),
                (System.nanoTime() - savingStart) / 1e9);

        // display execution time
        long stopTime = System.nanoTime();
        System.out.printf(Locale.ENGLISH, "Start to Reading completed: %17.9f%n", (sortingStart - startTime) / 1e9);
        System.out.printf(Locale.ENGLISH, "Start to Sorting completed: %17.9f%n", (savingStart - startTime) / 1e9);
        System.out.printf(Locale.ENGLISH, "Start to Saving completed:  %17.9f%n", (stopTime - startTime) / 1e9);
        System.out.printf(Locale.ENGLISH, "Total saving time: %.7f seconds%n", (stopTime - savingStart) / 1e9);
        System.out.printf(Locale.ENGLISH, "Total execution time: %.7f seconds%n", (stopTime - startTime) / 1e9);
    }

    public static class TeraFileInput implements AutoCloseable {
        private static final int recordLength = 100;

        private static final int keyLength = 10;
        private static final int valueLength = recordLength - keyLength;
        private final FileChannel input;
        private final byte[] tempKeyBytes;
        private final byte[] tempValueBytes;
        private MappedByteBuffer mappedByteBuffer;

        public TeraFileInput(String inputFile) throws FileNotFoundException {
            RandomAccessFile raf = new RandomAccessFile(inputFile, "r");
            input = raf.getChannel();
            tempKeyBytes = new byte[keyLength];
            tempValueBytes = new byte[valueLength];

            mappedByteBuffer = null;
        }

        @Override
        public void close() throws IOException {
            input.close();
        }

        public long length() throws IOException {
            return input.size() / recordLength;
        }

        public void seek(long pos) throws IOException {
            mappedByteBuffer = null;
            input.position(pos * recordLength);
        }

        public Element readElement() throws IOException {
            if (mappedByteBuffer == null || mappedByteBuffer.remaining() == 0) {
                mappedByteBuffer = input.map(FileChannel.MapMode.READ_ONLY,
                        input.position(),
                        Math.min(input.size() - input.position(), 1_000_000 * recordLength));
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
            output = new BufferedOutputStream(new FileOutputStream(outputFile, true));
        }

        public void writeElement(Element element) throws IOException {
            output.write(element.getKey().value);
            output.write(element.getValue().value);
        }

        @Override
        public void close() throws IOException {
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
