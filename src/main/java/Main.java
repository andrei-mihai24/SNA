import com.opencsv.exceptions.CsvValidationException;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main {
    public static void main(String[] args) throws IOException, CsvValidationException {
        PrintStream out = new PrintStream(new FileOutputStream("output.txt"));
        System.setOut(out);

        DataProcessor.getInstance().run();
    }

    /**
     * Source: https://stackoverflow.com/a/2581754
     */
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(final Map<K, V> map, final boolean descending) {
        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        list.sort(Map.Entry.comparingByValue());

        if (descending) {
            Collections.reverse(list);
        }

        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

    private static List<List<String>> transpose(Collection<Collection<String>> input) {
        assert input.stream().findFirst().isPresent();
        List<List<String>> retVal =
                IntStream.range(0, input.stream().findFirst().get().size())
                        .<List<String>>mapToObj(i -> new ArrayList<>())
                        .collect(Collectors.toList());

        input.forEach(r -> {
            AtomicInteger column = new AtomicInteger(0);
            r.forEach(value -> retVal.get(column.getAndIncrement()).add(value));
        });

        return retVal;
    }
}
