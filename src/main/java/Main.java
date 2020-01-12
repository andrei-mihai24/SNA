import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVReaderHeaderAware;
import com.opencsv.CSVReaderHeaderAwareBuilder;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main {

    public static final String PATH = "C:\\Users\\Andrei\\Desktop\\Data files 01.05.2019-02.05.2019 (40,8 MB)-20200112\\Votes_01052019_02052019.csv";

    public static void main(String[] args) throws IOException, CsvValidationException {
        PrintStream out = new PrintStream(new FileOutputStream("output.txt"));
        System.setOut(out);

        CSVReaderBuilder csvReaderBuilder = new CSVReaderHeaderAwareBuilder(new FileReader(PATH))
                .withCSVParser(new CSVParserBuilder().withSeparator(';').build());
        CSVReaderHeaderAware csvReaderHeaderAware = ((CSVReaderHeaderAwareBuilder) csvReaderBuilder).build();
        List<Collection<String>> rows = new ArrayList<>();
        Map<String, String> nextRow;
        while ((nextRow = csvReaderHeaderAware.readMap()) != null) {
            System.out.println(nextRow.keySet());
            rows.add(nextRow.values());
        }

        var votings = transpose(rows);
        //votings.forEach(System.out::println);
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
