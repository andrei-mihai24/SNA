import ch.netzwerg.paleo.*;
import ch.netzwerg.paleo.ColumnIds.BooleanColumnId;
import ch.netzwerg.paleo.ColumnIds.IntColumnId;
import ch.netzwerg.paleo.ColumnIds.StringColumnId;
import ch.netzwerg.paleo.ColumnIds.TimestampColumnId;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVReaderHeaderAware;
import com.opencsv.CSVReaderHeaderAwareBuilder;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
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
        //System.setOut(out);

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

        final var ID_COMMUNITY_IDENTITY = IntColumnId.of("ID_CommunityIdentity");
        final var USER_COMMUNITY_NAME = StringColumnId.of("UserCommunityName");
        final var USER_GENDER = StringColumnId.of("UserGender");
        final var USER_CREATED_AT = TimestampColumnId.of("UserCreatedAt");
        final var ID_POSTING = IntColumnId.of("ID_Posting");
        final var VOTE_NEGATIVE = BooleanColumnId.of("VoteNegative");
        final var VOTE_POSITIVE = BooleanColumnId.of("VotePositive");
        final var VOTE_CREATED_AT = TimestampColumnId.of("VoteCreatedAt");

        var idCommunityIdentityColumn = IntColumn.ofAll(ID_COMMUNITY_IDENTITY, votings.get(1).stream().mapToInt(Integer::valueOf));
        var userCommunityNameColumn = StringColumn.ofAll(USER_COMMUNITY_NAME, votings.get(4));
        var userGenderColumn = StringColumn.ofAll(USER_GENDER, votings.get(3));
        var userCreatedAtColumn = TimestampColumn.ofAll(USER_CREATED_AT, votings.get(6).stream().map(Main::parseDate).collect(Collectors.toList()));
        var idPosting = IntColumn.ofAll(ID_POSTING, votings.get(7).stream().mapToInt(Integer::valueOf));
        var voteNegative = BooleanColumn.ofAll(VOTE_NEGATIVE, votings.get(0).stream().map(Boolean::valueOf).collect(Collectors.toList()));
        var votePositive = BooleanColumn.ofAll(VOTE_POSITIVE, votings.get(5).stream().map(Boolean::valueOf).collect(Collectors.toList()));
        var voteCreatedAt = TimestampColumn.ofAll(VOTE_CREATED_AT, votings.get(2).stream().map(Main::parseDate).collect(Collectors.toList()));


        // Grouping columns into a data frame
        DataFrame dataFrame = DataFrame.ofAll(idCommunityIdentityColumn, userCommunityNameColumn, userGenderColumn, userCreatedAtColumn, idPosting, voteNegative, votePositive, voteCreatedAt);

        //votings.forEach(System.out::println);
    }

    private static Instant parseDate(final String dateAsString) {
        Instant retVal = null;
        try {
            retVal = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").parse(dateAsString).toInstant();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return retVal;
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
