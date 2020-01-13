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

import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class VotesDataFrame {
    //private static final String PATH = "C:\\Users\\Andrei\\Desktop\\Data files 01.05.2019-02.05.2019 (40,8 MB)-20200112\\Votes_01052019_02052019.csv";
    private static final String PATH = "C:\\Users\\Andrei\\Desktop\\Data files 01.05.2019-31.05.2019 (660,6 MB)-20200112\\";

    public static final IntColumnId ID_COMMUNITY_IDENTITY = IntColumnId.of("ID_CommunityIdentity");
    public static final IntColumnId ID_POSTING = IntColumnId.of("ID_Posting");
    public static final BooleanColumnId VOTE_NEGATIVE = BooleanColumnId.of("VoteNegative");
    public static final BooleanColumnId VOTE_POSITIVE = BooleanColumnId.of("VotePositive");
    public static final TimestampColumnId VOTE_CREATED_AT = TimestampColumnId.of("VoteCreatedAt");
    public static final StringColumnId USER_COMMUNITY_NAME = StringColumnId.of("UserCommunityName");
    public static final StringColumnId USER_GENDER = StringColumnId.of("UserGender");
    public static final TimestampColumnId USER_CREATED_AT = TimestampColumnId.of("UserCreatedAt");

    private static VotesDataFrame instance = null;

    private DataFrame dataFrame;

    private VotesDataFrame() throws IOException, CsvValidationException {
        CSVReaderBuilder csvReaderBuilder = new CSVReaderHeaderAwareBuilder(new FileReader(PATH + "Votes_01052019_15052019.csv"))
                .withCSVParser(new CSVParserBuilder().withSeparator(';').build());
        CSVReaderHeaderAware csvReaderHeaderAware = ((CSVReaderHeaderAwareBuilder) csvReaderBuilder).build();

        Map<String, List<String>> data = new HashMap<>();
        Map<String, String> nextRow;
        while ((nextRow = csvReaderHeaderAware.readMap()) != null) {
            nextRow.forEach((k, v) -> data.computeIfAbsent(sanitize(k), i -> new ArrayList<>()).add(v));
        }

        csvReaderBuilder = new CSVReaderHeaderAwareBuilder(new FileReader(PATH + "Votes_16052019_31052019.csv"))
                .withCSVParser(new CSVParserBuilder().withSeparator(';').build());
        csvReaderHeaderAware = ((CSVReaderHeaderAwareBuilder) csvReaderBuilder).build();
        while ((nextRow = csvReaderHeaderAware.readMap()) != null) {
            nextRow.forEach((k, v) -> data.computeIfAbsent(sanitize(k), i -> new ArrayList<>()).add(v));
        }

        var idCommunityIdentityColumn = IntColumn.ofAll(VotesDataFrame.ID_COMMUNITY_IDENTITY, data.get(VotesDataFrame.ID_COMMUNITY_IDENTITY.getName()).stream().mapToInt(Integer::valueOf));
        var userCommunityNameColumn = StringColumn.ofAll(VotesDataFrame.USER_COMMUNITY_NAME, data.get(VotesDataFrame.USER_COMMUNITY_NAME.getName()));
        var userGenderColumn = StringColumn.ofAll(VotesDataFrame.USER_GENDER, data.get(VotesDataFrame.USER_GENDER.getName()));
        var userCreatedAtColumn = TimestampColumn.ofAll(VotesDataFrame.USER_CREATED_AT, data.get(VotesDataFrame.USER_CREATED_AT.getName()).stream().map(VotesDataFrame::parseDate).collect(Collectors.toList()));
        var idPosting = IntColumn.ofAll(VotesDataFrame.ID_POSTING, data.get(VotesDataFrame.ID_POSTING.getName()).stream().mapToInt(Integer::valueOf));
        var voteNegative = BooleanColumn.ofAll(VotesDataFrame.VOTE_NEGATIVE, data.get(VotesDataFrame.VOTE_NEGATIVE.getName()).stream().map(this::getFromString).collect(Collectors.toList()));
        var votePositive = BooleanColumn.ofAll(VotesDataFrame.VOTE_POSITIVE, data.get(VotesDataFrame.VOTE_POSITIVE.getName()).stream().map(this::getFromString).collect(Collectors.toList()));
        var voteCreatedAt = TimestampColumn.ofAll(VotesDataFrame.VOTE_CREATED_AT, data.get(VotesDataFrame.VOTE_CREATED_AT.getName()).stream().map(VotesDataFrame::parseDate).collect(Collectors.toList()));

        dataFrame = DataFrame.ofAll(idCommunityIdentityColumn, idPosting, voteNegative, votePositive, voteCreatedAt, userCommunityNameColumn, userGenderColumn, userCreatedAtColumn);
    }

    public static VotesDataFrame getInstance() throws IOException, CsvValidationException {
        if (instance == null) {
            instance = new VotesDataFrame();
        }

        return instance;
    }

    private Boolean getFromString(final String s) {
        Boolean retVal;

        if (s.equals("1")) {
            retVal = true;
        } else if (s.equals("0")) {
            retVal = false;
        } else {
            retVal = null;
        }

        return retVal;
    }

    public DataFrame getDataFrame() {
        return dataFrame;
    }

    private String sanitize(final String s) {
        return s.trim().replaceAll("[\uFEFF-\uFFFF]", "");
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
}
