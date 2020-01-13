import ch.netzwerg.paleo.ColumnIds.IntColumnId;
import ch.netzwerg.paleo.ColumnIds.LongColumnId;
import ch.netzwerg.paleo.ColumnIds.StringColumnId;
import ch.netzwerg.paleo.ColumnIds.TimestampColumnId;
import ch.netzwerg.paleo.*;
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

public class PostingsDataFrame {
    //private static final String PATH = "C:\\Users\\Andrei\\Desktop\\Data files 01.05.2019-02.05.2019 (40,8 MB)-20200112\\Postings_01052019_02052019.csv";
    private static final String PATH = "C:\\Users\\Andrei\\Desktop\\Data files 01.05.2019-31.05.2019 (660,6 MB)-20200112\\";

    public static final IntColumnId ID_POSTING = IntColumnId.of("ID_Posting");
    public static final StringColumnId ID_POSTING_PARENT = StringColumnId.of("ID_Posting_Parent");
    public static final IntColumnId ID_COMMUNITY_IDENTITY = IntColumnId.of("ID_CommunityIdentity");
    public static final StringColumnId POSTING_HEADLINE = StringColumnId.of("PostingHeadline");
    public static final StringColumnId POSTING_COMMENT = StringColumnId.of("PostingComment");
    public static final TimestampColumnId POSTING_CREATED_AT = TimestampColumnId.of("PostingCreatedAt");
    public static final LongColumnId ID_ARTICLE = LongColumnId.of("ID_Article");
    public static final TimestampColumnId ARTICLE_PUBLISHING_DATE = TimestampColumnId.of("ArticlePublishingDate");
    public static final StringColumnId ARTICLE_TITLE = StringColumnId.of("ArticleTitle");
    public static final StringColumnId ARTICLE_CHANNEL = StringColumnId.of("ArticleChannel");
    public static final StringColumnId ARTICLE_RESSORT_NAME = StringColumnId.of("ArticleRessortName");
    public static final StringColumnId USER_COMMUNITY_NAME = StringColumnId.of("UserCommunityName");
    public static final StringColumnId USER_GENDER = StringColumnId.of("UserGender");
    public static final TimestampColumnId USER_CREATED_AT = TimestampColumnId.of("UserCreatedAt");

    private static PostingsDataFrame instance = null;

    private DataFrame dataFrame;

    private PostingsDataFrame() throws IOException, CsvValidationException {
        CSVReaderBuilder csvReaderBuilder = new CSVReaderHeaderAwareBuilder(new FileReader(PATH + "Postings_01052019_15052019.csv"))
                .withCSVParser(new CSVParserBuilder().withSeparator(';').build());
        CSVReaderHeaderAware csvReaderHeaderAware = ((CSVReaderHeaderAwareBuilder) csvReaderBuilder).build();

        Map<String, List<String>> data = new HashMap<>();
        Map<String, String> nextRow;
        while ((nextRow = csvReaderHeaderAware.readMap()) != null) {
            nextRow.forEach((k, v) -> data.computeIfAbsent(sanitize(k), i -> new ArrayList<>()).add(v));
        }

        csvReaderBuilder = new CSVReaderHeaderAwareBuilder(new FileReader(PATH + "Postings_16052019_31052019.csv"))
                .withCSVParser(new CSVParserBuilder().withSeparator(';').build());
        csvReaderHeaderAware = ((CSVReaderHeaderAwareBuilder) csvReaderBuilder).build();
        while ((nextRow = csvReaderHeaderAware.readMap()) != null) {
            nextRow.forEach((k, v) -> data.computeIfAbsent(sanitize(k), i -> new ArrayList<>()).add(v));
        }

        var idPosting = IntColumn.ofAll(ID_POSTING, data.get(ID_POSTING.getName()).stream().mapToInt(Integer::valueOf));
        var idPostingParent = StringColumn.ofAll(ID_POSTING_PARENT, data.get(ID_POSTING_PARENT.getName()));
        var idCommunityIdentityColumn = IntColumn.ofAll(ID_COMMUNITY_IDENTITY, data.get(ID_COMMUNITY_IDENTITY.getName()).stream().mapToInt(Integer::valueOf));
        var postingHeadlineColumn = StringColumn.ofAll(POSTING_HEADLINE, data.get(POSTING_HEADLINE.getName()));
        var postingCommentColumn = StringColumn.ofAll(POSTING_COMMENT, data.get(POSTING_COMMENT.getName()));
        var postingCreatedAtColumn = TimestampColumn.ofAll(POSTING_CREATED_AT, data.get(POSTING_CREATED_AT.getName()).stream().map(PostingsDataFrame::parseDate).collect(Collectors.toList()));
        var idArticleColumn = LongColumn.ofAll(ID_ARTICLE, data.get(ID_ARTICLE.getName()).stream().mapToLong(Long::valueOf));
        var articlePublishingDateColumn = TimestampColumn.ofAll(ARTICLE_PUBLISHING_DATE, data.get(ARTICLE_PUBLISHING_DATE.getName()).stream().map(PostingsDataFrame::parseDate).collect(Collectors.toList()));
        var articleTitleColumn = StringColumn.ofAll(ARTICLE_TITLE, data.get(ARTICLE_TITLE.getName()));
        var articleChannelColumn = StringColumn.ofAll(ARTICLE_CHANNEL, data.get(ARTICLE_CHANNEL.getName()));
        var articleRessortNameColumn = StringColumn.ofAll(ARTICLE_RESSORT_NAME, data.get(ARTICLE_RESSORT_NAME.getName()));
        var userCommunityNameColumn = StringColumn.ofAll(USER_COMMUNITY_NAME, data.get(USER_COMMUNITY_NAME.getName()));
        var userGenderColumn = StringColumn.ofAll(USER_GENDER, data.get(USER_GENDER.getName()));
        var userCreatedAtColumn = TimestampColumn.ofAll(USER_CREATED_AT, data.get(USER_CREATED_AT.getName()).stream().map(PostingsDataFrame::parseDate).collect(Collectors.toList()));

        dataFrame = DataFrame.ofAll(
                idPosting,
                idPostingParent,
                idCommunityIdentityColumn,
                postingHeadlineColumn,
                postingCommentColumn,
                postingCreatedAtColumn,
                idArticleColumn,
                articlePublishingDateColumn,
                articleTitleColumn,
                articleChannelColumn,
                articleRessortNameColumn,
                userCommunityNameColumn,
                userGenderColumn,
                userCreatedAtColumn);
    }

    public static PostingsDataFrame getInstance() throws IOException, CsvValidationException {
        if (instance == null) {
            instance = new PostingsDataFrame();
        }

        return instance;
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
