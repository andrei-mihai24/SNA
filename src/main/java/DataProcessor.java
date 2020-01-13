import ch.netzwerg.paleo.DataFrame;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;

public class DataProcessor {
    private static final String OUTPUT_PATH_LURKERS = "C:\\Users\\Andrei\\Desktop\\Data files 01.05.2019-02.05.2019 (40,8 MB)-20200112\\output\\lurkers.csv";
    private static final String OUTPUT_PATH_NON_LURKERS = "C:\\Users\\Andrei\\Desktop\\Data files 01.05.2019-02.05.2019 (40,8 MB)-20200112\\output\\non_lurkers.csv";

    private final DataFrame votesDataFrame;
    private final DataFrame postingsDataFrame;

    private static DataProcessor instance = null;

    private DataProcessor() throws IOException, CsvValidationException {
        votesDataFrame = VotesDataFrame.getInstance().getDataFrame();
        postingsDataFrame = PostingsDataFrame.getInstance().getDataFrame();
    }

    public static DataProcessor getInstance() throws IOException, CsvValidationException {
        if (instance == null) {
            instance = new DataProcessor();
        }

        return instance;
    }

    public void run() throws IOException {
        var postingIdToUserId = new HashMap<Integer, Integer>();
        var userIdToNumberOfPostings = new HashMap<Integer, Integer>();
        var userIdToUsername = new HashMap<Integer, String>();
        for (int i = 0; i < postingsDataFrame.getRowCount(); i++) {
            int userId = postingsDataFrame.getValueAt(i, PostingsDataFrame.ID_COMMUNITY_IDENTITY);
            postingIdToUserId.put(postingsDataFrame.getValueAt(i, PostingsDataFrame.ID_POSTING), userId);
            userIdToNumberOfPostings.merge(userId, 1, Integer::sum);
            userIdToUsername.putIfAbsent(userId, postingsDataFrame.getValueAt(i, PostingsDataFrame.USER_COMMUNITY_NAME));
        }

        var userIdToUpvotesReceived = new HashMap<Integer, Integer>();
        var userIdToDownvotesReceived = new HashMap<Integer, Integer>();
        var lurkers_userIdToUsername = new HashMap<Integer, String>();
        var lurkers_userIdToUpvotes = new HashMap<Integer, Integer>();
        var lurkers_userIdToDownvotes = new HashMap<Integer, Integer>();
        var lurkers_userIdToGender = new HashMap<Integer, String>();
        for (int i = 0; i < votesDataFrame.getRowCount(); i++) {
            int idPosting = votesDataFrame.getValueAt(i, VotesDataFrame.ID_POSTING);
            Integer userId = postingIdToUserId.get(idPosting);
            assert userId != null;

            if (votesDataFrame.getValueAt(i, VotesDataFrame.VOTE_POSITIVE)) {
                userIdToUpvotesReceived.merge(userId, 1, Integer::sum);
            }

            if (votesDataFrame.getValueAt(i, VotesDataFrame.VOTE_NEGATIVE)) {
                userIdToDownvotesReceived.merge(userId, 1, Integer::sum);
            }

            int lurkerId = votesDataFrame.getValueAt(i, VotesDataFrame.ID_COMMUNITY_IDENTITY);
            if (!userIdToNumberOfPostings.containsKey(lurkerId)) {
                lurkers_userIdToUsername.put(lurkerId, votesDataFrame.getValueAt(i, VotesDataFrame.USER_COMMUNITY_NAME));
                lurkers_userIdToGender.putIfAbsent(lurkerId, votesDataFrame.getValueAt(i, VotesDataFrame.USER_GENDER));

                if (votesDataFrame.getValueAt(i, VotesDataFrame.VOTE_POSITIVE)) {
                    lurkers_userIdToUpvotes.merge(lurkerId, 1, Integer::sum);
                }

                if (votesDataFrame.getValueAt(i, VotesDataFrame.VOTE_NEGATIVE)) {
                    lurkers_userIdToDownvotes.merge(lurkerId, 1, Integer::sum);
                }
            }
        }

        var lurkersCsvWriter = new CSVWriter(new FileWriter(OUTPUT_PATH_LURKERS));
        lurkersCsvWriter.writeNext(new String[]{"ID_CommunityIdentity", "UserCommunityName", "UserGender", "Votes_positive", "Votes_negative"});
        lurkers_userIdToUsername.forEach((userId, username) -> {
            var csvOutputRow = new String[5];
            csvOutputRow[0] = userId.toString();
            csvOutputRow[1] = username;
            csvOutputRow[2] = lurkers_userIdToGender.get(userId);
            csvOutputRow[3] = Optional.ofNullable(lurkers_userIdToUpvotes.get(userId)).orElse(0).toString();
            csvOutputRow[4] = Optional.ofNullable(lurkers_userIdToDownvotes.get(userId)).orElse(0).toString();

            lurkersCsvWriter.writeNext(csvOutputRow);
        });

        var nonLurkersCsvWriter = new CSVWriter(new FileWriter(OUTPUT_PATH_NON_LURKERS));
        nonLurkersCsvWriter.writeNext(new String[]{"ID_CommunityIdentity", "UserCommunityName", "Number_of_postings", "Positive_votes_received", "Negative_votes_received"});
        userIdToUsername.forEach((userId, username) -> {
            var csvOutputRow = new String[5];
            csvOutputRow[0] = userId.toString();
            csvOutputRow[1] = username;
            csvOutputRow[2] = userIdToNumberOfPostings.get(userId).toString();
            csvOutputRow[3] = Optional.ofNullable(userIdToUpvotesReceived.get(userId)).orElse(0).toString();
            csvOutputRow[4] = Optional.ofNullable(userIdToDownvotesReceived.get(userId)).orElse(0).toString();

            nonLurkersCsvWriter.writeNext(csvOutputRow);
        });
    }
}
