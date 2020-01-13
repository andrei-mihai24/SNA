import ch.netzwerg.paleo.DataFrame;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Optional;

public class DataProcessor {
    private static final String OUTPUT_PATH_TEST = "C:\\Users\\Andrei\\Desktop\\Data files 01.05.2019-02.05.2019 (40,8 MB)-20200112\\output\\";
    private static final String OUTPUT_PATH = "C:\\Users\\Andrei\\Desktop\\Data files 01.05.2019-02.05.2019 (40,8 MB)-20200112\\output\\";

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

    public void createCsv() throws IOException {
        var writer = new CSVWriter(new FileWriter(OUTPUT_PATH_TEST + "votes.csv"));
        for (int i = 0; i < votesDataFrame.getRowCount(); i++) {
            var csvOutputRow = new String[6];
            csvOutputRow[0] = String.valueOf(votesDataFrame.getValueAt(i, VotesDataFrame.ID_COMMUNITY_IDENTITY));
            csvOutputRow[1] = String.valueOf(votesDataFrame.getValueAt(i, VotesDataFrame.ID_POSTING));
            csvOutputRow[2] = votesDataFrame.getValueAt(i, VotesDataFrame.VOTE_CREATED_AT).toString();
            csvOutputRow[3] = votesDataFrame.getValueAt(i, VotesDataFrame.USER_COMMUNITY_NAME);
            csvOutputRow[4] = votesDataFrame.getValueAt(i, VotesDataFrame.USER_GENDER);
            csvOutputRow[5] = votesDataFrame.getValueAt(i, VotesDataFrame.USER_CREATED_AT).toString();

            writer.writeNext(csvOutputRow);
        }

        writer.close();

        writer = new CSVWriter(new FileWriter(OUTPUT_PATH_TEST + "postings.csv"));
        for (int i = 0; i < postingsDataFrame.getRowCount(); i++) {
            var csvOutputRow = new String[6];
            csvOutputRow[0] = String.valueOf(postingsDataFrame.getValueAt(i, PostingsDataFrame.ID_COMMUNITY_IDENTITY));
            csvOutputRow[1] = String.valueOf(postingsDataFrame.getValueAt(i, PostingsDataFrame.ID_POSTING));
            csvOutputRow[2] = postingsDataFrame.getValueAt(i, PostingsDataFrame.USER_COMMUNITY_NAME);
            csvOutputRow[3] = postingsDataFrame.getValueAt(i, PostingsDataFrame.USER_GENDER);
            csvOutputRow[4] = postingsDataFrame.getValueAt(i, PostingsDataFrame.POSTING_CREATED_AT).toString();
            csvOutputRow[5] = postingsDataFrame.getValueAt(i, PostingsDataFrame.USER_CREATED_AT).toString();

            writer.writeNext(csvOutputRow);
        }

        writer.close();
    }

    public void run() throws IOException {
        var postingIdToUserId = new HashMap<Integer, Integer>();
        var userIdToNumberOfPostings = new HashMap<Integer, Integer>();
        var userIdToUsername = new HashMap<Integer, String>();
        var userIdToUserCreatedAt = new HashMap<Integer, Instant>();
        var userIdToGender = new HashMap<Integer, String>();
        for (int i = 0; i < postingsDataFrame.getRowCount(); i++) {
            int userId = postingsDataFrame.getValueAt(i, PostingsDataFrame.ID_COMMUNITY_IDENTITY);
            postingIdToUserId.put(postingsDataFrame.getValueAt(i, PostingsDataFrame.ID_POSTING), userId);
            userIdToNumberOfPostings.merge(userId, 1, Integer::sum);
            userIdToUsername.putIfAbsent(userId, postingsDataFrame.getValueAt(i, PostingsDataFrame.USER_COMMUNITY_NAME));
            userIdToUserCreatedAt.putIfAbsent(userId, postingsDataFrame.getValueAt(i, PostingsDataFrame.USER_CREATED_AT));
            userIdToGender.putIfAbsent(userId, postingsDataFrame.getValueAt(i, PostingsDataFrame.USER_GENDER));
        }

        var userIdToUpvotesReceived = new HashMap<Integer, Integer>();
        var userIdToDownvotesReceived = new HashMap<Integer, Integer>();
        var userIdToUpvotesGiven = new HashMap<Integer, Integer>();
        var userIdToDownvotesGiven = new HashMap<Integer, Integer>();
        var lurkers_userIdToUsername = new HashMap<Integer, String>();
        var lurkers_userIdToUpvotes = new HashMap<Integer, Integer>();
        var lurkers_userIdToDownvotes = new HashMap<Integer, Integer>();
        var lurkers_userIdToGender = new HashMap<Integer, String>();
        var lurkers_userIdToUserCreatedAt = new HashMap<Integer, Instant>();
        for (int i = 0; i < votesDataFrame.getRowCount(); i++) {
            int idPosting = votesDataFrame.getValueAt(i, VotesDataFrame.ID_POSTING);
            Integer userId = postingIdToUserId.get(idPosting);
            assert userId != null;

            if (votesDataFrame.getValueAt(i, VotesDataFrame.VOTE_POSITIVE)) {
                userIdToUpvotesReceived.merge(userId, 1, Integer::sum);
                userIdToUpvotesGiven.merge(votesDataFrame.getValueAt(i, VotesDataFrame.ID_COMMUNITY_IDENTITY), 1, Integer::sum);
            }

            if (votesDataFrame.getValueAt(i, VotesDataFrame.VOTE_NEGATIVE)) {
                userIdToDownvotesReceived.merge(userId, 1, Integer::sum);
                userIdToDownvotesGiven.merge(votesDataFrame.getValueAt(i, VotesDataFrame.ID_COMMUNITY_IDENTITY), 1, Integer::sum);
            }

            int lurkerId = votesDataFrame.getValueAt(i, VotesDataFrame.ID_COMMUNITY_IDENTITY);
            if (!userIdToNumberOfPostings.containsKey(lurkerId)) {
                lurkers_userIdToUsername.put(lurkerId, votesDataFrame.getValueAt(i, VotesDataFrame.USER_COMMUNITY_NAME));
                lurkers_userIdToGender.putIfAbsent(lurkerId, votesDataFrame.getValueAt(i, VotesDataFrame.USER_GENDER));
                lurkers_userIdToUserCreatedAt.putIfAbsent(lurkerId, votesDataFrame.getValueAt(i, VotesDataFrame.USER_CREATED_AT));

                if (votesDataFrame.getValueAt(i, VotesDataFrame.VOTE_POSITIVE)) {
                    lurkers_userIdToUpvotes.merge(lurkerId, 1, Integer::sum);
                }

                if (votesDataFrame.getValueAt(i, VotesDataFrame.VOTE_NEGATIVE)) {
                    lurkers_userIdToDownvotes.merge(lurkerId, 1, Integer::sum);
                }
            }
        }

        var lurkersCsvWriter = new CSVWriter(new FileWriter(OUTPUT_PATH + "lurkers.csv"));
        lurkersCsvWriter.writeNext(new String[]{"ID_CommunityIdentity", "UserCommunityName", "UserGender", "Votes_positive", "Votes_negative", "UserCreatedAt"});
        lurkers_userIdToUsername.forEach((userId, username) -> {
            var csvOutputRow = new String[6];
            csvOutputRow[0] = userId.toString();
            csvOutputRow[1] = username;
            csvOutputRow[2] = lurkers_userIdToGender.get(userId);
            csvOutputRow[3] = Optional.ofNullable(lurkers_userIdToUpvotes.get(userId)).orElse(0).toString();
            csvOutputRow[4] = Optional.ofNullable(lurkers_userIdToDownvotes.get(userId)).orElse(0).toString();
            csvOutputRow[5] = lurkers_userIdToUserCreatedAt.get(userId).toString();

            lurkersCsvWriter.writeNext(csvOutputRow);
        });

        var nonLurkersCsvWriter = new CSVWriter(new FileWriter(OUTPUT_PATH + "non_lurkers.csv"));
        nonLurkersCsvWriter.writeNext(new String[]{"ID_CommunityIdentity", "UserCommunityName", "Number_of_postings", "Positive_votes_received", "Negative_votes_received", "Positive_votes_given", "Negative_votes_given", "UserCreatedAt", "Gender"});
        userIdToUsername.forEach((userId, username) -> {
            var csvOutputRow = new String[9];
            csvOutputRow[0] = userId.toString();
            csvOutputRow[1] = username;
            csvOutputRow[2] = userIdToNumberOfPostings.get(userId).toString();
            csvOutputRow[3] = Optional.ofNullable(userIdToUpvotesReceived.get(userId)).orElse(0).toString();
            csvOutputRow[4] = Optional.ofNullable(userIdToDownvotesReceived.get(userId)).orElse(0).toString();
            csvOutputRow[5] = Optional.ofNullable(userIdToUpvotesGiven.get(userId)).orElse(0).toString();
            csvOutputRow[6] = Optional.ofNullable(userIdToDownvotesGiven.get(userId)).orElse(0).toString();
            csvOutputRow[7] = userIdToUserCreatedAt.get(userId).toString();
            csvOutputRow[8] = userIdToGender.get(userId);

            nonLurkersCsvWriter.writeNext(csvOutputRow);
        });
    }
}
