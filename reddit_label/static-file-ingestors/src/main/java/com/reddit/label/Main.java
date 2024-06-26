package com.reddit.label;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.reddit.label.BlobStorage.BlobStorageConfig;
import com.reddit.label.Databases.DB;
import com.reddit.label.Databases.SubredditTablesDB;
import com.reddit.label.Parsers.RedditJsonParser;
import com.reddit.label.StaticFileIngestors.RedditHostedVideoIngestor;
import com.reddit.label.SubredditIngestor.SubredditStaticContentIngestor;
import com.reddit.label.Databases.SubredditPost;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;

public class Main 
{
    public static void main( String[] args ) throws SQLException, InvalidKeyException, ErrorResponseException, InsufficientDataException, InternalException, InvalidResponseException, NoSuchAlgorithmException, ServerException, XmlParserException, IllegalArgumentException, IOException
    {


        /* 
         * Download logic:
         *  - Query the first 10 results where static file processed and static file type is unknown.
         *  - If json path does not exist, ingest the JSON static file content.
         *  - If screenshot does not exsist, ingest the screenshot.
         *  - Run the static file ingestion. 
         *  - Just keep doing this.
         */
        Connection conn = DB.connect();

        List<SubredditPost> postsToProcess = SubredditTablesDB.getPostsNoStatic(conn, 1);
        System.out.printf("Queried %d posts ready to ingest static \n", postsToProcess.size());

        MinioClient minioClient = MinioClient.builder()
            .endpoint(BlobStorageConfig.getMinioTestEndpoint())
            .credentials(BlobStorageConfig.getMinioTestUserId(), BlobStorageConfig.getMinioTestAccesskey())
            .build();

        for (SubredditPost post: postsToProcess) {
            
            System.out.println(post.getId());

            String jsonFileName;
            if (post.getJsonPostPath() == null) {
                System.out.printf("%s post has no json file. Downloading json file. \n", post.getId());
                jsonFileName = SubredditStaticContentIngestor.IngestJSONContent(conn, minioClient, post);
                System.out.printf("json file for %s post has been ingested as %s \n", post.getId(), jsonFileName);
            } else {
                jsonFileName = post.getJsonPostPath();
            }

            if (post.getScreenshotPath() == null) {
                System.out.printf("%s post has no screenshot. Taking screenshot\n", post.getId());
                String screenshotPath = SubredditStaticContentIngestor.IngestSnapshotImage(conn, minioClient, post);
                System.out.printf("Screenshot for %s post has been ingessted at %s \n", post.getId(), screenshotPath);
            }


            System.out.printf("Getting json post from database from post %s \n", post.getId());
            try (InputStream stream = minioClient.getObject(
                GetObjectArgs.builder()
                .bucket("reddit-posts")
                .object(jsonFileName)
                .build())) {

                // Reading the input Stream:
                StringBuilder content = new StringBuilder();
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
                    String line;
                    while((line = reader.readLine()) != null) {
                        content.append(line).append("\n");
                    }
                }

                System.out.printf("Beginning Json Parsing \n");
                RedditJsonParserResponse parsedJsonResponse = RedditJsonParser.parseDefaultRedditPostJson(content.toString());
                
                if (parsedJsonResponse.getInitalPostFields().getStaticFileType() == RedditContentPostType.HOSTED_VIDEO) {

                    System.out.printf("Post %s has a static file type of video:hosted. Beginning static file ingestion.", post.getId());

                    RedditHostedVideoIngestor hostedVideoIngestor = new RedditHostedVideoIngestor();
                    hostedVideoIngestor.fileToBlob(
                        post, 
                        parsedJsonResponse.getInitalPostFields(),
                        parsedJsonResponse.getFullPostJson(),
                        conn,
                        minioClient);

                        System.out.printf("Finished ingesting all data for post %s \n", post.getId());

                } else {
                    // Update here as I add parsers:
                    System.out.printf(
                        "Parsed JSON from post %s. The static file type was %s which is not currently supported. Setting static_file_type to unknown", 
                        parsedJsonResponse.initalPostFields.getStaticFileType()
                    );

                    int updatedStaticFileTypeResponse = SubredditTablesDB.updateStaticFileType(conn, post.getId(), "unknown");
                    if (updatedStaticFileTypeResponse != 1) {
                        System.out.printf("Error in updating static file type. Result integer returned is: %d \n", updatedStaticFileTypeResponse);
                    } else {
                        System.out.printf("Sucessfully set the static file type for post %s to unknown", post.getId());
                    }
                }
            }

        }

    }

}
