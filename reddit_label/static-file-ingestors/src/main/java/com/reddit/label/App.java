package com.reddit.label;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;

import com.reddit.label.BlobStorage.BlobStorageConfig;
import com.reddit.label.Databases.DB;
import com.reddit.label.Databases.SubredditPost;
import com.reddit.label.Databases.SubredditTablesDB;
import com.reddit.label.Parsers.RedditJsonParser;
import com.reddit.label.StaticFileIngestors.RedditHostedVideoIngestor;
import com.reddit.label.SubredditIngestor.SubredditStaticContentIngestor;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;

public class App 
{
    public static void main( String[] args ) throws SQLException, InvalidKeyException, ErrorResponseException, InsufficientDataException, InternalException, InvalidResponseException, NoSuchAlgorithmException, ServerException, XmlParserException, IllegalArgumentException, IOException
    {

        try (Connection conn = DB.connect()) {
            
            SubredditPost examplePost = SubredditTablesDB.getPost(conn, "001abb4babd4c8b85fbb315a0244e9e6");
            System.out.println(examplePost);

            MinioClient testClient = MinioClient.builder()
                .endpoint(BlobStorageConfig.getMinioTestEndpoint())
                .credentials(BlobStorageConfig.getMinioTestUserId(), BlobStorageConfig.getMinioTestAccesskey())
                .build();

            String jsonPath = SubredditStaticContentIngestor.IngestJSONContent(conn, testClient, examplePost);
            String screenshotPath = SubredditStaticContentIngestor.IngestSnapshotImage(conn, testClient, examplePost);

            try (InputStream stream = testClient.getObject(
                GetObjectArgs.builder()
                .bucket("reddit-posts")
                .object(String.format("%s/post.json", examplePost.getId()))
                .build())) {

                // Reading the input Stream:
                StringBuilder content = new StringBuilder();
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
                    String line;
                    while((line = reader.readLine()) != null) {
                        content.append(line).append("\n");
                    }
                }


                RedditJsonParserResponse parsedJsonResponse = RedditJsonParser.parseDefaultRedditPostJson(content.toString());
                if (parsedJsonResponse.initalPostFields.getStaticFileType() == RedditContentPostType.HOSTED_VIDEO) {

                    RedditHostedVideoIngestor staticFileIngestor = new RedditHostedVideoIngestor();

                    staticFileIngestor.fileToBlob(
                        examplePost,
                        parsedJsonResponse.initalPostFields,
                        parsedJsonResponse.fullPostJson,
                        conn,
                        testClient
                    );

                } 

            }



        }

    }

}
