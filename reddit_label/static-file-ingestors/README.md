## How Static Files are ingested:

Get RedditPost from metadata table. -> For each post extract JSON description from bolb. Load as string.

Pass string into RedditJsonParser.parseDefaultRedditPostJson(String). This returns a RedditJsonParserResponse which contains some default attributes and the Json Tree if we need more fields from it.

Take the RedditJsonParserResponse and depending on what the Static file types are, run them through the static file parsers. 

    Eg if RedditJsonParserResponse copntains type HOSTED_VIDEO, run the output through the RedditHostedVideoIngestior.

   RedditHostedVideoIngestor - Extracts the mpd file, puts it into blob. Parses the mpd file for all of the video/audio periods and puts all of them into blob too. Then updates the database

[MPEG-DASH description](https://ottverse.com/structure-of-an-mpeg-dash-mpd/)


## Example of extracting static data from a specific reddit post:
```java
    public static void main( String[] args ) throws SQLException, InvalidKeyException, ErrorResponseException, InsufficientDataException, InternalException, InvalidResponseException, NoSuchAlgorithmException, ServerException, XmlParserException, IllegalArgumentException, IOException
    {

        try (Connection conn = DB.connect()) {
            
            SubredditPost examplePost = SubredditTablesDB.getPost(conn, "001abb4babd4c8b85fbb315a0244e9e6");
            System.out.println(examplePost);

            MinioClient testClient = MinioClient.builder()
                .endpoint("http://localhost:9000")
                .credentials("EvlShBk9qpieKWjI", "vNJ68dCpGvo9fwlGH9DtZXjsUbp8I1UC")
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
```

### I need to understand how streaming file buffer bytes work (Just copy and pasted this from GPT lol)
```Java
try {
    // Replace this with your actual HttpURLConnection
    HttpURLConnection videoHttpConn = (HttpURLConnection) new URL("http://your-video-url.com").openConnection();
    int videoResponseCode = videoHttpConn.getResponseCode();
    if (videoResponseCode == HttpURLConnection.HTTP_OK) {
        InputStream videoInputStream = videoHttpConn.getInputStream();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        int nRead;
        byte[] data = new byte[16384];

        while ((nRead = videoInputStream.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }

        buffer.flush();

        byte[] videoBytes = buffer.toByteArray();
        InputStream videoStream = new ByteArrayInputStream(videoBytes);
        long videoContentLength = videoBytes.length;

        String videoFileName = String.format("%s/%s", redditPost.getId(), period.getVideoUrl());
        try {
            ObjectWriteResponse videoInsertionResponse = minioClient.putObject(
                    PutObjectArgs.builder().bucket("reddit-posts").object(videoFileName)
                            .stream(videoStream, videoContentLength, -1)
                            .contentType("video/mp4")
                            .build()
            );

            System.out.println("File uploaded successfully. ETag: " + videoInsertionResponse.etag());
        } catch (Exception e) {
            e.printStackTrace();
        }
    } else {
        System.out.printf("Failed to get video. HTTP response code: %d\n", videoResponseCode);
    }
} catch (Exception e) {
    e.printStackTrace();
}
```
