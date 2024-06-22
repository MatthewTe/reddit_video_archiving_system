package com.reddit.label.StaticFileIngestors;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.xml.sax.InputSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.reddit.label.RedditPostJsonDefaultAttributes;
import com.reddit.label.Parsers.MPDFileParser;
import com.reddit.label.Parsers.MPDUtils.DashPeriod;

import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;

import com.reddit.label.Databases.SubredditPost;

public class RedditHostedVideoIngestor implements StaticFileIngestor {

    public String fileToBlob(SubredditPost redditPost, RedditPostJsonDefaultAttributes defaultPostAttributes, JsonNode redditPostNode, Connection conn, MinioClient minioClient) throws MalformedURLException, IOException {

        ArrayNode rootArrayNode = (ArrayNode)redditPostNode;

        if (!rootArrayNode.isArray()) {
            System.out.println("redditPostNode is not an array. Exiting");
            return null;
        }

        JsonNode firstArrayItem = rootArrayNode.get(0);
        JsonNode mainAttributesNode = firstArrayItem.get("data").get("children").get(0).get("data");
        JsonNode dashUrl = mainAttributesNode.get("media").get("reddit_video").get("dash_url");

        URI mpdFileUrl;
        try {
            mpdFileUrl = new URI(dashUrl.asText());
        } catch (URISyntaxException e) {
            e.printStackTrace();
            System.out.println("Error in creating the URI from the parsed json.");
            return null;
        }

        HttpURLConnection httpConn = (HttpURLConnection) mpdFileUrl.toURL().openConnection();
        httpConn.setRequestMethod("GET");

        int responseCode = httpConn.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            InputStream inputStream = httpConn.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            StringBuilder stringBuilder = new StringBuilder();
            String line;

            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append(System.lineSeparator());
            }

            reader.close();
            inputStream.close();
            httpConn.disconnect();
            
            // MPD file goes to blob:
            String mpdFileName = String.format("%s/hosted_video.mpd", redditPost.getId());
            try {
                minioClient.putObject(
                    PutObjectArgs.builder().bucket("reddit-posts").object(mpdFileName)
                        .stream(inputStream, stringBuilder.toString().getBytes().length, -1)
                        .contentType("text/xml")
                        .build()
                    );
            } catch (InvalidKeyException | ErrorResponseException | InsufficientDataException | InternalException
                    | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException
                    | IllegalArgumentException | IOException e) {
                System.out.println("Error in uploading the mpd file to blob storage");
                e.printStackTrace();
            }

            // Parses the XML of the mpd file to extract the url:
            InputSource inputSource = new InputSource(new String(stringBuilder.toString()));

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            
            try {
                DocumentBuilder builder = factory.newDocumentBuilder();
                org.w3c.dom.Document doc = builder.parse(inputSource);

                MPDFileParser parser = new MPDFileParser(doc);
                List<DashPeriod> extractedPeriods =  parser.getRedditVideoMPDHighestResoloutionPeriods();

                for (DashPeriod period: extractedPeriods) {

                    if (period.getVideoUrl() != null) {
                        URI videoFileURL = new URI(defaultPostAttributes.getUrl() + period.getVideoUrl());
                        
                        HttpURLConnection videoHttpConn = (HttpURLConnection) videoFileURL.toURL().openConnection();
                        videoHttpConn.setRequestMethod("GET");

                        int videoResponseCode = videoHttpConn.getResponseCode();
                        if (videoResponseCode == HttpURLConnection.HTTP_OK) {
                            InputStream videoInputStream = videoHttpConn.getInputStream();

                            String videoFileName = String.format("%s/%s", redditPost.getId(), period.getVideoUrl());
                            try {

                                minioClient.putObject(
                                    PutObjectArgs.builder().bucket("reddit-posts").object(videoFileName)
                                    .stream(videoInputStream, -1, -1)
                                    .contentType("video/mp4")
                                    .build()
                                );

                            } catch (InvalidKeyException | ErrorResponseException | InsufficientDataException | InternalException
                                    | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException
                                    | IllegalArgumentException | IOException e) {
                                System.out.println("Error in uploading video file to blob");
                                e.printStackTrace();
                            } finally {
                                videoInputStream.close();
                            }
                    }

                    if (period.getAudioUrl() != null) {
                        URI audioFileURL = new URI(defaultPostAttributes.getUrl() + period.getAudioUrl());

                        HttpURLConnection audioHttpConn = (HttpURLConnection) audioFileURL.toURL().openConnection();
                        audioHttpConn.setRequestMethod("GET");

                        int audioResponseCode = audioHttpConn.getResponseCode();
                        if (audioResponseCode == HttpURLConnection.HTTP_OK) {
                            InputStream audioInputStream = audioHttpConn.getInputStream();

                            String audioFileName = String.format("%s/%s", redditPost.getId(), period.getAudioUrl());

                            try {

                                minioClient.putObject(
                                    PutObjectArgs.builder().bucket("reddit-posts").object(audioFileName)
                                    .stream(audioInputStream, -1, -1)
                                    .contentType("audio/mp4")
                                    .build()
                                );

                            } catch (InvalidKeyException | ErrorResponseException | InsufficientDataException | InternalException
                                    | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException
                                    | IllegalArgumentException | IOException e) {
                                System.out.println("Error in uploading audio file to blob");
                                e.printStackTrace();
                            } finally {
                                audioInputStream.close();
                            }
                        }

                    }

                }
            }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        return null;
    }

    public URI getFileUrl(JsonNode redditPostNode) {
        return null;
    }
}
