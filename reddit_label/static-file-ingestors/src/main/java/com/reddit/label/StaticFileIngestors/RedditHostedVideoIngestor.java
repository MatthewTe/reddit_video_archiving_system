package com.reddit.label.StaticFileIngestors;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.xml.sax.InputSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.reddit.label.RedditContentPostType;
import com.reddit.label.RedditPostJsonDefaultAttributes;
import com.reddit.label.Parsers.MPDFileParser;
import com.reddit.label.Parsers.MPDUtils.DashPeriod;

import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;

import com.reddit.label.Databases.SubredditPost;
import com.reddit.label.Databases.SubredditTablesDB;

public class RedditHostedVideoIngestor implements StaticFileIngestor {

    public String fileToBlob(SubredditPost redditPost, RedditPostJsonDefaultAttributes defaultPostAttributes, JsonNode redditPostNode, Connection conn, MinioClient minioClient) throws MalformedURLException, IOException {

        if (defaultPostAttributes.getStaticFileType() != RedditContentPostType.HOSTED_VIDEO) {
            System.out.printf("Warining! Reddit Post Hosted Video Ingestor has been called on a post with a different static file type: %s. Exiting without ingesting static content", RedditContentPostType.HOSTED_VIDEO);
            return null;
        }

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
            
            System.out.printf("Read in the the MPD file content of length %d bytes \n %s\n", 
                stringBuilder.toString().length(),
                stringBuilder.toString()
            );

            byte[] mpdFileBytes = stringBuilder.toString().getBytes(StandardCharsets.UTF_8);
            InputStream contentStream = new ByteArrayInputStream(mpdFileBytes);
            long contentLength = mpdFileBytes.length;

            // MPD file goes to blob:
            String mpdFileName = String.format("%s/hosted_video.mpd", redditPost.getId());
            System.out.printf("File name for the created MPD file: %s", mpdFileName);

            try {
                ObjectWriteResponse mpdInsertionResponse = minioClient.putObject(
                    PutObjectArgs.builder().bucket("reddit-posts").object(mpdFileName)
                        .stream(contentStream, contentLength, -1)
                        .contentType("text/xml")
                        .build()
                    );

                    if (mpdInsertionResponse.toString() != null) {
                        System.out.printf("Sucessfully inserted MPD file %s into blob\n", mpdInsertionResponse.toString());
                    } else {
                        System.out.printf("Error in inserting MPD file into minio blob. Should have inserted %s \n", mpdInsertionResponse.toString());
                    }

            } catch (InvalidKeyException | ErrorResponseException | InsufficientDataException | InternalException
                    | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException
                    | IllegalArgumentException | IOException e) {
                System.out.println("Error in uploading the mpd file to blob storage");
                e.printStackTrace();
            } finally {
                inputStream.close();
            } 

            // Parses the XML of the mpd file to extract the url:
            InputSource inputSource = new InputSource(new StringReader(stringBuilder.toString()));

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            
            try {
                DocumentBuilder builder = factory.newDocumentBuilder();
                org.w3c.dom.Document doc = builder.parse(inputSource);

                MPDFileParser parser = new MPDFileParser(doc);
                List<DashPeriod> extractedPeriods =  parser.getRedditVideoMPDHighestResoloutionPeriods();

                System.out.printf("Parsed %d periods from the MPD file \n", extractedPeriods.size());

                for (DashPeriod period: extractedPeriods) {

                    System.out.printf(
                        "Ingesting static file data from period from %s. Video Url: %s and Audio Url: %s\n", 
                        period.getPeriodId(),
                        period.getVideoUrl(), 
                        period.getAudioUrl()
                    );

                    if (period.getVideoUrl() != null) {

                        String videoUrlString = String.format("%s/%s", defaultPostAttributes.getUrl(), period.getVideoUrl());
                        System.out.printf("Url for Video file found: %s", videoUrlString);

                        URI videoFileURL = new URI(videoUrlString);
                        
                        HttpURLConnection videoHttpConn = (HttpURLConnection) videoFileURL.toURL().openConnection();
                        videoHttpConn.setRequestMethod("GET");

                        int videoResponseCode = videoHttpConn.getResponseCode();
                        if (videoResponseCode == HttpURLConnection.HTTP_OK) {

                            InputStream videoInputStream = videoHttpConn.getInputStream();
                            ByteArrayOutputStream videoContentBufer = new ByteArrayOutputStream();

                            int nRead;
                            byte[] videoData = new byte[16384]; 

                            while ((nRead = videoInputStream.read(videoData, 0, videoData.length)) != -1) {
                                videoContentBufer.write(videoData, 0, nRead);
                            }

                            videoContentBufer.flush();

                            byte[] videoBytes = videoContentBufer.toByteArray();
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

                                if (videoInsertionResponse.toString() != null) {
                                    System.out.printf("Sucessfully inserted video static file %s into blob \n", videoInsertionResponse.toString());
                                } else {
                                    System.out.printf("Error in inserting video static file into blob. Should have been: %s", videoInsertionResponse.toString());
                                }   

                            } catch (InvalidKeyException | ErrorResponseException | InsufficientDataException | InternalException
                                    | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException
                                    | IllegalArgumentException | IOException e) {
                                System.out.println("Error in uploading video file to blob");
                                e.printStackTrace();
                            } finally {
                                videoInputStream.close();
                            }
                        }
                    } else {
                        System.out.printf("No vide url found for period %s", period.getPeriodId());
                    }

                    if (period.getAudioUrl() != null) {

                        String audioFileUrl = String.format("%s/%s",  defaultPostAttributes.getUrl(), period.getAudioUrl());
                        System.out.printf("Full url for Audio content found : %s\n", audioFileUrl);

                        URI audioFileURL = new URI(audioFileUrl);

                        HttpURLConnection audioHttpConn = (HttpURLConnection) audioFileURL.toURL().openConnection();
                        audioHttpConn.setRequestMethod("GET");

                        int audioResponseCode = audioHttpConn.getResponseCode();
                        if (audioResponseCode == HttpURLConnection.HTTP_OK) {

                            InputStream audioStringInputStream = audioHttpConn.getInputStream();
                            ByteArrayOutputStream audioBuffer = new ByteArrayOutputStream();

                            int nRead;
                            byte[] audioData = new byte[16384];

                            while ((nRead = audioStringInputStream.read(audioData, 0, audioData.length)) != -1) {
                                audioBuffer.write(audioData, 0, nRead);
                            }

                            byte[] audioBytes = audioBuffer.toByteArray();
                            InputStream audioStream = new ByteArrayInputStream(audioBytes);
                            long audioContentLength = audioBytes.length;


                            String audioFileName = String.format("%s/%s", redditPost.getId(), period.getAudioUrl());

                            try {

                                ObjectWriteResponse audioInsertionResponse = minioClient.putObject(
                                    PutObjectArgs.builder().bucket("reddit-posts").object(audioFileName)
                                    .stream(audioStream, audioContentLength, -1)
                                    .contentType("audio/mp4")
                                    .build()
                                );

                                if (audioInsertionResponse.toString() != null) {
                                    System.out.printf("Sucessfully inserted audio static file %s into blob\n", audioInsertionResponse.toString());
                                } else {
                                    System.out.printf("Error in inserting audio static file into blob. Should have been %s \n", audioInsertionResponse.toString());
                                }

                            } catch (InvalidKeyException | ErrorResponseException | InsufficientDataException | InternalException
                                    | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException
                                    | IllegalArgumentException | IOException e) {
                                System.out.println("Error in uploading audio file to blob");
                                e.printStackTrace();
                            } finally {
                                audioStream.close();
                            }
                        }

                    } else {
                        System.out.printf("No Audo url found for period %s", period.getPeriodId());
                    }

                }

                // Database update to indicate that static files have been ingested:
                redditPost.setStaticDownloaded(true);
                redditPost.setStaticRootPath(redditPost.getId() + "/");
                redditPost.setStaticFileType("hosted:video");

                int staticFieldUpdatedResult = SubredditTablesDB.updateStaticFields(conn, redditPost);

                if (staticFieldUpdatedResult == -1) {
                    System.out.printf("Error in updating the static variable fields in the database for post: %s \n", redditPost.getId());
                } else {
                    System.out.printf("Successfully updated static variable fields in the database for post: %s \n", redditPost.getId());
                }


            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        return null;
    }

}
