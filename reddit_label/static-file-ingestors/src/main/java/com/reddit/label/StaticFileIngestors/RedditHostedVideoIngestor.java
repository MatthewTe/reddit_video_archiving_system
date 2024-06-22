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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.*;
import org.xml.sax.InputSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.reddit.label.RedditPostJsonDefaultAttributes;

import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;

public class RedditHostedVideoIngestor implements StaticFileIngestor {

    public String fileToBlob(RedditPostJsonDefaultAttributes defaultPostAttributes, JsonNode redditPostNode, Connection conn, MinioClient minioClient) throws MalformedURLException, IOException {

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
            String mpdFileName = String.format("%s/hosted_video.mpd", "test_output");
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

                doc.getDocumentElement().normalize();
                NodeList representations = doc.getElementsByTagNameNS("urn:mpeg:dash:schema:mpd:2011", "Representation");

                if (representations.getLength() > 0) {
                    // Get the first Representation element
                    Node representation = representations.item(0);

                    if (representation.getNodeType() == Node.ELEMENT_NODE) {
                        Element repElement = (Element) representation;

                        // Get the BaseURL element within the first Representation
                        NodeList baseURLs = repElement.getElementsByTagNameNS("urn:mpeg:dash:schema:mpd:2011", "BaseURL");

                        if (baseURLs.getLength() > 0) {
                            // Extract the text content of the BaseURL element
                            String baseURL = baseURLs.item(0).getTextContent();
                            System.out.println("BaseURL of the first Representation: " + baseURL);

                            // Constructing URL to the actual video:
                            //URI mp4Uri = new URI(defaultPostAttributes.getUrl() + "/" + baseURL);

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
