package com.reddit.label.SubredditIngestor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.time.Duration;

import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

import com.reddit.label.Databases.SubredditPost;
import com.reddit.label.Databases.SubredditTablesDB;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;

public class SubredditStaticContentIngestor {
    
    public static String IngestJSONContent(Connection conn, MinioClient minioClient, SubredditPost post) throws InvalidKeyException, ErrorResponseException, InsufficientDataException, InternalException, InvalidResponseException, NoSuchAlgorithmException, ServerException, XmlParserException, IllegalArgumentException {

        var client = HttpClient.newHttpClient();
        var request = HttpRequest.newBuilder(
            URI.create(post.getUrl() + ".json"))
            .GET()
            .header("accept", "application/json")
            .build();

        try {
            HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

            boolean redditBucket = minioClient.bucketExists(BucketExistsArgs.builder().bucket("reddit-posts").build());

            if (!redditBucket) {
                MakeBucketArgs mbArgs = MakeBucketArgs.builder()
                    .bucket("reddit-posts")
                    .build();

                minioClient.makeBucket(mbArgs);
            } else {
                System.out.println("Bucket Reddit_Post exists");
            }

            String jsonFileName = String.format("%s/post.json", post.getId());

            byte[] jsonByteContent = response.body().getBytes(StandardCharsets.UTF_8);

            System.out.printf("Attempting to upload %s json file to blob", jsonFileName);
            
            try {
                ObjectWriteResponse jsonUploadResponse = minioClient.putObject(
                    PutObjectArgs.builder().bucket("reddit-posts").object(jsonFileName)
                        .stream(new ByteArrayInputStream(jsonByteContent), jsonByteContent.length, -1)
                        .contentType("application/json")
                        .build()
                );

                if (jsonUploadResponse.toString() == null) {
                    System.out.printf("Error in uploading %s json byte stream to blob\n", jsonFileName);
                    return null;
                } else {
                    System.out.printf("\nSuccessfully uploaded json file to blob: %s. Inserting record into db\n", jsonFileName);
                    int updatedRows = SubredditTablesDB.updateSubredditJSON(conn, post.getId(), jsonFileName);
                    if (updatedRows != 1) {
                        System.out.println(String.format("Error in inserting the subreddit JSON. Updated rows: %d", updatedRows));
                        return null;
                    } else {
                        return jsonFileName;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                return e.getMessage();
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        
        return null;

    }

    public static void IngestSnapshotImage(SubredditPost post) {

        WebDriver driver = new ChromeDriver();

        driver.get(post.getUrl());

        driver.manage().timeouts().implicitlyWait(Duration.ofMillis(3000));

        WebElement LoginButton = driver.findElement(By.id("login-button"));
        LoginButton.click();

        driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(1));

        String redditUsername = RedditConfig.getRedditUsername();
        String redditPassword = RedditConfig.getRedditPassword();
        WebElement loginUsernameInput = driver.findElement(By.id("login-username"));
        WebElement loginPasswordInput = driver.findElement(By.id("login-password"));

        driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(1));

        loginUsernameInput.sendKeys(redditUsername);
        loginPasswordInput.sendKeys(redditPassword);

        driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(1));

        loginPasswordInput.sendKeys(Keys.ENTER);

        driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(10));

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        String encodedScreenshotImg = ((TakesScreenshot)driver).getScreenshotAs(OutputType.BASE64);
        System.out.println(encodedScreenshotImg);

        driver.quit();


    }


}
