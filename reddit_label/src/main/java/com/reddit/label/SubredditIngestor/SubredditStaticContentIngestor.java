package com.reddit.label.SubredditIngestor;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
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

public class SubredditStaticContentIngestor {
    
    public static void IngestJSONContent(SubredditPost post) {

        // Take the post, grab the url and make an http request with the JSON url.
        // Get JSON and then ingest it into a blob storage account. 
        // Once it has been ingested update the record into the database.

        var client = HttpClient.newHttpClient();
        var request = HttpRequest.newBuilder(
            URI.create(post.getUrl() + ".json"))
            .GET()
            .header("accept", "application/json")
            .build();

        try {
            HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

            System.out.println(response.body());

            // Placeholder for modifying the json value in the db:
            String placeholderJsonpath = "AHHHHHHHHHH";
            int updatedRows = SubredditTablesDB.updateSubredditJSON(post.getId(), placeholderJsonpath);

            System.out.println(updatedRows);


        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }



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
