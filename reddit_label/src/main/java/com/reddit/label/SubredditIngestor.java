package com.reddit.label;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;


public class SubredditIngestor {
    
    public static void CsvListBuilder(Path rootDir, String rootUrl) {

        // Create the csv if it doesn't exist:
        try {
            Files.createDirectories(rootDir);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Path trackingCsvFilePath = rootDir.resolve("subreddit_posts.csv");

        if (Files.exists(trackingCsvFilePath)) {
            try {
                Files.delete(trackingCsvFilePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            Files.createFile(trackingCsvFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Now we create the columns in the Csv:
        try (BufferedWriter writer = Files.newBufferedWriter(trackingCsvFilePath)) {

            writer.write("id,post_url,downloaded,local_screenshot,local_json,ingested,csv_inserted_date");
            writer.newLine();
            writer.close();

            // Creating the webdriver and populating the csv with data for each posts:
            WebDriver driver = new ChromeDriver();
            driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(5));

            driver.get(rootUrl);
            WebElement LoginButton = driver.findElement(By.id("login-button"));
            LoginButton.click();

            driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(1));

            String redditUsername = System.getenv("REDDIT_USERNAME");
            String redditPassword = System.getenv("REDDIT_PASSWORD");
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

            while (true) {
                List<WebElement> currentArticles = driver.findElements(By.tagName("article"));

                System.out.println(String.format("Successfully grabbed list of %x posts to ingest", currentArticles.size()));

                BufferedWriter rowWriter = Files.newBufferedWriter(trackingCsvFilePath, StandardOpenOption.APPEND);
                for (WebElement articleElement: currentArticles) {
                    WebElement articlePost = articleElement.findElement(By.tagName("shreddit-post"));

                    String relativePostUrl = articlePost.getAttribute("permalink");
                    String redditPostUrl = "https://www.reddit.com" + relativePostUrl;

                    try {

                        // Id generation: 
                        MessageDigest md = MessageDigest.getInstance("MD5");
                        byte[] messageDigest = md.digest(redditPostUrl.getBytes());

                        BigInteger no = new BigInteger(1, messageDigest);

                        String idHashText = no.toString(16);
                        while (idHashText.length() < 32) {
                            idHashText = "0" + idHashText;
                        }

                        String allCsvFieldsSingleString = String.format(
                            "%s,%s,%d,%s,%s,%d,%s",
                            idHashText, // Id
                            redditPostUrl, // Reddit Url
                            0, // Downloaded
                            "", // Local Screenshot
                            "", // Local Json
                            0, // Ingested
                            Instant.now().toString() // CSV Inserted Date
                        );

                        // Adding all of the fields to the csv:
                        rowWriter.write(allCsvFieldsSingleString);
                        rowWriter.newLine();
                        rowWriter.flush();

                        System.out.println(String.format("Successfully inserted %s post to csv w/ Id %s", rootUrl, idHashText));

                    } catch (Exception e) {
                        e.printStackTrace();
                    } 
                }

                rowWriter.close();               

                // Scrolling down to grab new points:
                JavascriptExecutor js = (JavascriptExecutor) driver;
                js.executeScript("window.scrollTo(0, document.body.scrollHeight)");

                driver.manage().timeouts().implicitlyWait(Duration.ofMillis(2000));
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }

        catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void LocalDirectoryBuilder(Path rootDir) {

    }

    public void LocalDirectoryUpploader(Path rootDir) {

    }
   

}
