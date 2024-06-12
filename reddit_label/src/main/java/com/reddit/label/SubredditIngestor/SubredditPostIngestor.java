package com.reddit.label.SubredditIngestor;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.time.Duration;

import java.util.ArrayList;
import java.util.List;

import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

import com.reddit.label.Databases.SubredditPost;
import com.reddit.label.Databases.SubredditTablesDB;

public class SubredditPostIngestor {

    public static void RunSubredditIngestor(String subreddit, Boolean stopOnExisting) {

        // Create a selenium browser. Log in, start scrolling.
        // Iterate through all of the reddit posts. Extract all of the relevant fields. 
        // Query all ids from the database that we currently have ingested.
        // Find out which ids are unique. If none are perform exit check. Remove all non-uniuque ids
        // Ingest records into the database

        WebDriver driver = new ChromeDriver();
        driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(5));

        String subredditUrl = String.format("https://www.reddit.com/r/%s/new/", subreddit);
        driver.get(subredditUrl);

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

        while (true) {
            List<String> previouslyIngestedPosts = SubredditTablesDB.findAllIds();
            System.out.println(
                String.format("%d existing records found in the database", previouslyIngestedPosts.size())
            );

            List<SubredditPost> postToIngest = new ArrayList<SubredditPost>();

            List<WebElement> currentArticles = driver.findElements(By.tagName("article"));
            System.out.println(String.format("Successfully grabbed list of %x posts to ingest", currentArticles.size()));

            for (WebElement articleElement: currentArticles) {

                WebElement articlePost = articleElement.findElement(By.tagName("shreddit-post"));

                String relativePostUrl = articlePost.getAttribute("permalink");
                String redditPostUrl = "https://www.reddit.com" + relativePostUrl;

                try {

                    // Generating a unique id based on post url:
                    MessageDigest md = MessageDigest.getInstance("MD5");
                    byte[] messageDigest = md.digest(redditPostUrl.getBytes());

                    BigInteger no = new BigInteger(1, messageDigest);

                    String idHashText = no.toString(16);
                    while (idHashText.length() < 32) {
                        idHashText = "0" + idHashText;
                    }
                    
                    SubredditPost post = new SubredditPost(
                        idHashText, 
                        subreddit, 
                        redditPostUrl, 
                        false
                    );

                    if (previouslyIngestedPosts.contains(idHashText)) {
                        System.out.println(
                            String.format(
                                "%s (%s) post already in database. Not adding to list of posts to insert", 
                                idHashText,
                                redditPostUrl)
                            );
                    } else {
                        System.out.println(
                            String.format(
                                "%s (%s) post not found in the database. Adding the list to posts to insert into the db", 
                                idHashText,
                                redditPostUrl)
                            );

                            postToIngest.add(post);
                    }


                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            // Only continue to scroll and try to ingest if given the 'continue' flag:
            if (postToIngest.size() == 0) {
                System.out.println("No new articles found for individual posts for the pull.");

                if (stopOnExisting) {
                    System.out.println("stopOnExisting set to true. Exiting program now that postToIngest is empty");
                    return;

                } else {
                    System.out.println("stopOnExisting set to false, continuing to scroll and extract posts even though no uniuqe posts found");
                }
            } else {

                int numRowsInserted = SubredditTablesDB.InsertBasicSubredditPost(postToIngest);
                if (numRowsInserted == -1) {
                    System.err.println("Number of rows inserted into the database is -1. There was some error inserting records into the database");
                } else {
                    System.out.println(String.format("%d subreddit posts inserted into the database", numRowsInserted));
                }

            }

            // Scrolling down to grab new points:
            JavascriptExecutor js = (JavascriptExecutor) driver;
            js.executeScript("window.scrollTo(0, document.body.scrollHeight)");

            driver.manage().timeouts().implicitlyWait(Duration.ofMillis(2000));
            try {
                Thread.sleep(12000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }

    public static void RunSubredditIngestorOld(String subreddit, Boolean stopOnExisting) {

        WebDriver driver = new ChromeDriver();

        String subredditUrl = String.format("https://old.reddit.com/r/%s/new/", subreddit);

        driver.get(subredditUrl);

        driver.manage().timeouts().implicitlyWait(Duration.ofMillis(1000));

        Boolean isRunning = true;
        
        while (isRunning) {

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            List<WebElement> subredditPostElements = driver.findElements(By.className("thing"));

            List<String> existingPostIds = SubredditTablesDB.findAllIds();
            List<SubredditPost> newPostsToIngest = new ArrayList<SubredditPost>();

            for (WebElement post: subredditPostElements) {

                String relativePostUrl = post.getAttribute("data-permalink");

                if (relativePostUrl == null) {
                    System.out.println("Null detected when trying to extract relativePostUrl. Skipping...");
                    continue;
                }

                String fullPostUrl = "https://www.reddit.com" + relativePostUrl;

                try {

                    // Generating a unique id based on post url:
                    MessageDigest md = MessageDigest.getInstance("MD5");
                    byte[] messageDigest = md.digest(fullPostUrl.getBytes());

                    BigInteger no = new BigInteger(1, messageDigest);

                    String idHashText = no.toString(16);
                    while (idHashText.length() < 32) {
                        idHashText = "0" + idHashText;
                    }
                    
                    if (existingPostIds.contains(idHashText)) {
                        System.out.println(
                            String.format(
                                "%s (%s) post already in database. Not adding to list of posts to insert", 
                                idHashText,
                                fullPostUrl)
                            );
                    } else {
                        System.out.println(
                            String.format(
                                "%s (%s) post not found in the database. Adding the list to posts to insert into the db", 
                                idHashText,
                                fullPostUrl)
                            );
                            
                            SubredditPost newPost = new SubredditPost(
                                idHashText, 
                                subreddit, 
                                fullPostUrl, 
                                false
                            );

                            newPostsToIngest.add(newPost);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (newPostsToIngest.size() == 0) {
                System.out.println("No new articles found for individual posts for the pull.");

                if (stopOnExisting) {
                    System.out.println("stopOnExisting set to true. Exiting program now that postToIngest is empty");
                    isRunning = false;

                } else {
                    System.out.println("stopOnExisting set to false, continuing to extract posts even though no uniuqe posts found");
                }
            } else {
                System.out.println(String.format("%d new posts to insert into the database", newPostsToIngest.size()));
                
                int numRowsInserted = SubredditTablesDB.InsertBasicSubredditPost(newPostsToIngest);
                if (numRowsInserted == -1) {
                    System.err.println("Number of rows inserted into the database is -1. There was some error inserting records into the database");
                } else {
                    System.out.println(String.format("%d subreddit posts inserted into the database", numRowsInserted));
                }

            }

            WebElement nextButton;
            String nextUrl;
            try {
                nextButton = driver.findElement(By.className("next-button"));
                nextUrl = nextButton.findElement(By.tagName("a")).getAttribute("href");
            } catch (Exception e) {
                System.out.println("No next button found. Exiting...");
                return;
            }
           
            if (nextUrl == null) {
                System.out.println("No next url found - shutting down");
                return;
            }

            System.out.println(String.format("%s found as the url to the next page", nextUrl));

            driver.manage().timeouts().implicitlyWait(Duration.ofMillis(2000));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println(String.format("Going to new page %s", nextUrl));
            driver.get(nextUrl);
        }

        driver.close();

    }

    public static void RunSubredditIngestorOld(String subreddit, Boolean stopOnExisting, String overrideUrl) {

        WebDriver driver = new ChromeDriver();

        String subredditUrl = String.format(
            "https://old.reddit.com/r/%s/new/%s", 
            subreddit,
            overrideUrl
        );

        driver.get(subredditUrl);

        driver.manage().timeouts().implicitlyWait(Duration.ofMillis(1000));

        Boolean isRunning = true;
        
        while (isRunning) {

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            List<WebElement> subredditPostElements = driver.findElements(By.className("thing"));

            List<String> existingPostIds = SubredditTablesDB.findAllIds();
            List<SubredditPost> newPostsToIngest = new ArrayList<SubredditPost>();

            for (WebElement post: subredditPostElements) {

                String relativePostUrl = post.getAttribute("data-permalink");

                if (relativePostUrl == null) {
                    System.out.println("Null detected when trying to extract relativePostUrl. Skipping...");
                    continue;
                }

                String fullPostUrl = "https://www.reddit.com" + relativePostUrl;

                try {

                    // Generating a unique id based on post url:
                    MessageDigest md = MessageDigest.getInstance("MD5");
                    byte[] messageDigest = md.digest(fullPostUrl.getBytes());

                    BigInteger no = new BigInteger(1, messageDigest);

                    String idHashText = no.toString(16);
                    while (idHashText.length() < 32) {
                        idHashText = "0" + idHashText;
                    }
                    
                    if (existingPostIds.contains(idHashText)) {
                        System.out.println(
                            String.format(
                                "%s (%s) post already in database. Not adding to list of posts to insert", 
                                idHashText,
                                fullPostUrl)
                            );
                    } else {
                        System.out.println(
                            String.format(
                                "%s (%s) post not found in the database. Adding the list to posts to insert into the db", 
                                idHashText,
                                fullPostUrl)
                            );
                            
                            SubredditPost newPost = new SubredditPost(
                                idHashText, 
                                subreddit, 
                                fullPostUrl, 
                                false
                            );

                            newPostsToIngest.add(newPost);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (newPostsToIngest.size() == 0) {
                System.out.println("No new articles found for individual posts for the pull.");

                if (stopOnExisting) {
                    System.out.println("stopOnExisting set to true. Exiting program now that postToIngest is empty");
                    isRunning = false;

                } else {
                    System.out.println("stopOnExisting set to false, continuing to extract posts even though no uniuqe posts found");
                }
            } else {
                System.out.println(String.format("%d new posts to insert into the database", newPostsToIngest.size()));
                
                int numRowsInserted = SubredditTablesDB.InsertBasicSubredditPost(newPostsToIngest);
                if (numRowsInserted == -1) {
                    System.err.println("Number of rows inserted into the database is -1. There was some error inserting records into the database");
                } else {
                    System.out.println(String.format("%d subreddit posts inserted into the database", numRowsInserted));
                }

            }

            WebElement nextButton;
            String nextUrl;
            try {
                nextButton = driver.findElement(By.className("next-button"));
                nextUrl = nextButton.findElement(By.tagName("a")).getAttribute("href");
            } catch (Exception e) {
                System.out.println("No next button found. Exiting...");
                return;
            }
            
            if (nextUrl == null) {
                System.out.println("No next url found - shutting down");
                return;
            }

            System.out.println(String.format("%s found as the url to the next page", nextUrl));

            driver.manage().timeouts().implicitlyWait(Duration.ofMillis(2000));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println(String.format("Going to new page %s", nextUrl));
            driver.get(nextUrl);
        }

        driver.close();

 


    }
}
