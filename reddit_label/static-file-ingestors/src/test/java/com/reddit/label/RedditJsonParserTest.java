package com.reddit.label;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.reddit.label.Parsers.RedditJsonParser;

public class RedditJsonParserTest {

    InputStream noPostJsonStream;
    InputStream videoPostJsonStream;

    @BeforeEach
    void setUp() throws IOException {

        noPostJsonStream = getClass().getClassLoader().getResourceAsStream("test-data/no_post_hint.json");
        videoPostJsonStream = getClass().getClassLoader().getResourceAsStream("test-data/video_post_hint.json");

    }

    @Test
    void testParseDefaultRedditPostJson() throws JsonProcessingException, IOException {

        String noPostHintJsonString = new String(noPostJsonStream.readAllBytes(), StandardCharsets.UTF_8);

        RedditJsonParserResponse noPostHintJsonResponse = RedditJsonParser.parseDefaultRedditPostJson(noPostHintJsonString);
        RedditPostJsonDefaultAttributes noPostHintJsonResponseFields = noPostHintJsonResponse.getInitalPostFields();

        assertEquals(noPostHintJsonResponseFields.getId(), "1cplv4s");
        assertEquals(noPostHintJsonResponseFields.getSubReddit(), "CombatFootage");
        assertEquals(noPostHintJsonResponseFields.getUrl(), "https://www.reddit.com/r/CombatFootage/comments/1cplv4s/ukraine_discussionquestion_thread_51024/");
        assertEquals(noPostHintJsonResponseFields.getTitle(), "Ukraine Discussion/Question Thread - 5/10/24+");
        assertEquals(noPostHintJsonResponseFields.getPostHint(), null);
        assertEquals(noPostHintJsonResponseFields.getStaticFileType(), RedditContentPostType.NO_STATIC);


        String videoPostJsonString = new String(videoPostJsonStream.readAllBytes(), StandardCharsets.UTF_8);
        RedditJsonParserResponse videoPostJsonResponse = RedditJsonParser.parseDefaultRedditPostJson(videoPostJsonString);
        RedditPostJsonDefaultAttributes videoPostJsonResponseFields = videoPostJsonResponse.getInitalPostFields();

        assertEquals(videoPostJsonResponseFields.getId(), "1didc5o");
        assertEquals(videoPostJsonResponseFields.getSubReddit(), "CombatFootage");
        assertEquals(videoPostJsonResponseFields.getUrl(), "https://v.redd.it/xvkx66c2487d1");
        assertEquals(videoPostJsonResponseFields.getTitle(), "Russian soldier standing up against a tree takes a direct FPV hit from the side");
        assertEquals(videoPostJsonResponseFields.getPostHint(), "hosted:video");
        assertEquals(videoPostJsonResponseFields.getStaticFileType(), RedditContentPostType.HOSTED_VIDEO);

    }
}
