package com.reddit.label.Parsers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.reddit.label.Parsers.MPDUtils.DashPeriod;

public class MPDFileParserTest {

    InputStream onePeriodMPDStream;

    @BeforeEach
    void setUp() throws IOException {

        onePeriodMPDStream = getClass().getClassLoader().getResourceAsStream("test-data/One_Period.mpd");
    }

    @Test
    void testGetHighestResoloutionPeriods() throws IOException, SAXException, ParserConfigurationException {

        String onePeriodMPDStreamString = new String(onePeriodMPDStream.readAllBytes(), StandardCharsets.UTF_8);
        InputSource inputSource = new InputSource(new StringReader(onePeriodMPDStreamString));

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);

        DocumentBuilder builder = factory.newDocumentBuilder();
        org.w3c.dom.Document doc = builder.parse(inputSource);

        MPDFileParser parser = new MPDFileParser(doc);
        List<DashPeriod> extractedPeriods =  parser.getRedditVideoMPDHighestResoloutionPeriods();
        System.out.println(extractedPeriods);

        DashPeriod singleExtractedPeriod = extractedPeriods.get(0);

        List<DashPeriod> expecedPeriodsOutputs = new ArrayList<DashPeriod>();
        DashPeriod expectedDashPeriod = new DashPeriod();
        expectedDashPeriod.setPeriodId(0);
        expectedDashPeriod.setVideoUrl("DASH_720.mp4");
        expectedDashPeriod.setAudioUrl("DASH_AUDIO_128.mp4");
        expecedPeriodsOutputs.add(expectedDashPeriod);

        assertEquals(expectedDashPeriod.getPeriodId(), singleExtractedPeriod.getPeriodId());
        assertEquals(expectedDashPeriod.getAudioUrl(), singleExtractedPeriod.getAudioUrl());
        assertEquals(expectedDashPeriod.getVideoUrl(), singleExtractedPeriod.getVideoUrl());

    }
}
