package com.reddit.label.Parsers;

import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.reddit.label.Parsers.MPDUtils.DashPeriod;

public class MPDFileParser {

    Document xmlDocument;

    public MPDFileParser(Document xmlDocument) {
        this.xmlDocument = xmlDocument;
        xmlDocument.getDocumentElement().normalize();
    }

   public List<DashPeriod> getHighestResoloutionPeriods() {

        System.out.println("Starting MPD XML parsing...");

        List<DashPeriod> formattedDashPeriodList = new ArrayList<>();

        NodeList periodList = xmlDocument.getElementsByTagName("Period");
        System.out.printf("Extracted %d periods from MPD file.\n", periodList.getLength());

        for (int i = 0; i < periodList.getLength(); i++) {

            Node period = periodList.item(i);
            if (period == null) {
                System.out.println("No period found in period list. Exiting loop");
            } else {
                System.out.println("First Period is not none");
            }

            if (period.getNodeType() == Node.ELEMENT_NODE) {
                
                System.out.println("First period is the correct node type. Continuing parsing...");

                Element periodElement = (Element) period;
                DashPeriod formattedPeriod = new DashPeriod();

                //   <Period duration="PT16.433332443S" id="0">
                int periodId = Integer.valueOf(periodElement.getAttribute("id"));
                formattedPeriod.setPeriodId(periodId);
                System.out.printf("Extracted the Period id: %d\n", periodId);

                // <AdaptationSet contentType="video" id="0" maxHeight="720" maxWidth="980" > </AdaptationSet>
                NodeList adaptationSets = periodElement.getElementsByTagName("AdaptationSet");
                System.out.printf("In Period %d found %d AdaptationSets\n", periodId, adaptationSets.getLength());

                if (adaptationSets.getLength() == 0) {
                    return null;
                }

                System.out.println("Iterating through AdaptationSet...");
                String adaptationSetStaticType;
                for (int j = 0; j < adaptationSets.getLength(); j++) {

                    Node adaptationSet = adaptationSets.item(j);

                    if (adaptationSet.getNodeType() == Node.ELEMENT_NODE) {
                        System.out.println("AdaptationSet is the correct Node type");

                        Element adaptationSetElement = (Element) adaptationSet;
                        adaptationSetStaticType = adaptationSetElement.getAttribute("contentType");
                        System.out.printf("AdaptationSet content type found: %s \n", adaptationSetStaticType);

                        // Grabbing the highest resoloution video:
                        if (adaptationSetStaticType.contains("video")) {
                            System.out.println("Starting video extraction logic...");
                            NodeList RepresentationList = adaptationSetElement.getElementsByTagName("Representation");
                            
                            int representationWidth = 0;
                            int representationHeight = 0;
                            int representationIndex = 0;
                            
                            for (int k = 0; k < RepresentationList.getLength(); k++) {
                                
                                Node representation = RepresentationList.item(k);
                                Element representationElement = (Element)representation;

                                int currentRepresentationHeight = Integer.parseInt(representationElement.getAttribute("height"));
                                int currentRepresentationWidth = Integer.parseInt(representationElement.getAttribute("width"));

                                if ((currentRepresentationHeight > representationHeight) & (currentRepresentationWidth > representationWidth)) {
                                    representationWidth = currentRepresentationWidth;
                                    representationHeight = currentRepresentationHeight;
                                    representationIndex = k;
                                }

                            }

                            System.out.printf(
                                "Iterated through %d Representations and determined that the highest quality video is Height: %d, Weight: %d at index %d\n", 
                                RepresentationList.getLength(),
                                representationHeight,
                                representationWidth,
                                representationIndex
                            );

                            // Drilling in to the selected Representation element to get the Base Url:
                            Element highestResoloutionRepresentationElement = (Element)RepresentationList.item(representationIndex);
                            NodeList baseUrlElements = highestResoloutionRepresentationElement.getElementsByTagName("BaseURL");
                            String highestResoloutionUrl = ((Element) baseUrlElements.item(0)).getTextContent();

                            formattedPeriod.setVideoUrl(highestResoloutionUrl);                           
                            System.out.printf("Setting the video url to the Dash Period element: %s \n", highestResoloutionUrl);

                        } else if (adaptationSetStaticType.contains("audio")) {

                            System.out.println("Starting to process audio sections...");

                            NodeList RepresentationList = adaptationSetElement.getElementsByTagName("Representation");

                            int representationAudioBandwidth = 0;
                            int representationAudioIndex = 0;

                           for (int l=0; l < RepresentationList.getLength(); l++) {

                                Node representation = RepresentationList.item(l);
                                Element representationElement = (Element)representation;

                                int currentAudioBandwidth = Integer.parseInt(representationElement.getAttribute("bandwidth"));

                                if (currentAudioBandwidth > representationAudioBandwidth) {
                                    representationAudioBandwidth = currentAudioBandwidth;
                                    representationAudioIndex = l;
                                }

                            }

                            System.out.printf(
                                "Iterated through %d Audio Representations and determined that the highest quality audio stream has a bitrate of %d index %d\n", 
                                RepresentationList.getLength(),
                                representationAudioBandwidth,
                                periodId
                            );

                            // Drilling in to the selected Representation element to get the Base Url for audio content:
                            Element highestResoloutionRepresentaionAudioElement = (Element) RepresentationList.item(representationAudioIndex);
                            NodeList audioBaseUrlNode = highestResoloutionRepresentaionAudioElement.getElementsByTagName("BaseURL");
                            String hightestResoloutionAudioUrl = ((Element)audioBaseUrlNode.item(0)).getTextContent();
                            formattedPeriod.setAudioUrl(hightestResoloutionAudioUrl);
                            System.out.printf("Setting the Audio URL to the Dash Period %s \n", hightestResoloutionAudioUrl);
                        }

                    }

                }
                formattedDashPeriodList.add(formattedPeriod);
            }
        }

        return formattedDashPeriodList;
   }


}
