import xml.etree.ElementTree as ET
from loguru import logger
import requests 
import io

# ET.fromstring(country_data_as_string)
def parse_video_from_mpd_document(mpd_document_str, reddit_post_json_data):

    root = ET.fromstring(mpd_document_str)
    
    namespace = root.tag.split('}')[0].strip('{')

    for period in root.findall("Period", namespaces={"":namespace}):
        logger.info(f"Period {period.attrib['id']} with a duration of: {period.attrib['duration']}")

        # Getting each Adaptation set for the perod:
        for adaptation_set in period.findall("AdaptationSet", namespaces={"": namespace}):
            logger.info(f"Adaptation Set {adaptation_set.attrib['id']} is {adaptation_set.attrib['contentType']} for period {period.attrib['id']}")

            if adaptation_set.attrib['contentType'] == "video":

                # Mapping adaptaion set ids to bandwith values to find the highest resoloution video:
                adaptation_set_video_bandwith_dict = {}

                for representation in adaptation_set.findall("Representation", namespaces={"":namespace}):
                    adaptation_set_video_bandwith_dict[int(representation.attrib['bandwidth'])] = int(representation.attrib['id'])

                all_video_bandwidths = [bandwidth for bandwidth in adaptation_set_video_bandwith_dict.keys()]
                all_video_bandwidths.sort()
                largest_video_bandwidth = all_video_bandwidths[0]
                logger.info(f"Found a video for adaptation set {adaptation_set.attrib['id']} that has a bandwidth of {largest_video_bandwidth} in Representation {adaptation_set_video_bandwith_dict[largest_video_bandwidth]}")

                for representation in adaptation_set.findall("Representation", namespaces={"":namespace}):
                    if int(representation.attrib['id']) == adaptation_set_video_bandwith_dict[largest_video_bandwidth]:

                        # TODO: Implement that actual video extraction:
                        logger.info("Extracted video from largest bandwidth representation set")

                        reddit_post_base_url = reddit_post_json_data.get("url", None)
                        video_root_url = representation.find("BaseURL", namespaces={'':namespace})

                        if reddit_post_base_url is not None or video_root_url is not None:
                            video_url = f"{reddit_post_base_url}/{video_root_url}"
                            logger.info(f"Making request to get video from {video_url}")
                            video_response = requests.get(video_root_url)
                            video_response.raise_for_status()

                            video_stream = io.BytesIO(initial_bytes=video_response.content)
                            logger.info(f"Extracted video file from reddit with {len(video_response.content)} bytes")


            if adaptation_set.attrib['contentType'] =='audio':
                
                # Mapping adaptaion set ids to bandwith values to find the highest resoloution video:
                adaptation_set_audio_bandwith_dict = {}

                for representation in adaptation_set.findall("Representation", namespaces={"":namespace}):
                    adaptation_set_audio_bandwith_dict[int(representation.attrib['bandwidth'])] = int(representation.attrib['id'])

                all_audio_bandwidths = [bandwidth for bandwidth in adaptation_set_audio_bandwith_dict.keys()]
                all_audio_bandwidths.sort()
                largest_audio_bandwidth = all_audio_bandwidths[0]
                logger.info(f"Found audio for adaptation set {adaptation_set.attrib['id']} that has a bandwidth of {largest_audio_bandwidth} in Representation {adaptation_set_audio_bandwith_dict[largest_audio_bandwidth]}")
                
                for representation in adaptation_set.findall("Representation", namespaces={"":namespace}):
                    if int(representation.attrib['id']) == adaptation_set_audio_bandwith_dict[largest_audio_bandwidth]:

                        # TODO: Implement that actual audio extraction:
                        logger.info("Extracted audio from largest bandwidth representation set")
                        reddit_post_base_url = reddit_post_json_data.get("url", None)
                        audio_root_url = representation.find("BaseURL", namespaces={"":namespace})

                        if reddit_post_base_url is not None or audio_root_url is not None:
                            audio_full_url = f"{reddit_post_base_url}/{audio_root_url}"
                            logger.info(f"Making requests to get audio from {audio_full_url}")
                            audio_response = requests.get(audio_full_url)
                            audio_response.raise_for_status()
                            
                            audio_stream = io.BytesIO(initial_bytes=audio_response.content)
                            logger.info(f"Extracted audio file from reddit with {len(audio_response.content)} bytes")


if __name__ == "__main__":

    with open("/Users/matthewteelucksingh/Repos/java_webpage_content_extractor_POC/python_poc/reddit_post_ingestor/example_data/test.mpd", "r") as f:
        mpd_str = f.read()

        # TODO: Add a dict respponse type to store arrays of potential video/audio byte streams from multiple periods
        parse_video_from_mpd_document(mpd_document_str=mpd_str)