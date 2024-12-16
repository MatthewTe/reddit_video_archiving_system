import typing
import io
import requests
import time
import random
import xml.etree.ElementTree as ET
from loguru import logger

class RedditVideoInfoDict(typing.TypedDict):
    bitrate_kbps: int
    fallback_url: str
    has_audio: bool
    height: int
    width: int
    scrubber_media_url: str
    dash_url: str
    duration: int
    hls_url: str
    is_gif: bool
    transcoding_status: str

'''
"reddit_video": {
    "bitrate_kbps": 2400,
    "fallback_url": "https://v.redd.it/s4yta4stlkrd1/DASH_720.mp4?source=fallback",
    "has_audio": true,
    "height": 720,
    "width": 1280,
    "scrubber_media_url": "https://v.redd.it/s4yta4stlkrd1/DASH_96.mp4",
    "dash_url": "https://v.redd.it/s4yta4stlkrd1/DASHPlaylist.mpd?a=1730157735%2CNTU1ZTkyOTBjYTY1NjIwMzZhZDg1MGI3MDJlZDQ0NjQwNDA0NzdhZjhmNWRiNmFjZDc4YzFhYjA1MGM0MDYyZA%3D%3D&amp;v=1&amp;f=sd",
    "duration": 17,
    "hls_url": "https://v.redd.it/s4yta4stlkrd1/HLSPlaylist.m3u8?a=1730157735%2CZjE5ODZhNzViMTFmMzY5NjQzY2UyMDMwYWZiOWYxMmE3NGJhNmYzMDc4NWQ0ODQ4MzhiNmEyZmU5ZjY2YjBlNQ%3D%3D&amp;v=1&amp;f=sd",
    "is_gif": false,
    "transcoding_status": "completed"
}
'''
class VideoMPDResult(typing.TypedDict):
    extension: str
    url: str
    mime_type: str
    video_byte_stream: io.BytesIO
    
class AudioMPDResult(typing.TypedDict):
    extension: str
    url: str
    mime_type: str
    audio_byte_stream: io.BytesIO

class ParsedMPDResult(typing.TypedDict):
    mpd_file: str
    videos_periods: dict[int, VideoMPDResult]
    audio_periods: dict[int, AudioMPDResult]
def parse_video_from_mpd_document(reddit_video_info: RedditVideoInfoDict, reddit_post_data: dict) -> ParsedMPDResult:

    response = requests.get(reddit_video_info['dash_url'])
    logger.info(f"Extracted mpd file from {reddit_video_info['dash_url']}")

    mpd_str: str = response.content.decode()

    root = ET.fromstring(mpd_str)
    
    namespace = root.tag.split('}')[0].strip('{')

    parsed_result: ParsedMPDResult = {
        "mpd_file": mpd_str,
        "audio_periods": {},
        "videos_periods": {}
    }

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

                        reddit_post_base_url = reddit_post_data.get("url", None)
                        video_root_url = representation.find("BaseURL", namespaces={'':namespace}).text

                        if reddit_post_base_url is not None or video_root_url is not None:
                            video_url = f"{reddit_post_base_url}/{video_root_url}"
                            logger.info(f"Making request to get video from {video_url}")


                            time.sleep(random.randint(1, 3))
                            video_response = requests.get(video_url)
                            video_response.raise_for_status()
                            time.sleep(random.randint(1, 3))

                            video_stream = io.BytesIO(initial_bytes=video_response.content)
                            logger.info(f"Extracted video file from reddit with {len(video_response.content)} bytes")
                            parsed_result['videos_periods'][int(period.attrib['id'])] = {
                                "mime_type": representation.attrib['mimeType'],
                                "extension": video_root_url,
                                "url":video_url,
                                "video_byte_stream":video_stream
                            }

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

                        logger.info("Extracted audio from largest bandwidth representation set")
                        reddit_post_base_url = reddit_post_data.get("url", None)
                        audio_root_url = representation.find("BaseURL", namespaces={"":namespace}).text

                        if reddit_post_base_url is not None or audio_root_url is not None:
                            audio_full_url = f"{reddit_post_base_url}/{audio_root_url}"
                            logger.info(f"Making requests to get audio from {audio_full_url}")
                            
                            time.sleep(random.randint(1, 3))
                            audio_response = requests.get(audio_full_url)
                            audio_response.raise_for_status()
                            time.sleep(random.randint(1, 3))

                            audio_stream = io.BytesIO(initial_bytes=audio_response.content)
                            logger.info(f"Extracted audio file from reddit with {len(audio_response.content)} bytes")
                            parsed_result['audio_periods'][int(period.attrib['id'])] = {
                                "mime_type": representation.attrib['mimeType'],
                                "extension": audio_root_url,
                                "url":audio_full_url,
                                "audio_byte_stream": audio_stream
                            }

    return parsed_result


