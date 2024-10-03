import typing
import requests
import xml.etree.ElementTree as ET

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


def extract_video_files_from_reddit_video_dict(reddit_video_info: RedditVideoInfoDict):

    # Get the MPD video file:
    response = requests.get(reddit_video_info['dash_url'])
    print(response.headers)
    print(response.content.decode())
    pass