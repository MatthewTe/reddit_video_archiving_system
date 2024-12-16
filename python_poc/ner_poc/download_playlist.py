import argparse
from pytube import Playlist, YouTube

playlist: Playlist = Playlist('https://www.youtube.com/playlist?list=PLnArnDQHeUqdZkNhCycPth2yf0An-thnn')
print(playlist.title, playlist.playlist_id)

for video in playlist.videos:
    print(video.watch_url)