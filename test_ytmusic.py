from ytmusicapi import YTMusic

yt = YTMusic("browser.json")

print(yt.get_library_playlists(limit=5))
