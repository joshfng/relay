# Relay

Go backend service that accepts an RTMP stream and proxies it out to multiple destinations. For example, using OBS to stream to this replay, and have it stream to both YouTube and Twitch simultaneously.

This was created before there were any options available, but now there are many offerings both free and paid.

Depends on ffmpeg and redis

# TODO:

- Document messages passed via redis to the relay
- Option to restrict outbound bitrate to conform to upstream provider limits
