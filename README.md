# Relay

Go backend service that accepts an RTMP stream and proxies it out to multiple sources. Depends on ffmpeg and redis

# TODO:

- Document messages passed via redis to the relay
- Option to restrict outbound bitrate to conform to upstream provider limits
