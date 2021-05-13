FROM golang:1.16-alpine AS builder

RUN apk --update add git bash make curl vim dumb-init ffmpeg htop redis postgresql

WORKDIR /go/src/github.com/suspiciousmilk/relay

COPY . .

RUN go get .
RUN go build -o relay *.go

RUN mv /go/src/github.com/suspiciousmilk/relay/relay /usr/local/relay

EXPOSE 1935

# STOPSIGNAL SIGTERM

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["relay"]
