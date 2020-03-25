FROM nginx:stable-alpine

RUN apk --update add git bash make curl vim dumb-init ffmpeg htop redis postgresql

EXPOSE 1935

CMD ["nginx", "-g", "daemon off;"]
