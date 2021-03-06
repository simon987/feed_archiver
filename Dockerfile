# Build
FROM golang:1.15 as go_build
WORKDIR /build/

COPY *.go ./
COPY go.mod .
RUN GOOS=linux CGO_ENABLED=0 go build -a -installsuffix cgo -o feed_archiver .
RUN strip feed_archiver

FROM scratch

WORKDIR /root/
COPY --from=go_build ["/build/feed_archiver", "/root/"]

CMD ["/root/feed_archiver"]
