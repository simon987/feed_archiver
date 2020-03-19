# Build
FROM golang:1.14 as go_build
WORKDIR /build/

COPY main.go .
COPY go.mod .
RUN GOOS=linux CGO_ENABLED=0 go build -a -installsuffix cgo -o feed_archiver .

FROM scratch

WORKDIR /root/
COPY --from=go_build ["/build/feed_archiver", "/root/"]

CMD ["/root/feed_archiver"]