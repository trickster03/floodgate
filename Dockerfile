# Build a tiny static image for the ratelimitd service.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /ratelimitd ./cmd/ratelimitd

FROM scratch
COPY --from=build /ratelimitd /ratelimitd
EXPOSE 8080
ENTRYPOINT ["/ratelimitd"]
