FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
COPY . .
RUN go mod download
RUN CGO_ENABLED=0 go build -o /out/coordinator ./pc4/cmd/coordinator
RUN CGO_ENABLED=0 go build -o /out/node ./pc4/cmd/node

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /out/coordinator /app/coordinator
COPY --from=build /out/node /app/node
