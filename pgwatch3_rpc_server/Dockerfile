FROM golang:1.22

WORKDIR /usr/src/app

# pre-copy/cache go.mod for pre-downloading dependencies and only redownloading them in subsequent builds if they change
COPY src/ .
RUN go mod download && go mod verify

RUN go build -o go-app .
EXPOSE 5050:5050
CMD ["./go-app", "--type=csv", "--port=5050"]
# CMD ["ls"]
