.PHONY: build clean

build:
	mkdir -p dist
	env GOOS=linux GOARCH=amd64 go build -o dist/kafkaWriterForCentos ./cmd/kafkaWriter
	go build -o dist/kafkaWriter ./cmd/kafkaWriter

clean:
	rm -rf dist
