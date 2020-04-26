VERSION=1.0

LOADER_IMAGE_TAG=impulsogov/simulacovid:$(VERSION)-loader
SERVER_IMAGE_TAG=impulsogov/simulacovid:$(VERSION)-server

# Loader
loader-build:
	docker build \
		-f loader.dockerfile \
		-t $(LOADER_IMAGE_TAG) .

loader-run:
	docker run -it --rm \
		--name datasource-loader \
		-v "datasource:/output" \
		$(LOADER_IMAGE_TAG)

loader-build-run: loader-build loader-run

loader-shell:
	docker run --rm -it \
		--entrypoint "/bin/bash" \
		-v "datasource:/output" \
		$(LOADER_IMAGE_TAG)


# Server
server-build:
	docker build \
		-f server.dockerfile \
		-t $(SERVER_IMAGE_TAG) .

server-run:
	docker run -it --rm \
		--name datasource-server \
		-p 80:80 \
		-v "datasource:/output" \
		$(SERVER_IMAGE_TAG)

server-build-run: server-build server-run

server-shell:
	docker run --rm -it \
		--entrypoint "/bin/bash" \
		-p 80:80 \
		-v "datasource:/output" \
		$(SERVER_IMAGE_TAG)

