VERSION=1.0

LOADER_IMAGE_TAG=impulsogov/simulacovid:$(VERSION)-loader
SERVER_IMAGE_TAG=impulsogov/simulacovid:$(VERSION)-server

build-and-run-all: loader-build-run server-build-run



# Loader
loader-remove:
	docker rm -f datasource-loader 2>/dev/null || true

loader-build: loader-remove
	docker build \
		-f loader.dockerfile \
		-t $(LOADER_IMAGE_TAG) .

loader-run: loader-remove
	docker run -d --restart=unless-stopped \
		--name datasource-loader \
		-v "datasource:/output" \
		$(LOADER_IMAGE_TAG)

loader-build-run: loader-build loader-run

loader-shell: loader-build
	docker run --rm -it \
		--entrypoint "/bin/bash" \
		-v "datasource:/output" \
		$(LOADER_IMAGE_TAG)

loader-run-shell:
	docker run --rm -it \
		--entrypoint "/bin/bash" \
		-v "datasource:/output" \
		$(LOADER_IMAGE_TAG)

loader-create-env-analysis:
	virtualenv .loader-anaylsis
	source .loader-anaylsis/bin/activate; \
			pip3 install --upgrade -r requirements-analysis.txt; \
			python -m ipykernel install --user --name=loader-anaylsis

loader-dev: loader-build
	docker run --rm -it \
		--entrypoint "/bin/bash" \
		-v "$(PWD):/app/:ro" \
		-v "datasource:/output" \
		$(LOADER_IMAGE_TAG)

# Server
server-remove:
	docker rm -f datasource-server 2>/dev/null || true

server-build: server-remove
	docker build \
		-f server.dockerfile \
		-t $(SERVER_IMAGE_TAG) .

server-run: server-remove
	docker run -d --restart=unless-stopped \
		--name datasource-server \
		-p 80:80 \
		-v "datasource:/output" \
		$(SERVER_IMAGE_TAG)

server-build-run: server-build server-run

server-shell: server-build
	docker run --rm -it \
		--entrypoint "/bin/bash" \
		-p 7000:7000 \
		-v "datasource:/output" \
		$(SERVER_IMAGE_TAG)

server-dev: server-build
	docker run --rm -it \
		--entrypoint "/bin/bash" \
		-v "$(PWD):/app/:ro" \
		-v "datasource:/output" \
		$(SERVER_IMAGE_TAG)

