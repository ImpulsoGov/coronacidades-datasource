VERSION=1.0

LOADER_IMAGE_TAG=impulsogov/simulacovid:$(VERSION)-loader
SERVER_IMAGE_TAG=impulsogov/simulacovid:$(VERSION)-server

# Loader
loader-build:
	docker build \
		-f loader.dockerfile \
		-t $(LOADER_IMAGE_TAG) .

loader-run:
	docker rm -f datasource-loader 2>/dev/null || true
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

loader-create-env:
	virtualenv loader_venv
	source loader_venv/bin/activate; \
			pip3 install --upgrade -r requirements.txt; 

# Server
server-build:
	docker build \
		-f server.dockerfile \
		-t $(SERVER_IMAGE_TAG) .

server-run:
	docker rm -f datasource-server 2>/dev/null || true
	docker run -d --restart=unless-stopped \
		--name datasource-server \
		-p 7000:80 \
		-v "datasource:/output" \
		$(SERVER_IMAGE_TAG)

server-build-run: server-build server-run

server-shell:
	docker run --rm -it \
		--entrypoint "/bin/bash" \
		-p 80:80 \
		-v "datasource:/output" \
		$(SERVER_IMAGE_TAG)

