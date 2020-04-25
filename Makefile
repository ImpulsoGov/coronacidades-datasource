IMAGE_TAG=impulsogov/simulacovid

# Docker
server-build:
	docker build -t $(IMAGE_TAG) .

server-run:
	docker run -it --rm \
		-p 80:80 \
		-v "datasource:/uploads" \
		$(IMAGE_TAG)

server-build-run: server-build server-run

server-shell:
	docker run --rm -it \
		--entrypoint "/bin/bash" \
		-p 80:80 \
		-v "datasource:/uploads" \
		$(IMAGE_TAG)
