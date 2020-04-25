IMAGE_TAG=impulsogov/simulacovid

# Docker
docker-build:
	docker build -t $(IMAGE_TAG) .

docker-run:
	docker run -it --rm -p 8501:8501 $(IMAGE_TAG)

docker-build-run: docker-build docker-run

docker-shell:
	docker run --rm -it \
		--entrypoint "/bin/bash" \
		$(IMAGE_TAG)
