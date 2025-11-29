.PHONY: build push deploy test clean

IMAGE_NAME = node-refresh-operator
TAG = latest
NAMESPACE = node-refresh-operator

build:
	docker build -t $(IMAGE_NAME):$(TAG) .

push: build
	# Push to your container registry
	# docker push $(IMAGE_NAME):$(TAG)

deploy:
	kubectl apply -f deploy/namespace.yaml
	kubectl apply -f deploy/crd.yaml
	kubectl apply -f deploy/rbac.yaml
	kubectl apply -f deploy/deployment.yaml

test:
	python -m pytest tests/ -v

clean:
	kubectl delete -f deploy/ || true
	kubectl delete crd noderefreshes.operations.example.com || true

all: build deploy