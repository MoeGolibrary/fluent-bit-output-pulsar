AWS_ECR_URL=693727988157.dkr.ecr.us-west-2.amazonaws.com
PULSAR_LIB_PATH=/usr/lib/libpulsar.so.2.10.2
PLUGIN_NAME=fluent-bit-output-pulsar
PLUGIN_VERSION=1.1.0
PLUGIN_IMAGE_NAME=${PLUGIN_NAME}:${PLUGIN_VERSION}
PLUGIN_REMOTE_IMAGE=${AWS_ECR_URL}/fluent-bit:${PLUGIN_VERSION}

.PHONY: clean compile build push

push:
	@echo "docker tag ${PLUGIN_REMOTE_IMAGE}"
	@docker tag ${PLUGIN_IMAGE_NAME} ${PLUGIN_REMOTE_IMAGE}
	@echo "docker push ${PLUGIN_REMOTE_IMAGE}"
	@docker push ${PLUGIN_REMOTE_IMAGE}

build: compile
	@echo "Start to build image ${PLUGIN_IMAGE_NAME} ..."
	@docker build -t ${PLUGIN_IMAGE_NAME} .
	@echo "Built image: ${PLUGIN_IMAGE_NAME}"

compile: clean
	@echo "Start to compile pulsar output plugin ..."
	@mkdir build
	@cd build && cmake -DPULSAR_CPP_HEADERS=${PULSAR_CPP_HEADERS} -DFLB_SOURCE=${FLB_SOURCE} -DPLUGIN_NAME=out_pulsar .. && make && cp ${PULSAR_LIB_PATH} ./
	@echo "Compile pulsar output plugin done"

clean:
	@rm -rf build
	@echo "Clean the build done"
