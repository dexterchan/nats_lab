ARG RUN_DIR="/var/task"
ARG RUN_USER="appuser"

FROM python:3.9.13-alpine3.16 as base_python
ARG RUN_DIR
ARG RUN_USER
USER root
WORKDIR $RUN_DIR
RUN mkdir -p ${RUN_DIR}
RUN addgroup  -g 9999  ${RUN_USER} 
RUN adduser -h $RUN_DIR -u 9999 -G ${RUN_USER} -D ${RUN_USER}
RUN  chown $RUN_USER:$RUN_USER  ${RUN_DIR}

COPY --chown=$RUN_USER:$RUN_USER src/async_work_stream ./async_work_stream
COPY --chown=$RUN_USER:$RUN_USER src/jetstreams ./jetstreams
COPY --chown=$RUN_USER:$RUN_USER src/pubsub ./pubsub
COPY --chown=$RUN_USER:$RUN_USER src/utility ./utility
COPY --chown=$RUN_USER:$RUN_USER requirements.txt .


FROM base_python as build_lib
ARG RUN_DIR
RUN apk add gcc musl-dev 
RUN python3 -m pip install -r requirements.txt  --target ${RUN_DIR}

FROM base_python as final
ARG RUN_DIR
ARG RUN_USER
USER ${RUN_USER}
COPY --from=build_lib --chown=$APPUSER:$APPUSER $RUN_DIR $RUN_DIR
ENV PYTHONPATH=$RUN_DIR

FROM final as test
ARG RUN_DIR
ARG RUN_USER
USER ${RUN_USER}
COPY --chown=$RUN_USER:$RUN_USER requirements_dev.txt .
COPY --chown=$RUN_USER:$RUN_USER tests tests
COPY --chown=$RUN_USER:$RUN_USER resources resources
RUN python3 -m pip install -r requirements_dev.txt --target ${RUN_DIR}

ENV PYTHONPATH=$RUN_DIR