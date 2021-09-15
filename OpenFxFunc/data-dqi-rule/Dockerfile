# Argruments from FROM
ARG REGISTRY

# Get watcher - if watcher is uploaded on github, remove this line.
FROM ${REGISTRY}/fxwatcher:ubuntu-python3

ARG handler_file=handler.py
ARG handler_name=Handler

ENV HANDLER_DIR=/openfx/handler
ENV HANDLER_FILE=${HANDLER_DIR}/${handler_file}
ENV HANDLER_NAME=${handler_name}
ENV PYTHONUNBUFFERED=0

RUN mkdir -p ${HANDLER_DIR}
WORKDIR ${HANDLER_DIR}
COPY ./requirements.txt .
RUN pip3 install -r requirements.txt

COPY ./src/handler.py .
COPY ./src/DB/IRISDB.py ./DB/
COPY ./src/DB/M6/* ./DB/M6/
COPY ./src/RULE/* ./RULE/
RUN touch ${HANDLER_DIR}/__init__.py

RUN cp -r /openfx/executor/* ${HANDLER_DIR}

RUN export PYTHONIOENCODING=UTF-8

EXPOSE 50051
EXPOSE 50052

HEALTHCHECK --interval=5s CMD [ -e /tmp/.lock ] || exit 1

ENTRYPOINT ["python3"]
CMD ["server.py"]
