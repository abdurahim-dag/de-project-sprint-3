FROM python:3.10
WORKDIR .
COPY ./requirements.txt /tmp/pip-tmp/
RUN pip --disable-pip-version-check --no-cache-dir install -r /tmp/pip-tmp/requirements.txt \
    && rm -rf /tmp/pip-tmp