ARG PYTHON_VERSION=3.7.13
FROM python:$PYTHON_VERSION

USER root

WORKDIR /opt
RUN wget https://github.com/AdoptOpenJDK/openjdk8-binaries/releases/download/jdk8u292-b10_openj9-0.26.0/OpenJDK8U-jdk_x64_linux_openj9_8u292b10_openj9-0.26.0.tar.gz && \
    wget https://downloads.lightbend.com/scala/2.12.5/scala-2.12.5.tgz && \
    wget https://archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz
RUN tar xzf OpenJDK8U-jdk_x64_linux_openj9_8u292b10_openj9-0.26.0.tar.gz && \
    tar xvf scala-2.12.5.tgz && \
    tar xvf spark-2.4.5-bin-hadoop2.7.tgz
ENV PATH="/opt/jdk8u292-b10/bin:/opt/scala-2.12.5/bin:/opt/spark-2.4.5-bin-hadoop2.7/bin:$PATH"

RUN curl -sSL https://install.python-poetry.org | POETRY_VERSION=1.1.15 python3 -
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app
COPY ./pyproject.toml /app/pyproject.toml

RUN poetry config virtualenvs.in-project false
RUN poetry config experimental.new-installer false
RUN poetry install