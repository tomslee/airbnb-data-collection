FROM continuumio/miniconda3:4.5.4

COPY ./*.py collector/
COPY ./docker/requirements.txt collector/
COPY ./docker/*.config collector/

RUN apt-get update

RUN pip install --upgrade pip
RUN pip install -r collector/requirements.txt
