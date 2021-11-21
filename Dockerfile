FROM python:3.9.5-buster
WORKDIR /usr/src
COPY ./requirements.txt ./
RUN pip install -r requirements.txt
COPY ./ ./
