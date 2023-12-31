FROM python:slim

COPY ./requirements.txt /usr/local/baskervillehall/requirements.txt
RUN pip install -r /usr/local/baskervillehall/requirements.txt

COPY ./src /usr/local/baskervillehall/src
COPY ./setup.py /usr/local/baskervillehall/setup.py
COPY ./README.md /usr/local/baskervillehall/README.md

WORKDIR /usr/local/baskervillehall
RUN pip install -e .

