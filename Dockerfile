FROM jupyter/scipy-notebook:python-3.11.4

COPY ./requirements.txt /usr/bin/baskervillehall/requirements.txt
RUN pip install -r /usr/bin/baskervillehall/requirements.txt

COPY ./src /usr/bin/baskervillehall/src
COPY ./setup.py /usr/bin/baskervillehall/setup.py
COPY ./README.md /usr/bin/baskervillehall/README.md

WORKDIR /usr/bin/baskervillehall
RUN pip install tensorflow
USER root
RUN pip install -e .

