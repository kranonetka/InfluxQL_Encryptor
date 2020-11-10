FROM python:3.8

RUN mkdir service
WORKDIR service

RUN python -m pip install --upgrade pip && python -m pip install requests psutil

ADD main.py .

ENTRYPOINT python main.py