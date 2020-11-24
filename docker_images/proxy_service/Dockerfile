FROM python:3.8

RUN mkdir service
WORKDIR service

RUN python -m pip install --upgrade pip && python -m pip install pipenv

ADD . / ./
RUN pipenv install --system --verbose

CMD python main.py
