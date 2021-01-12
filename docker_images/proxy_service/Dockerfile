FROM python:3.8

RUN mkdir service
WORKDIR service

RUN python -m pip install --upgrade pip && python -m pip install pipenv

ADD . / ./
RUN pipenv install --system

CMD gunicorn --workers `python get_workers_count.py` --bind 0.0.0.0:$FLASK_PORT --timeout 0 main:app
