FROM python:3.10-alpine as build

WORKDIR /app

COPY requirements.txt /app/
RUN apk add git && pip install -r ./requirements.txt

FROM python:3.10-alpine as run

WORKDIR /app
COPY --from=build /app /app
COPY --from=build /usr/local /usr/local
COPY fastagi.py /app/
RUN chmod +x /app/fastagi.py
