FROM python:3.10.17-slim-bullseye
RUN apt-get update -y --fix-missing && apt-get install git vim -y
WORKDIR /app
COPY client_config.toml /app
COPY main.py /app
COPY requirements.txt /app
COPY custom_metaflow /app/custom_metaflow
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

ENV USERNAME="xop5"
ENV PYTHONPATH="/app:/app/custom_metaflow"

CMD ["python", "main.py", "run"]
