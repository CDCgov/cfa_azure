FROM python:3.10.17-slim-bullseye
RUN apt-get update -y --fix-missing && apt-get install git -y
WORKDIR /app
COPY client_config_states.toml /app
COPY multiple_states.py /app
COPY requirements.txt /app
COPY states.txt /app
COPY custom_metaflow /app/custom_metaflow
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

ENV USERNAME="YOUR_USER_NAME"
ENV PYTHONPATH="/app:/app/custom_metaflow"

CMD ["python", "multiple_states.py", "run"]
