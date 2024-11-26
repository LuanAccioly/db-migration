FROM python:3.9-slim

WORKDIR /app

COPY app/ /app
# Certifique-se de que este caminho está correto
COPY ./requirements.txt /app/requirements.txt


RUN pip install --no-cache-dir -r requirements.txt


CMD ["python", "main.py"]

