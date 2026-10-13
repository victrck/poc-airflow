FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir minio==7.2.0 httpx==0.27.2
COPY script.py /app/script.py
ENTRYPOINT ["python", "/app/script.py"]
