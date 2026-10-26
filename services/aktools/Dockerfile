FROM python:3.13-slim
WORKDIR /service
COPY requirements.txt requirements.lock ./
RUN pip install --no-cache-dir -r requirements.txt && useradd --create-home sora
USER sora
EXPOSE 8080
CMD ["python", "-m", "aktools", "--host", "0.0.0.0", "--port", "8080"]
