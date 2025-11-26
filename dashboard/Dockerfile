FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src /app
COPY output /app/output
COPY credentials.json /app/credentials.json
EXPOSE 8050
CMD ["python", "app.py"]