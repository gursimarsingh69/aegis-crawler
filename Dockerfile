FROM python:3.11-slim

WORKDIR /app

# Install system deps for playwright
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN playwright install chromium --with-deps

COPY . .

CMD ["python", "main.py"]
