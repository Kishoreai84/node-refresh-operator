FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY operator/ ./operator/
COPY config/ ./config/

RUN useradd -m -u 1000 operator
USER operator

CMD ["python", "-m", "operator.main"]