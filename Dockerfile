FROM python:3.10

WORKDIR /app

COPY requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PREFECT_LOGGING_LEVEL="INFO"

# Run pipeline once container launches
CMD ["python", "pipeline.py"]

