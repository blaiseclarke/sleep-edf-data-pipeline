FROM python:3.10

# Create a non-root user
RUN useradd -m -u 1000 appuser

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Change ownership of the application directory to the non-root user
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

ENV PREFECT_LOGGING_LEVEL="INFO"

# Run pipeline once container launches
CMD ["python", "pipeline.py"]

