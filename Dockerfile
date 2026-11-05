FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .
COPY data ./data
RUN useradd --create-home app && chown -R app /app
USER app
CMD ["python", "-m", "lakehouse.job", "--landing", "data/landing", "--lakehouse", "build/lakehouse"]
