FROM python:3.10-slim

# Set to "true" to include Chromium + Xvfb for the Selenium-based store scraper.
ARG INSTALL_CHROME=false

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN if [ "$INSTALL_CHROME" = "true" ]; then \
        apt-get update && \
        apt-get install -y --no-install-recommends chromium chromium-driver xvfb && \
        rm -rf /var/lib/apt/lists/*; \
    fi

# Install dependencies first, so this layer is cached until requirements.txt changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
# Normalize line endings in case the repo was checked out on Windows
RUN sed -i 's/\r$//' run_process.sh && chmod +x run_process.sh

EXPOSE 5000

CMD ["gunicorn", "-w", "2", "-b", "0.0.0.0:5000", "--timeout", "120", "src.servers.webserver.server:server"]
