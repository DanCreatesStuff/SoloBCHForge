# SoloBCH Forge - Stratum server container
# Stdlib-only Python; no pip install needed -> tiny, fast, arm64-friendly (Pi 5).
FROM python:3.13-slim

# Run as a non-root user for safety.
RUN useradd -r -u 10001 -m app

WORKDIR /app
COPY server/ /app/server/

ENV PYTHONUNBUFFERED=1

USER app

EXPOSE 3334 3335

# Health = the status service answering locally.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python3 -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:3335/status', timeout=3)" || exit 1

CMD ["python3", "/app/server/stratum.py"]
