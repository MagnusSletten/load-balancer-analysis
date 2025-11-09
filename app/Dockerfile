FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir flask gunicorn
COPY server.py .
ENV PORT=8000
# Important: a long-running process so the container doesn't exit
CMD ["/bin/sh","-c","gunicorn -w ${GUNICORN_WORKERS:-1} --timeout ${GUNICORN_TIMEOUT:-180} -b 0.0.0.0:8000 server:app"]
