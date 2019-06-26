FROM python:2.7-alpine
WORKDIR /app
COPY requirements.txt /app/
RUN pip install -r requirements.txt
COPY *.py /app/
COPY graph /app/graph
RUN mkdir -p local_uploads
CMD ["python", "app.py"]