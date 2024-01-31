FROM python:3.9.1

RUN  pip install pandas==2.1.4 psycopg2-binary==2.9.9 pyarrow==14.0.2 fastparquet SQLAlchemy==2.0.25

RUN apt-get install wget

WORKDIR /app

COPY ingest_data.py ingest_data.py

ENTRYPOINT ["python", "ingest_data.py"]