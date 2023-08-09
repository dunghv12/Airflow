FROM python:3.9-slim
COPY scripts/requirements.txt /tmp/requirements.txt
RUN python -m pip install -r /tmp/requirements.txt
COPY scripts/ranking_movie.py /usr/local/bin/ranking_movie
RUN chmod +x /usr/local/bin/ranking_movie
