FROM python:3.12-slim
COPY pyproject.toml ./
RUN pip install uv
RUN uv pip install -r pyproject.toml --system
RUN uv pip install apache-airflow prefect polars --system
COPY *.py ./
COPY *.csv ./
CMD ["python","schedule.py"]
