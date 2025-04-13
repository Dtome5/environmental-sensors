FROM python:3.12-slim
COPY pyproject.toml ./
RUN pip install uv
RUN uv pip install -r pyproject.toml --system
COPY *.py ./
COPY *.csv ./
CMD ["python","launch.py"]
