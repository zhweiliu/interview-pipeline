FROM python:3.13-slim

WORKDIR /app/online_retails

RUN pip install poetry

COPY poetry.lock pyproject.toml ./
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --only main --no-root

COPY online_retails/tasks ./tasks
COPY online_retails/flow.py .
COPY utilx ./utilx

ENV PREFECT_HOME=/app/.prefect

CMD ["poetry", "run", "python", "flow.py"]
