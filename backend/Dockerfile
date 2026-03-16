FROM python:3.13-slim AS builder
WORKDIR /app

# RUN apt-get update && apt-get install -y \
#   curl --no-install-recommends \
#   && rm -rf /var/lib/apt/lists/*

RUN apt-get update \
  && apt-get install -y \
  curl \
  build-essential \
  libffi-dev \
  && rm -rf /var/lib/apt/lists/*

ENV POETRY_HOME=/opt/poetry
ENV POETRY_VERSION=2.1.4
RUN curl -sSL https://install.python-poetry.org | python3 -

ENV PATH="${POETRY_HOME}/bin:$PATH"
RUN python -m venv ./venv

COPY pyproject.toml poetry.lock ./

RUN . /app/venv/bin/activate && poetry install --only main --no-root


FROM python:3.13-slim AS runner
WORKDIR /app

COPY --from=builder /app/venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

COPY . .

# HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
#   CMD curl -f http://localhost:8000/lab/health || exit 1

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
