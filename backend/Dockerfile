FROM public.ecr.aws/lambda/python:3.12

# Install poetry
ENV POETRY_HOME=/opt/poetry
ENV POETRY_VERSION=2.1.4
RUN pip install poetry==${POETRY_VERSION}

# Install dependencies (no venv needed — Lambda base has one Python)
COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false && \
  poetry install --only main --no-root --no-interaction --no-ansi && \
  pip uninstall poetry -y

# Copy only the FastAPI app
COPY src/ ${LAMBDA_TASK_ROOT}/src/

# Lambda calls src.main.handler → Mangum(app)
CMD ["src.main.handler"]
