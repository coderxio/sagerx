FROM python:3.11-slim

# Install marimo and database dependencies
RUN pip install --no-cache-dir \
    marimo \
    psycopg2-binary \
    sqlalchemy \
    pandas \
    numpy \
    matplotlib \
    openai \
    anthropic \
    duckdb \
    sqlglot \
    altair \
    ruff

# Create marimo config directory
RUN mkdir -p /root/.marimo

# Copy the marimo configuration
COPY marimo.toml /root/.config/marimo/marimo.toml

WORKDIR /notebooks

CMD ["marimo", "edit", "--host", "0.0.0.0", "--port", "2718", "--no-token"]