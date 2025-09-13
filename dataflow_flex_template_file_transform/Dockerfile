FROM gcr.io/dataflow-templates-base/python39-template-launcher-base

WORKDIR /app

COPY . /app/

# ✅ Install Python dependencies (this is the missing part!)
RUN pip install --no-cache-dir -r requirements.txt


ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/app/main.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/app/requirements.txt"

