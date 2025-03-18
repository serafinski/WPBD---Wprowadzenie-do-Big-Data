FROM python:3.9-slim

WORKDIR /app

# Install required packages
RUN pip install requests

# Copy script and config
COPY create_connector.py /app/
COPY config/ /app/config/

CMD ["python", "create_connector.py"]