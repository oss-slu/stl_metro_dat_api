# Docker file for the write service Python app
FROM python:3.13-slim

# Copy project files to Docker container
COPY src/write_service /write_service
COPY requirements.txt .

# Install required Python libraries and then start the app
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "/write_service/app.py"]