# Use official Python image with OpenJDK
FROM python:3.11-slim

# Install Java 11 (required for Spark)
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Create app directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application
COPY . .

# Create necessary directories
RUN mkdir -p /app/src/resources/output

# Set Python path
ENV PYTHONPATH=/app:$PYTHONPATH

# Expose Spark UI port
EXPOSE 4040

# Default command
CMD ["python", "-m", "src.main.main"]
