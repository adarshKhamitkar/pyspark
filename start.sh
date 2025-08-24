#!/bin/bash

echo "🚀 Starting PySpark-Kafka-Cassandra Bundle..."
echo "==============================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose not found. Please install docker-compose."
    exit 1
fi

# Create logs directory
mkdir -p logs

# Build and start all services
echo "🔨 Building Docker images..."
docker-compose build

echo "🚀 Starting all services..."
docker-compose up -d

echo "⏳ Waiting for services to be ready..."
sleep 30

echo "✅ Services Status:"
docker-compose ps

echo ""
echo "📊 Access URLs:"
echo "  • Spark UI:    http://localhost:4040"
echo "  • Kafka UI:    http://localhost:8090"
echo "  • Cassandra:   localhost:9042"
echo ""
echo "📝 Useful Commands:"
echo "  • View logs:           docker-compose logs -f"
echo "  • Stop services:       docker-compose down"
echo "  • Restart PySpark:     docker-compose restart pyspark-app"
echo "  • Access Cassandra:    docker exec -it cassandra cqlsh"
echo ""
echo "🎉 Bundle is ready! Your PySpark application is running."
