#!/bin/bash

echo "🔍 PySpark-Kafka-Cassandra Bundle Health Check"
echo "==============================================="

# Function to check if port is open
check_port() {
    local port=$1
    local service=$2
    if nc -z localhost $port 2>/dev/null; then
        echo "✅ $service (port $port) - Running"
        return 0
    else
        echo "❌ $service (port $port) - Not running"
        return 1
    fi
}

# Function to check Docker container status
check_container() {
    local container=$1
    local status=$(docker inspect -f '{{.State.Status}}' $container 2>/dev/null)
    if [ "$status" = "running" ]; then
        echo "✅ Container $container - Running"
        return 0
    else
        echo "❌ Container $container - Not running (Status: $status)"
        return 1
    fi
}

echo ""
echo "📊 Port Status:"
check_port 2181 "Zookeeper"
check_port 9092 "Kafka"
check_port 9042 "Cassandra"
check_port 4040 "Spark UI"
check_port 8090 "Kafka UI"

echo ""
echo "🐳 Container Status:"
check_container "zookeeper"
check_container "kafka"
check_container "cassandra"
check_container "pyspark-app"
check_container "kafka-ui"

echo ""
echo "📈 Docker Compose Status:"
docker-compose ps

echo ""
echo "🔗 Access URLs:"
echo "  • Spark UI:    http://localhost:4040"
echo "  • Kafka UI:    http://localhost:8090"
echo ""
echo "🛠️  Quick Commands:"
echo "  • View logs:       docker-compose logs -f [service]"
echo "  • Restart:         docker-compose restart [service]"
echo "  • Stop all:        ./stop.sh"
