#!/bin/bash

echo "🛑 Stopping PySpark-Kafka-Cassandra Bundle..."
echo "==============================================="

# Stop all services
echo "🔄 Stopping all services..."
docker-compose down

# Optional: Remove volumes (uncomment if you want to clear all data)
# echo "🧹 Removing volumes..."
# docker-compose down -v

# Optional: Remove images (uncomment if you want to clear all images)
# echo "🧹 Removing images..."
# docker-compose down --rmi all

echo "✅ All services stopped successfully!"
echo ""
echo "📝 Useful Commands:"
echo "  • Start again:         ./start.sh"
echo "  • Remove all data:     docker-compose down -v"
echo "  • Remove images:       docker-compose down --rmi all"
