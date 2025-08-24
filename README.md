# PySpark-Kafka-Cassandra Bundle 🚀

A complete containerized data processing pipeline with PySpark, Apache Kafka, and Cassandra.

## 📋 Prerequisites

- Docker Desktop installed and running
- docker-compose installed
- 8GB+ RAM recommended
- Ports 4040, 8090, 9042, 9092, 2181 should be available

## 🚀 Quick Start

### 1. Download and Deploy
```bash
# Clone or download this bundle to any machine
git clone <your-repo-url>
cd pyspark

# Start the entire stack
./start.sh
```

### 2. Access Services
- **Spark UI**: http://localhost:4040
- **Kafka UI**: http://localhost:8090  
- **Cassandra**: localhost:9042

### 3. Stop Services
```bash
./stop.sh
```

## 📁 Project Structure

```
pyspark/
├── src/
│   ├── main/
│   │   ├── extraction/     # Data extraction logic
│   │   ├── transformation/ # Data transformation logic
│   │   ├── loader/         # Data loading logic
│   │   └── utilities/      # Utility functions
│   └── resources/
│       ├── input/          # Input data files
│       └── output/         # Output data files
├── docker-compose.yml      # Service orchestration
├── Dockerfile             # PySpark app container
├── requirements.txt       # Python dependencies
├── start.sh               # Deployment script
├── stop.sh                # Cleanup script
└── config.env             # Environment configuration
```

## 🔧 Configuration

### Environment Variables (config.env)
```bash
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
CASSANDRA_HOST=cassandra
SPARK_UI_PORT=4040
```

### Custom Configuration
Modify `docker-compose.yml` for:
- Resource allocation
- Port mappings  
- Volume mounts
- Environment variables

## 📊 Services Overview

### Apache Kafka
- **Port**: 9092 (external), 29092 (internal)
- **Zookeeper**: 2181
- **UI**: http://localhost:8090

### Apache Cassandra  
- **Port**: 9042
- **Cluster**: spark-cluster
- **Access**: `docker exec -it cassandra cqlsh`

### PySpark Application
- **Spark UI**: http://localhost:4040
- **Python**: 3.11 with OpenJDK 11
- **Dependencies**: Auto-installed from requirements.txt

## 🛠️ Development

### Local Development
```bash
# Run services without the app
docker-compose up zookeeper kafka cassandra -d

# Run your app locally
python -m src.main.main
```

### Adding Dependencies
1. Update `requirements.txt`
2. Rebuild: `docker-compose build pyspark-app`

### Viewing Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f pyspark-app
```

## 🔍 Monitoring & Debugging

### Spark UI Analysis
1. Run your pipeline: `./start.sh`
2. Open http://localhost:4040
3. Monitor Jobs, Stages, Storage, and Executors

### Kafka Monitoring
1. Open http://localhost:8090
2. View topics, messages, and consumer groups

### Cassandra Access
```bash
docker exec -it cassandra cqlsh
```

## 🚨 Troubleshooting

### Port Conflicts
```bash
# Check port usage
lsof -i :4040
lsof -i :9092

# Modify ports in docker-compose.yml if needed
```

### Memory Issues
```bash
# Increase Docker memory allocation
# Docker Desktop → Settings → Resources → Advanced
```

### Service Health
```bash
docker-compose ps
docker-compose logs <service-name>
```

## 📦 Deployment to Any Machine

### Option 1: Git Repository
```bash
git clone <your-repo>
cd pyspark
./start.sh
```

### Option 2: ZIP Bundle
1. Download and extract bundle
2. Run `./start.sh`

### Option 3: Docker Registry
```bash
# Push images to registry
docker-compose build
docker tag pyspark_pyspark-app your-registry/pyspark-app
docker push your-registry/pyspark-app

# Update docker-compose.yml with registry image
# Deploy on any machine
```

## 🧹 Cleanup

### Stop Services
```bash
./stop.sh
```

### Remove All Data
```bash
docker-compose down -v
```

### Complete Cleanup
```bash
docker-compose down --rmi all -v
docker system prune -f
```

## 🎯 Next Steps

1. **Add your data processing logic** in `src/main/transformation/`
2. **Configure Kafka topics** in your application
3. **Set up Cassandra keyspaces** and tables
4. **Customize the pipeline** for your use case

## 📞 Support

For issues or questions:
1. Check service logs: `docker-compose logs -f`
2. Verify port availability: `lsof -i :<port>`
3. Ensure Docker has sufficient resources
4. Review configuration in `docker-compose.yml`

---

**Happy Data Processing! 🎉**
