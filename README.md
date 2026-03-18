# Worker Safety Gear Detection

A standalone application for detecting worker safety gear (hard hats and safety vests) using Intel's DLStreamer Pipeline Server.

## Overview

This application detects whether workers are wearing required safety gear including:
- Hard hats
- Safety vests

It uses deep learning models optimized for edge deployment on Intel hardware.

## Prerequisites

- Docker and Docker Compose
- NVIDIA/Intel GPU support (optional, but recommended)
- At least 8GB RAM
- Linux-based system

## Quick Start

1. **Clone the repository**
   ```bash
   git clone <your-new-repo-url>
   cd worker-safety-gear-detection
   ```

2. **Configure environment variables**
   ```bash
   # Edit .env file with your settings
   nano .env
   ```
   Key variables to set:
   - `HOST_IP`: Your machine's IP address
   - `DOCKER_REGISTRY`: Docker registry if using private images
   - `REST_SERVER_PORT`: Port for REST API (default: 8080)

3. **Download artifacts (models and videos)**
   ```bash
   bash apps/worker-safety-gear-detection/setup.sh
   ```

4. **Start the application**
   ```bash
   docker-compose up -d
   ```

5. **Access the application**
   - REST API: `http://<HOST_IP>:8080`
   - WebRTC Stream: `http://<HOST_IP>` (via nginx)

## Architecture

- **DLStreamer Pipeline Server**: Core inference engine
- **MediaMTX**: WebRTC streaming server
- **MQTT Broker**: Message broker for events
- **Nginx**: Reverse proxy and static content serving
- **Model Registry**: Manages ML models and versions
- **MinIO**: S3-compatible object storage for models

## Configuration Files

- `.env`: Environment variables
- `apps/worker-safety-gear-detection/configs/`: Application-specific configs
  - `pipeline-server-config.json`: Pipeline definitions
  - `mosquitto.conf`: MQTT broker configuration
  - `nginx/nginx.conf`: Web server configuration

## Deployment

### Docker Compose (Development)
```bash
docker-compose up -d
```

### Kubernetes/Helm (Production)
```bash
helm install worker-safety helm/ -f helm/values_worker_safety_gear_detection.yaml
```

## API Usage

### List Available Pipelines
```bash
curl -X GET http://localhost:8080/pipelines
```

### Start a Pipeline
```bash
curl -X POST http://localhost:8080/pipelines/worker_safety_gear_detection/start \
  -H "Content-Type: application/json" \
  -d @apps/worker-safety-gear-detection/payload.json
```

### Stop a Pipeline
```bash
curl -X POST http://localhost:8080/pipelines/worker_safety_gear_detection/stop
```

## Logs and Monitoring

View logs:
```bash
docker-compose logs -f dlstreamer-pipeline-server
```

Access Prometheus metrics:
- `http://<HOST_IP>:9999`

## Troubleshooting

### Models not loading
- Ensure artifacts are downloaded: `bash apps/worker-safety-gear-detection/setup.sh`
- Check the `resources/` directory structure

### Connection issues
- Verify `HOST_IP` in `.env` matches your machine's IP
- Check firewall rules for required ports

### Performance issues
- Enable GPU: Set device to "GPU" in pipeline parameters
- Check available resources: `docker stats`

## Documentation

- [Application README](apps/worker-safety-gear-detection/README.md)
- [Release Notes](apps/worker-safety-gear-detection/RELEASE_NOTES.md)
- [Docker Hub Info](apps/worker-safety-gear-detection/README_dockerhub.md)

## License

Refer to the original project license.

## Support

For issues and questions, contact the development team.
