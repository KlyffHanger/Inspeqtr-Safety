# Worker Safety Gear Detection

## Overview

**Automated Worker Safety Compliance System** - This enterprise-grade application automatically monitors and enforces worker safety compliance by detecting the presence of required personal protective equipment (PPE) in real-time. Using advanced deep learning models, the system identifies workers not wearing mandatory safety gear, enabling immediate intervention and improving workplace safety culture.

**Required Software**:

- Docker 27.3.1 or higher
- Python 3.10+
- Git, jq, unzip

## Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/KlyffHanger/Inspeqtr-Safety.git
   cd Inspeqtr-Safety
   ```

2. Set app specific environment variable file

   ```bash
   cp .env_worker_safety_gear_detection .env
   ```

3. **Generate local SSL certificates for Nginx**
   ```bash
   ./setup.sh
   ```

4. **Add or change videos**


   Update the video path in:
   - `apps/worker-safety-gear-detection/payload.json`

   Example (set the `source.uri` to your file):
   ```json
   "source": {
     "uri": "file:///home/pipeline-server/resources/videos/worker02.avi",
     "type": "uri"
   }
   ```

## Deploy the Application

1. Start the Docker application:

   ```bash
   docker compose up -d
   ```

2. Fetch the list of pipeline loaded available to launch

   ```bash
   ./sample_list.sh
   ```

   This lists the pipeline loaded in DL Streamer Pipeline Server.

   Example Output:

   ```bash
   # Example output for Worker Safety gear detection
   Environment variables loaded from [WORKDIR]/manufacturing-ai-suite/industrial-edge-insights-vision/.env
   Running sample app: worker-safety-gear-detection
   Checking status of dlstreamer-pipeline-server...
   Server reachable. HTTP Status Code: 200
   Loaded pipelines:
   [
       ...
       {
           "description": "DL Streamer Pipeline Server pipeline",
           "name": "user_defined_pipelines",
           "parameters": {
           "properties": {
               "detection-properties": {
                   "element": {
                       "format": "element-properties",
                       "name": "detection"
                   }
               }
           },
           "type": "object"
           },
           "type": "GStreamer",
           "version": "worker_safety_gear_detection"
       }
       ...
   ]
   ```


3. Start the sample application with a pipeline.
   ```bash
   ./sample_start.sh -p worker_safety_gear_detection
   ```
   Output:

   ```text
   # Example output for Worker Safety gear detection
   Environment variables loaded from [WORKDIR]/manufacturing-ai-suite/industrial-edge-insights-vision/.env
   Running sample app: worker-safety-gear-detection
   Checking status of dlstreamer-pipeline-server...
   Server reachable. HTTP Status Code: 200
   Loading payload from [WORKDIR]/manufacturing-ai-suite/industrial-edge-insights-vision/apps/worker-safety-gear-detection/payload.json
   Payload loaded successfully.
   Starting pipeline: worker_safety_gear_detection
   Launching pipeline: worker_safety_gear_detection
   Extracting payload for pipeline: worker_safety_gear_detection
   Found 1 payload(s) for pipeline: worker_safety_gear_detection
   Payload for pipeline 'worker_safety_gear_detection' {"source":{"uri":"file:///home/pipeline-server/resources/videos/Safety_Full_Hat_and_Vest.avi","type":"uri"},"destination":{"frame":{"type":"webrtc","peer-id":"worker_safety"}},"parameters":{"detection-properties":{"model":"/home/pipeline-server/resources/models/worker-safety-gear-detection/deployment/Detection/model/model.xml","device":"CPU"}}}
   Posting payload to REST server at https://<HOST_IP>/api/pipelines/user_defined_pipelines/worker_safety_gear_detection
   Payload for pipeline 'worker_safety_gear_detection' posted successfully. Response: "784b87b45d1511f08ab0da88aa49c01e"
   ```

   NOTE: This will start the pipeline. The inference stream can be viewed on WebRTC, in a browser, at the following url:


   ```sh
   https://localhost/mediamtx/worker_safety/
   ```

4. Get the status of running pipeline instance(s).

   ```bash
   ./sample_status.sh
   ```

   This command lists the statuses of pipeline instances launched during the lifetime of sample application.

   Output:

   ```text
   # Example output for Worker Safety gear detection
   Environment variables loaded from [WORKDIR]/manufacturing-ai-suite/industrial-edge-insights-vision/.env
   Running sample app: worker-safety-gear-detection
   [
   {
       "avg_fps": 30.036955894826452,
       "elapsed_time": 3.096184492111206,
       "id": "784b87b45d1511f08ab0da88aa49c01e",
       "message": "",
       "start_time": 1752100724.3075056,
       "state": "RUNNING"
   }
   ]
   ```

5. Stop pipeline instances.

   ```bash
   ./sample_stop.sh
   ```
6. Stop the Docker application.

   ```bash
   docker compose down -v
   ```

  

