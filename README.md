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

4. **Configure dynamic NVR device discovery from KLYFF**

   The `klyff-bridge` service runs in dynamic sync mode. In this mode it:
   - fetches device attributes from KLYFF at runtime;
   - starts or restarts DL Streamer pipelines when devices are added or changed;
   - subscribes to the corresponding local MQTT metadata topics;
   - pushes flattened inference telemetry and sync status back to each KLYFF device.
   - uses [nvr-base-config.json](/home/cleaff/Desktop/Inspeqtr-Safety/apps/worker-safety-gear-detection/nvr-base-config.json:1) for shared-NVR defaults instead of the static camera inventory.

   Enable it in `.env`:
   ```dotenv
   KLYFF_API_BASE_URL=https://<your-klyff-host>
   KLYFF_API_KEY=<tenant-api-key>
   KLYFF_REQUIRE_NVR_ENABLED=true
   PIPELINE_SERVER_URL=http://dlstreamer-pipeline-server:8080
   PIPELINE_SERVER_API_PREFIX=
   LOCAL_MQTT_QOS=0
   ```

   `PIPELINE_SERVER_API_PREFIX` should stay empty for direct container-to-container access.
   If you ever route the sync service through nginx instead, set it to `/api`.

   Minimal operator flow:
   - create a device in KLYFF;
   - add `nvrEnabled=true`;
   - add `sourceUri=rtsp://...`;
   - the sync service derives the rest from the device name and app defaults.

   Auto-derived defaults:
   - `metadataTopic` becomes `worker_safety_predictions_<device-name-slug>`
   - `peerId` becomes `worker_safety_rtsp_<device-name-slug>`
   - `pipeline` falls back to the app topology default, currently `worker_safety_gear_detection_mqtt`
   - `enabled` defaults to `true`

   Optional device attributes:
   - `metadataTopic`
   - `peerId`
   - `pipeline`
   - `enabled`
   - `useSharedPipeline` to opt a device out of shared batching
   - `detectionProperties` as a JSON object

   By default the app now follows the shared batched NVR model:
   - each camera still launches its own pipeline instance;
   - all cameras share the same `model-instance-id`;
   - `batch-size` is auto-sized to the number of participating streams.
   Set `useSharedPipeline=false` on a device only if you want it isolated from the shared batch.

   The resolved values are written back to the device as server attributes:
   - `nvrMetadataTopic`
   - `nvrPeerId`
   - `nvrPipelineName`
   - `nvrSourceUri`
   - `nvrSyncStatus`

## Deploy the Application

1. Start the Docker application:

   ```bash
   docker compose up -d
   ```

2. Add a device in KLYFF.

   Add the minimal server attributes:
   ```json
   {
     "nvrEnabled": true,
     "sourceUri": "rtsp://mediamtx-server:8554/cam1"
   }
   ```

3. Watch the sync service onboard the device automatically.

   ```bash
   docker compose logs -f klyff-bridge
   ```

   Successful onboarding looks like:
   - `mqtt_topics_reconciled`
   - `pipeline_started`
   - `reconcile_completed`
   - `telemetry_forwarded`

4. Open the processed stream in a browser.

   For a device named `Safety_Cam1`, the derived peer id is `worker_safety_rtsp_safety_cam1`, so the view URL is:
   ```sh
   https://localhost/mediamtx/worker_safety_rtsp_safety_cam1/
   ```

5. Stop the Docker application.

   ```bash
   docker compose down -v
   ```

  
