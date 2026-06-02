# Deploy using Helm charts

## Prerequisites

- Ensure you have the **minimum system requirements** for this application.
- K8s installation on single or multi node must be done as pre-requisite to continue the following deployment. Note: The kubernetes cluster is set up with `kubeadm`, `kubectl` and `kubelet` packages on single and multi nodes with `v1.30.2`.
  Refer to tutorials online to setup kubernetes cluster on the web with host OS as ubuntu 22.04 and/or ubuntu 24.04.
- For helm installation, refer to [helm website](https://helm.sh/docs/intro/install/)

## Setup the application

> **Note**: The following instructions assume Kubernetes is already running in the host system with helm package manager installed.

1. Clone the **edge-ai-suites** repository and change into industrial-edge-insights-vision directory. The directory contains the utility scripts required in the instructions that follows.

    ```sh
    git clone https://github.com/open-edge-platform/edge-ai-suites.git -b release-2025.2.0
    cd edge-ai-suites/manufacturing-ai-suite/industrial-edge-insights-vision/
    ```

2. Set app specific values.yaml file.

    ```sh
    cp helm/values_worker_safety_gear_detection.yaml helm/values.yaml
    ```

3.  Edit the HOST_IP, proxy and other environment variables in `helm/values.yaml` as follows

    ```yaml
    env:
        HOST_IP: <HOST_IP>   # host IP address
        MINIO_ACCESS_KEY: <DATABASE USERNAME> #  example: minioadmin
        MINIO_SECRET_KEY: <DATABASE PASSWORD> #  example: minioadmin
        http_proxy: <http proxy> # proxy details if behind proxy
        https_proxy: <https proxy>
        POSTGRES_PASSWORD: <POSTGRES PASSWORD> #  example: intel1234
        MR_URL: https://<HOST_IP>:30443/registry/ # Model reigstry URL
        SAMPLE_APP: worker-safety-gear-detection # application directory
    webrtcturnserver:
        username: <username>  # WebRTC credentials e.g. intel1234
        password: <password>
    ```
4.  Install pre-requisites. Run with sudo if needed.
    ```sh
    ./setup.sh helm
    ```
    This sets up application pre-requisites, download artifacts, sets executable permissions for scripts etc. Downloaded resource directories.

## Deploy the application

5.  Install the helm chart
    ```sh
    helm install app-deploy helm -n apps --create-namespace
    ```
    After installation, check the status of the running pods:
    ```sh
    kubectl get pods -n apps
    ```
    To view logs of a specific pod, replace `<pod_name>` with the actual pod name from the output above:
    ```sh
    kubectl logs -n apps -f <pod_name>
    ```

6.  Copy the model resources to the `dlstreamer-pipeline-server` pod so they are available for dynamic pipeline launches.
    ```sh
    # Below is an example for Worker Safety Gear Detection.

    POD_NAME=$(kubectl get pods -n apps -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | grep deployment-dlstreamer-pipeline-server | head -n 1)

    kubectl cp resources/worker-safety-gear-detection/models/* $POD_NAME:/home/pipeline-server/resources/models/ -c dlstreamer-pipeline-server -n apps
    ```
7.  Fetch the list of loaded pipelines
    ```sh
    ./sample_list.sh
    ```
    This lists the pipeline loaded in DLStreamer Pipeline Server.

    Output:
    ```sh
    # Example output for Worker Safety Gear Detection
    Environment variables loaded from /home/intel/OEP/edge-ai-suites/manufacturing-ai-suite/industrial-edge-insights-vision/.env
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
8.  Configure the first camera device in KLYFF.
    ```sh
    {
      "nvrEnabled": true,
      "sourceUri": "rtsp://mediamtx-server:8554/cam1"
    }
    ```
    Add those as server-side attributes on a KLYFF device such as `Safety_Cam1`.

9.  Watch the sync service discover the device and start the pipeline automatically.
    ```sh
    kubectl logs -n apps -f <klyff-bridge-pod-name>
    ```
    Successful onboarding looks like:
    - `mqtt_topics_reconciled`
    - `pipeline_started`
    - `reconcile_completed`
    - `telemetry_forwarded`

    > NOTE: For a device named `Safety_Cam1`, the derived WebRTC path is typically `worker_safety_rtsp_safety_cam1`.

10.  Get status of pipeline instance(s) running.
    ```sh
    ./sample_status.sh
    ```
    This command lists status of pipeline instances launched during the lifetime of sample application.

    Output:
    ```sh
    # Example output for Worker Safety Gear Detection
    Environment variables loaded from /home/intel/OEP/edge-ai-suites/manufacturing-ai-suite/industrial-edge-insights-vision/.env
    Running sample app: worker-safety-gear-detection
    [
    {
        "avg_fps": 30.00446179356829,
        "elapsed_time": 36.927825689315796,
        "id": "99ac50d852b511f09f7c2242868ff651",
        "message": "",
        "start_time": 1750956469.620569,
        "state": "RUNNING"
    }
    ]
    ```

11. Stop pipeline instance.
    ```sh
    ./sample_stop.sh
    ```
    This command will stop all instances that are currently in `RUNNING` state and respond with the last status.

    Output:
    ```sh
    # Example output for Worker Safety Gear Detection
    No pipelines specified. Stopping all pipeline instances
    Environment variables loaded from /home/intel/OEP/edge-ai-suites/manufacturing-ai-suite/industrial-edge-insights-vision/.env
    Running sample app: worker-safety-gear-detection
    Checking status of dlstreamer-pipeline-server...
    Server reachable. HTTP Status Code: 200
    Instance list fetched successfully. HTTP Status Code: 200
    Found 1 running pipeline instances.
    Stopping pipeline instance with ID: 99ac50d852b511f09f7c2242868ff651
    Pipeline instance with ID '99ac50d852b511f09f7c2242868ff651' stopped successfully. Response: {
    "avg_fps": 30.01631239459745,
    "elapsed_time": 49.30651903152466,
    "id": "99ac50d852b511f09f7c2242868ff651",
    "message": "",
    "start_time": 1750960037.1471195,
    "state": "RUNNING"
    }
    ```
    If you wish to stop a specific instance, you can provide it with an `--id` argument to the command.
    For example, `./sample_stop.sh --id 99ac50d852b511f09f7c2242868ff651`

12. Uninstall the helm chart.
     ```sh
     helm uninstall app-deploy -n apps
     ```


## Troubleshooting
- [Troubleshooting Guide](../../../docs/user-guide/worker-safety-gear-detection/troubleshooting-guide.md)
