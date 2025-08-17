# MIMIC-IV FHIR Server with Docker Compose

> **A complete solution for serving MIMIC-IV healthcare data through a HAPI FHIR server with PostgreSQL backend, patient filtering capabilities, and bulk import automation.**

## 🎯 Overview

This repository provides a complete, production-ready toolkit for deploying the **MIMIC-IV dataset** as a FHIR R4 server using HAPI FHIR, PostgreSQL, and Docker Compose. Perfect for healthcare data scientists, ML engineers, and researchers who need rapid access to structured clinical data through standardized FHIR APIs.

### Key Features

- 🚀 **One-command deployment** with Docker Compose
- 🔍 **Patient subset filtering** for targeted analysis and resource optimization
- 📁 **Bulk import automation** with progress monitoring and error handling
- 🌐 **Optional Cloudflare tunnel** for external access (demo purposes only)
- 📝 **Jupyter notebook** for patient cohort exploration and filtering


## 📋 Prerequisites

### 1. MIMIC-IV Access
You **must** complete the PhysioNet credentialing process and agree to the data use agreement:

- **Full Dataset**: [MIMIC-IV v3.1](https://physionet.org/content/mimiciv/3.1/)
- **Demo Dataset** (for testing): [MIMIC-IV FHIR Demo v2.0](https://physionet.org/content/mimic-iv-fhir-demo/2.0/)

### 2. System Requirements
- **Docker** and **Docker Compose** (Docker Desktop on macOS)
- **8GB+ RAM** (4GB minimum for demo dataset)
- **50GB+ disk space** (for full dataset, 5GB for demo)
- **Python 3.8+** (optional, for filtering scripts)

### 3. Data Preparation
1. Download NDJSON files from PhysioNet
2. Place all `.ndjson.gz` files in the `./fhir` directory
3. Keep original filenames and compression (faster loading)

---

## 🚀 Quick Start

### 1. Clone and Setup
```bash
git clone https://github.com/your-username/mimic-iv-on-fhir-21.git
cd mimic-iv-on-fhir-21

# Create fhir directory and place your NDJSON files here
mkdir -p fhir
# Copy your downloaded MIMIC-IV FHIR files to ./fhir/
```

### 2. Start the Stack
```bash
# Start all services
docker compose up -d

# Monitor startup
docker compose logs -f fhir
```

### 3. Health Checks
```bash
# Check FHIR server capability
curl -f http://localhost:8080/fhir/metadata

# Check file server
curl -I http://localhost:8000/

# View service status
docker compose ps
```

### 4. Import Data
```bash
# Import all FHIR resources
python bulk_import.py \
    --fhir-url http://localhost:8080/fhir \
    --file-server-url http://fhir-files:8000

# Or import specific resources only
python bulk_import.py \
    --fhir-url http://localhost:8080/fhir \
    --file-server-url http://fhir-files:8000 \
    --files MimicPatient.ndjson.gz MimicCondition.ndjson.gz
```

### 5. Query Your Data
```bash
# Get patient count
curl "http://localhost:8080/fhir/Patient?_summary=count"

# Search for specific conditions
curl "http://localhost:8080/fhir/Condition?code=I50.9&_count=10"

# Get observations for a patient
curl "http://localhost:8080/fhir/Observation?subject=Patient/12345&_count=5"
```

---

## 🔍 Patient Subset Filtering (Performance Optimization)

For large-scale analysis or resource-constrained environments, you can filter data to include only specific patient cohorts:

### 1. Identify Target Patients

Use the included Jupyter notebook to explore and identify patients:

```bash
jupyter notebook explore.ipynb
```

**Example: Find patients with specific conditions**
```python
# Get all patients with diabetes (ICD-10: E11)
patients = get_patients_with_condition("E11")

# Save patient IDs for filtering
with open("patient_ids_to_filter.txt", "w") as f:
    for patient_id in patients:
        f.write(f"{patient_id}\n")
```

### 2. Filter NDJSON Files

```bash
python filter_fhir_by_patients.py \
    --patient-list patient_ids_to_filter.txt \
    --fhir-dir ./fhir \
    --output-dir ./filtered_fhir_cohort
```

### 3. Import Filtered Data

```bash
# Update the script to point to your filtered directory
# Then run the bulk import
python bulk_import.py \
    --fhir-url http://localhost:8080/fhir \
    --file-server-url http://fhir-files:8000
```
---

## 📊 Data Exploration Tools

### FHIR API Testing
- **Swagger UI**: `http://localhost:8080/fhir/` (built-in HAPI interface)
- **Vanya Labs**: [vanyalabs.com](https://vanyalabs.com) (point to `http://localhost:8080/fhir`)

---

## ⚙️ Configuration

### Bulk Import Script Parameters

```bash
python bulk_import.py [OPTIONS]
```

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--fhir-url` | HAPI FHIR server base URL | `http://localhost:8080/fhir` |
| `--file-server-url` | Internal file server URL | `http://fhir-files:8000` |
| `--files` | Specific files to import | Auto-discover all |
| `--dry-run` | Preview import plan without execution | `False` |
| `--timeout` | Max wait time for completion (seconds) | `3600` |


---

*Built with ❤️ for the healthcare AI community*
