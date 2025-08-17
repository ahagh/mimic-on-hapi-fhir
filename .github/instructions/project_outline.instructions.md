---
applyTo: '**'
---
Provide project context and coding guidelines that AI should follow when generating code, answering questions, or reviewing changes.

1. **Project Context**:
   - This project involves the use of FHIR (Fast Healthcare Interoperability Resources) standards for healthcare data.
   - The goal is to facilitate the import and export of healthcare data in NDJSON format.
   - The project includes a FHIR file server, a bulk import script, and a Docker Compose setup for easy deployment.

2. **Coding Guidelines**:
   - Follow PEP 8 style guidelines for Python code.
   - Use meaningful variable and function names.
   - Include docstrings for all functions and classes.
   - Write unit tests for all new features and bug fixes.
   - Use logging instead of print statements for debugging and information output.
   - Ensure that all code is properly commented and documented.

3. **Project Structure**:
   - All FHIR ndjson resources are stored in the `fhir/` directory.
   - `bulk_import.py`: Script for importing NDJSON files into the FHIR server.
   - `fhir_file_server.py`: HTTP server for serving NDJSON files for bulk import.
   - The `scripts/` directory contains utility scripts for data processing and manipulation.
   - The `tests/` directory includes unit tests for all components.
   