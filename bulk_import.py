#!/usr/bin/env python3
"""
HAPI FHIR Bulk Import Script
Performs bulk import of NDJSON files to HAPI FHIR server using the $import operation.
"""

import requests
import json
import time
import os
import argparse
from typing import List, Dict, Optional
from urllib.parse import urljoin

class FHIRBulkImporter:
    """Handles bulk import operations for HAPI FHIR server."""
    
    def __init__(self, fhir_base_url: str = "http://localhost:8080/fhir", 
                 file_server_url: str = "http://fhir-files:8000"):
        self.fhir_base_url = fhir_base_url.rstrip('/')
        self.file_server_url = file_server_url.rstrip('/')
        self.session = requests.Session()
        
        # Set default headers
        self.session.headers.update({
            'Content-Type': 'application/fhir+json',
            'Accept': 'application/fhir+json',
            'Prefer': 'respond-async'
        })
    
    def get_available_files(self) -> List[str]:
        """Get list of available NDJSON files from the file server."""
        try:
            # Try to get a simple directory listing (this might not work with our custom server)
            # Instead, we'll use the known file patterns
            fhir_dir = "./fhir"
            if os.path.exists(fhir_dir):
                files = [f for f in os.listdir(fhir_dir) 
                        if f.endswith(('.ndjson', '.ndjson.gz')) and not f.startswith('.')]
                return sorted(files)
            else:
                print(f"Warning: Local fhir directory not found at {fhir_dir}")
                return []
        except Exception as e:
            print(f"Error getting file list: {e}")
            return []
    
    def create_import_job(self, input_files: List[Dict]) -> Optional[str]:
        """
        Create a bulk import job using the $import operation.
        
        Args:
            input_files: List of input file dictionaries with 'type' and 'url' keys
            
        Returns:
            Job ID or None if failed
        """
        import_url = f"{self.fhir_base_url}/$import"
        
        # Prepare the import request payload using SMART on FHIR bulk import specification
        import_request = {
            "resourceType": "Parameters",
            "parameter": [
                {
                    "name": "inputFormat",
                    "valueCode": "application/fhir+ndjson"
                },
                {
                    "name": "inputSource",
                    "valueUri": self.file_server_url
                },
                {
                    "name": "storageDetail",
                    "part": [
                        {
                            "name": "type",
                            "valueCode": "https"
                        }
                    ]
                }
            ]
        }
        
        # Add input files using SMART specification format
        for input_file in input_files:
            import_request["parameter"].append({
                "name": "input",
                "part": [
                    {
                        "name": "type",
                        "valueCode": input_file["type"]
                    },
                    {
                        "name": "url",
                        "valueUri": input_file["url"]
                    }
                ]
            })
        
        try:
            print(f"Creating import job with {len(input_files)} files...")
            print(f"Import URL: {import_url}")
            
            # Debug: Show the request payload
            print(f"Request payload preview:")
            print(f"  Input format: application/fhir+ndjson")
            print(f"  Input source: {self.file_server_url}")
            print(f"  Files to import:")
            for input_file in input_files[:5]:  # Show first 5 files
                print(f"    {input_file['type']}: {input_file['url']}")
            if len(input_files) > 5:
                print(f"    ... and {len(input_files) - 5} more files")
            
            # HAPI FHIR requires async processing for $import operations
            headers = {
                'Content-Type': 'application/fhir+json',
                'Prefer': 'respond-async'
            }
            
            response = self.session.post(
                import_url,
                json=import_request,
                headers=headers
            )
            
            print(f"Response status: {response.status_code}")
            
            if response.status_code == 202:  # Accepted
                # Extract job ID from Content-Location header
                content_location = response.headers.get('Content-Location')
                if content_location:
                    job_id = content_location.split('/')[-1]
                    print(f"Import job created successfully. Job ID: {job_id}")
                    return job_id
                else:
                    print("Warning: No Content-Location header found")
                    print(f"Available headers: {dict(response.headers)}")
                    return None
            else:
                print(f"Failed to create import job: {response.status_code}")
                print(f"Response headers: {dict(response.headers)}")
                print(f"Response body: {response.text}")
                
                # Try to parse error details if it's JSON
                try:
                    error_data = response.json()
                    if "issue" in error_data:
                        for issue in error_data["issue"]:
                            severity = issue.get("severity", "unknown")
                            code = issue.get("code", "unknown")
                            details = issue.get("details", {}).get("text", issue.get("diagnostics", "No details"))
                            print(f"  {severity.upper()}: [{code}] {details}")
                except:
                    pass  # Response is not JSON
                
                return None
                
        except Exception as e:
            print(f"Error creating import job: {e}")
            return None
    
    def check_job_status(self, job_id: str) -> Dict:
        """Check the status of a bulk import job."""
        status_url = f"{self.fhir_base_url}/$import-poll-status/{job_id}"
        
        try:
            response = self.session.get(status_url)
            
            if response.status_code == 200:
                return {
                    "status": "completed",
                    "data": response.json()
                }
            elif response.status_code == 202:
                # Still in progress
                retry_after = response.headers.get('Retry-After', '30')
                return {
                    "status": "in-progress",
                    "retry_after": int(retry_after)
                }
            else:
                return {
                    "status": "error",
                    "message": f"HTTP {response.status_code}: {response.text}"
                }
                
        except Exception as e:
            return {
                "status": "error",
                "message": str(e)
            }
    
    def wait_for_completion(self, job_id: str, max_wait_time: int = 3600) -> bool:
        """
        Wait for import job completion.
        
        Args:
            job_id: The job ID to monitor
            max_wait_time: Maximum time to wait in seconds
            
        Returns:
            True if completed successfully, False otherwise
        """
        start_time = time.time()
        
        print(f"Monitoring job {job_id}...")
        
        while time.time() - start_time < max_wait_time:
            status = self.check_job_status(job_id)
            
            if status["status"] == "completed":
                print("Import job completed successfully!")
                if "data" in status:
                    self.print_completion_summary(status["data"])
                return True
            elif status["status"] == "error":
                print(f"Import job failed: {status['message']}")
                return False
            elif status["status"] == "in-progress":
                retry_after = status.get("retry_after", 30)
                print(f"Job still in progress. Checking again in {retry_after} seconds...")
                time.sleep(retry_after)
            else:
                print(f"Unknown status: {status}")
                time.sleep(30)
        
        print(f"Timeout waiting for job completion after {max_wait_time} seconds")
        return False
    
    def print_completion_summary(self, completion_data: Dict):
        """Print a summary of the completed import job using SMART specification format."""
        print("\n" + "="*60)
        print("IMPORT COMPLETION SUMMARY")
        print("="*60)
        
        # Check for transaction time
        if "transactionTime" in completion_data:
            print(f"Transaction Time: {completion_data['transactionTime']}")
        
        # Print output summary (successful imports)
        if "output" in completion_data:
            print(f"\nSuccessfully imported resources:")
            total_count = 0
            for output in completion_data["output"]:
                resource_type = output.get("type", "Unknown")
                count = output.get("count", 0)
                input_url = output.get("inputUrl", output.get("input", "Unknown"))
                total_count += count if isinstance(count, int) else 0
                print(f"  {resource_type:<30} {count:>10} resources")
                if "url" in output and output["url"]:
                    print(f"    Success details: {output['url']}")
            print(f"  {'TOTAL':<30} {total_count:>10} resources")
        
        # Print error summary
        if "error" in completion_data and completion_data["error"]:
            print(f"\nErrors encountered:")
            total_errors = 0
            for error in completion_data["error"]:
                resource_type = error.get("type", "Unknown")
                count = error.get("count", 0)
                input_url = error.get("inputUrl", error.get("input", "Unknown"))
                total_errors += count if isinstance(count, int) else 0
                print(f"  {resource_type:<30} {count:>10} errors")
                if "url" in error and error["url"]:
                    print(f"    Error details: {error['url']}")
            print(f"  {'TOTAL ERRORS':<30} {total_errors:>10}")
        else:
            print("\n✅ No errors reported")
        
        # Print any extensions
        if "extension" in completion_data:
            print(f"\nAdditional information:")
            for key, value in completion_data["extension"].items():
                print(f"  {key}: {value}")
        
        print("="*60)
    
    def map_filename_to_resource_type(self, filename: str) -> str:
        """Map MIMIC filename to FHIR resource type."""
        # Remove file extensions
        base_name = filename.replace('.ndjson.gz', '').replace('.ndjson', '')
        
        # Mapping from MIMIC file patterns to FHIR resource types
        type_mapping = {
            'MimicPatient': 'Patient',
            'MimicCondition': 'Condition',
            'MimicEncounter': 'Encounter',
            'MimicLocation': 'Location',
            'MimicOrganization': 'Organization',
            'MimicMedication': 'Medication',
            'MimicMedicationAdministration': 'MedicationAdministration',
            'MimicMedicationDispense': 'MedicationDispense',
            'MimicMedicationRequest': 'MedicationRequest',
            'MimicMedicationStatement': 'MedicationStatement',
            'MimicObservation': 'Observation',
            'MimicProcedure': 'Procedure',
            'MimicSpecimen': 'Specimen'
        }
        
        # Find matching pattern
        for pattern, resource_type in type_mapping.items():
            if base_name.startswith(pattern):
                return resource_type
        
        # Default fallback
        if 'Patient' in base_name:
            return 'Patient'
        elif 'Condition' in base_name:
            return 'Condition'
        elif 'Encounter' in base_name:
            return 'Encounter'
        elif 'Observation' in base_name:
            return 'Observation'
        elif 'Medication' in base_name:
            if 'Administration' in base_name:
                return 'MedicationAdministration'
            elif 'Dispense' in base_name:
                return 'MedicationDispense'
            elif 'Request' in base_name:
                return 'MedicationRequest'
            elif 'Statement' in base_name:
                return 'MedicationStatement'
            else:
                return 'Medication'
        elif 'Procedure' in base_name:
            return 'Procedure'
        elif 'Specimen' in base_name:
            return 'Specimen'
        elif 'Location' in base_name:
            return 'Location'
        elif 'Organization' in base_name:
            return 'Organization'
        else:
            return 'Unknown'

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='HAPI FHIR Bulk Import Tool')
    parser.add_argument('--fhir-url', default='http://localhost:8080/fhir',
                       help='HAPI FHIR server base URL')
    parser.add_argument('--file-server-url', default='http://fhir-files:8000',
                       help='File server URL (use localhost:8000 for local testing)')
    parser.add_argument('--files', nargs='*',
                       help='Specific files to import (default: all available files)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be imported without actually doing it')
    parser.add_argument('--timeout', type=int, default=3600,
                       help='Maximum time to wait for completion (seconds)')
    
    args = parser.parse_args()
    
    # Create importer instance
    importer = FHIRBulkImporter(args.fhir_url, args.file_server_url)
    
    # Get available files
    available_files = importer.get_available_files()
    
    if not available_files:
        print("No NDJSON files found!")
        return
    
    # Determine which files to import
    if args.files:
        files_to_import = [f for f in args.files if f in available_files]
        if len(files_to_import) != len(args.files):
            missing = set(args.files) - set(files_to_import)
            print(f"Warning: Files not found: {missing}")
    else:
        files_to_import = available_files
    
    if not files_to_import:
        print("No valid files to import!")
        return
    
    # Prepare input files for import
    input_files = []
    for filename in files_to_import:
        resource_type = importer.map_filename_to_resource_type(filename)
        file_url = f"{args.file_server_url}/{filename}"
        
        input_files.append({
            "type": resource_type,
            "url": file_url
        })
    
    # Show what will be imported
    print("Files to import:")
    print("-" * 60)
    for input_file in input_files:
        print(f"  {input_file['type']:<30} {input_file['url']}")
    print("-" * 60)
    print(f"Total files: {len(input_files)}")
    
    if args.dry_run:
        print("\nDry run mode - no actual import will be performed.")
        return
    
    # Confirm before proceeding
    response = input("\nProceed with bulk import? (y/N): ")
    if response.lower() != 'y':
        print("Import cancelled.")
        return
    
    # Create and monitor import job
    job_id = importer.create_import_job(input_files)
    
    if job_id:
        success = importer.wait_for_completion(job_id, args.timeout)
        if success:
            print(f"\nBulk import completed successfully!")
        else:
            print(f"\nBulk import failed or timed out.")
    else:
        print("Failed to create import job.")

if __name__ == '__main__':
    main()
