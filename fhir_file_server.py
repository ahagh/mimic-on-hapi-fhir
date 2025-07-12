#!/usr/bin/env python3
"""
FHIR File Server for Bulk Import
Serves NDJSON files via HTTP for HAPI FHIR server bulk import operations.
"""

import os
import mimetypes
import gzip
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import unquote
import argparse
import logging

class FHIRFileHandler(SimpleHTTPRequestHandler):
    """Custom HTTP request handler for serving FHIR NDJSON files."""
    
    def __init__(self, *args, **kwargs):
        # Set the directory to serve files from
        super().__init__(*args, directory="/app/fhir", **kwargs)
    
    def end_headers(self):
        """Add CORS headers and custom headers for FHIR compatibility."""
        # Enable CORS for FHIR server access
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        
        # Add cache control headers
        self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        
        super().end_headers()
    
    def do_OPTIONS(self):
        """Handle preflight CORS requests."""
        self.send_response(200)
        self.end_headers()
    
    def do_GET(self):
        """Handle GET requests with proper content type for NDJSON files."""
        # Log the request
        logging.info(f"GET request for: {self.path}")
        
        # Decode the path
        path = unquote(self.path.lstrip('/'))
        
        # Security check - prevent directory traversal
        if '..' in path or path.startswith('/'):
            self.send_error(403, "Forbidden")
            return
        
        # Build full file path
        full_path = os.path.join("/app/fhir", path)
        
        # Check if file exists
        if not os.path.exists(full_path):
            logging.error(f"File not found: {full_path}")
            self.send_error(404, "File not found")
            return
        
        # Check if it's a file (not directory)
        if not os.path.isfile(full_path):
            self.send_error(403, "Not a file")
            return
        
        try:
            # Determine content type
            content_type = 'application/fhir+ndjson'
            content_encoding = None
            
            if path.endswith('.ndjson.gz'):
                content_encoding = 'gzip'
            elif path.endswith('.ndjson'):
                pass  # Already set to application/fhir+ndjson
            else:
                # Fallback to default mime type detection
                content_type, _ = mimetypes.guess_type(full_path)
                if content_type is None:
                    content_type = 'application/octet-stream'
            
            # Get file size
            file_size = os.path.getsize(full_path)
            
            # Send response headers
            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(file_size))
            
            if content_encoding:
                self.send_header('Content-Encoding', content_encoding)
            
            self.end_headers()
            
            # Send file content
            with open(full_path, 'rb') as f:
                while True:
                    chunk = f.read(8192)  # Read in 8KB chunks
                    if not chunk:
                        break
                    self.wfile.write(chunk)
            
            logging.info(f"Successfully served: {path} ({file_size} bytes)")
            
        except Exception as e:
            logging.error(f"Error serving file {path}: {str(e)}")
            self.send_error(500, f"Internal server error: {str(e)}")
    
    def log_message(self, format, *args):
        """Override to use logging instead of stderr."""
        logging.info(f"{self.address_string()} - {format % args}")

def list_available_files():
    """List all available NDJSON files."""
    fhir_dir = "/app/fhir"
    if not os.path.exists(fhir_dir):
        print(f"FHIR directory not found: {fhir_dir}")
        return
    
    print("Available FHIR NDJSON files:")
    print("-" * 50)
    
    files = []
    for filename in os.listdir(fhir_dir):
        if filename.endswith(('.ndjson', '.ndjson.gz')) and not filename.startswith('.'):
            filepath = os.path.join(fhir_dir, filename)
            size = os.path.getsize(filepath)
            files.append((filename, size))
    
    # Sort files by name
    files.sort()
    
    for filename, size in files:
        size_mb = size / (1024 * 1024)
        print(f"  {filename:<40} ({size_mb:.2f} MB)")
    
    print("-" * 50)
    print(f"Total files: {len(files)}")

def main():
    """Main function to start the HTTP server."""
    parser = argparse.ArgumentParser(description='FHIR File Server for Bulk Import')
    parser.add_argument('--port', type=int, default=8000, help='Port to serve on (default: 8000)')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to (default: 0.0.0.0)')
    parser.add_argument('--list', action='store_true', help='List available files and exit')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.INFO if args.verbose else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # List files if requested
    if args.list:
        list_available_files()
        return
    
    # Check if FHIR directory exists
    if not os.path.exists("/app/fhir"):
        print("Error: FHIR directory '/app/fhir' not found!")
        print("Make sure to mount your fhir directory to /app/fhir in the Docker container")
        return
    
    # List available files on startup
    list_available_files()
    
    # Start the server
    server_address = (args.host, args.port)
    httpd = HTTPServer(server_address, FHIRFileHandler)
    
    print(f"\nFHIR File Server starting...")
    print(f"Serving files from: /app/fhir")
    print(f"Server address: http://{args.host}:{args.port}")
    print(f"Access files via: http://fhir-files:{args.port}/filename.ndjson.gz")
    print("\nExample bulk import URLs:")
    
    # Show example URLs for some files
    sample_files = ["MimicPatient.ndjson.gz", "MimicCondition.ndjson.gz", "MimicEncounter.ndjson.gz"]
    for filename in sample_files:
        if os.path.exists(f"/app/fhir/{filename}"):
            print(f"  http://fhir-files:{args.port}/{filename}")
    
    print("\nPress Ctrl+C to stop the server")
    print("-" * 60)
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down the server...")
        httpd.shutdown()

if __name__ == '__main__':
    main()
