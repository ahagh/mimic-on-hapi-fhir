#!/bin/bash

# FHIR Database Monitor Script
# Monitors the progress of FHIR data ingestion

FHIR_BASE_URL="http://localhost:8080/fhir"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}📊 FHIR Database Monitor${NC}"
echo "========================"

# Function to get resource count
get_resource_count() {
    local resource_type=$1
    curl -s "$FHIR_BASE_URL/$resource_type?_summary=count" | jq -r '.total // 0' 2>/dev/null || echo "0"
}

# Function to get server info
get_server_info() {
    echo -e "${BLUE}🖥️  Server Information:${NC}"
    local server_info=$(curl -s "$FHIR_BASE_URL/metadata" | jq -r '.software.name + " " + .software.version' 2>/dev/null || echo "Unable to get server info")
    local fhir_version=$(curl -s "$FHIR_BASE_URL/metadata" | jq -r '.fhirVersion' 2>/dev/null || echo "Unknown")
    echo "   Server: $server_info"
    echo "   FHIR Version: $fhir_version"
    echo "   Base URL: $FHIR_BASE_URL"
    echo ""
}

# Function to display resource counts
show_resource_counts() {
    echo -e "${BLUE}📊 Resource Counts:${NC}"
    echo "==================="
    
    local resource_types=(
        "Patient"
        "Organization" 
        "Location"
        "Encounter"
        "Condition"
        "Medication"
        "MedicationAdministration"
        "MedicationDispense"
        "MedicationRequest"
        "MedicationStatement"
        "Observation"
        "Procedure"
        "Specimen"
    )
    
    local total_resources=0
    
    for resource_type in "${resource_types[@]}"; do
        local count=$(get_resource_count "$resource_type")
        if [[ "$count" != "0" ]]; then
            printf "   %-25s: %'d resources\n" "$resource_type" "$count"
            total_resources=$((total_resources + count))
        fi
    done
    
    echo "   ----------------------------------------"
    printf "   %-25s: %'d resources\n" "TOTAL" "$total_resources"
    echo ""
}

# Function to show expected vs actual counts
show_expected_counts() {
    echo -e "${BLUE}📋 Expected vs Actual Counts:${NC}"
    echo "=============================="
    
    # Check specific resource types with expected counts
    local patient_count=$(get_resource_count "Patient")
    local org_count=$(get_resource_count "Organization")
    local encounter_count=$(get_resource_count "Encounter")
    local condition_count=$(get_resource_count "Condition")
    local observation_count=$(get_resource_count "Observation")
    
    if [[ "$patient_count" != "0" ]]; then
        local patient_pct=$((patient_count * 100 / 299712))
        printf "   %-20s: %8d / %-10s" "Patient" "$patient_count" "299,712"
        if [[ $patient_pct -ge 90 ]]; then
            echo -e " ${GREEN}(${patient_pct}% complete)${NC}"
        elif [[ $patient_pct -ge 50 ]]; then
            echo -e " ${YELLOW}(${patient_pct}% complete)${NC}"
        else
            echo -e " ${RED}(${patient_pct}% complete)${NC}"
        fi
    fi
    
    if [[ "$org_count" != "0" ]]; then
        printf "   %-20s: %8d / %-10s" "Organization" "$org_count" "1"
        echo -e " ${GREEN}(100% complete)${NC}"
    fi
    
    if [[ "$encounter_count" != "0" ]]; then
        printf "   %-20s: %8d / %-10s" "Encounter" "$encounter_count" "~750,000"
        echo " (estimated)"
    fi
    
    if [[ "$condition_count" != "0" ]]; then
        printf "   %-20s: %8d / %-10s" "Condition" "$condition_count" "~2,500,000"
        echo " (estimated)"
    fi
    
    if [[ "$observation_count" != "0" ]]; then
        printf "   %-20s: %8d / %-10s" "Observation" "$observation_count" "~50,000,000"
        echo " (estimated)"
    fi
    
    echo ""
}

# Function to check database size
show_database_info() {
    echo -e "${BLUE}💾 Database Information:${NC}"
    echo "========================"
    
    # Try to get database size if psql is available
    if command -v psql &> /dev/null; then
        local db_size=$(PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -d postgres -t -c "SELECT pg_size_pretty(pg_database_size('postgres'));" 2>/dev/null | xargs)
        if [[ -n "$db_size" ]]; then
            echo "   Database Size: $db_size"
        fi
        
        local table_count=$(PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -d postgres -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'hfj_%';" 2>/dev/null | xargs)
        if [[ -n "$table_count" ]]; then
            echo "   HAPI FHIR Tables: $table_count"
        fi
    else
        echo "   psql not available - cannot get database details"
    fi
    echo ""
}

# Function to show recent activity
show_recent_activity() {
    echo -e "${BLUE}📈 Recent Activity:${NC}"
    echo "==================="
    
    # Get recently created resources (simple check for any recent patients)
    local total_patients=$(get_resource_count "Patient")
    
    if [[ "$total_patients" != "0" ]]; then
        echo "   Total Patients in database: $total_patients"
        echo "   (Monitoring individual resource creation requires more complex queries)"
    else
        echo "   No resources detected"
    fi
    echo ""
}

# Function to monitor continuously
monitor_continuously() {
    echo -e "${YELLOW}🔄 Continuous monitoring (Ctrl+C to stop)${NC}"
    echo "==========================================="
    echo ""
    
    while true; do
        clear
        echo -e "${BLUE}📊 FHIR Database Monitor - $(date)${NC}"
        echo "=================================================="
        echo ""
        
        get_server_info
        show_resource_counts
        show_expected_counts
        show_database_info
        show_recent_activity
        
        echo "Press Ctrl+C to stop monitoring..."
        sleep 30
    done
}

# Main execution
case "${1:-once}" in
    "continuous"|"monitor"|"watch")
        monitor_continuously
        ;;
    "once"|"")
        get_server_info
        show_resource_counts
        show_expected_counts
        show_database_info
        show_recent_activity
        ;;
    "help"|"-h"|"--help")
        echo "FHIR Database Monitor"
        echo ""
        echo "Usage:"
        echo "  $0 [command]"
        echo ""
        echo "Commands:"
        echo "  once         Show current status (default)"
        echo "  continuous   Monitor continuously (refresh every 30s)"
        echo "  monitor      Alias for continuous"
        echo "  watch        Alias for continuous"
        echo "  help         Show this help"
        ;;
    *)
        echo "Unknown command: $1"
        echo "Use '$0 help' for usage information"
        exit 1
        ;;
esac
