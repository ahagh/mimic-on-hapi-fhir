#!/bin/bash

# HAPI FHIR Health Monitor
# Quick health check script for HAPI FHIR and PostgreSQL

FHIR_URL="http://localhost:8080/fhir"
POSTGRES_HOST="localhost"

echo "🏥 HAPI FHIR Health Monitor"
echo "=========================="
echo ""

# Check Docker Compose status
echo "🐳 Docker Services Status:"
docker-compose ps

echo ""

# Check FHIR server health
echo "🔍 FHIR Server Health:"
if curl -f -s "$FHIR_URL/metadata" > /dev/null; then
    echo "✅ FHIR server is healthy"
    
    # Get basic stats
    total_patients=$(curl -s "$FHIR_URL/Patient?_summary=count" | jq -r '.total // 0' 2>/dev/null)
    echo "   📊 Total Patients: $total_patients"
    
    total_observations=$(curl -s "$FHIR_URL/Observation?_summary=count" | jq -r '.total // 0' 2>/dev/null)
    echo "   📊 Total Observations: $total_observations"
else
    echo "❌ FHIR server is not responding"
fi

echo ""

# Check PostgreSQL health
echo "🗄️  PostgreSQL Health:"
if docker-compose exec -T postgres-db pg_isready -U postgres > /dev/null 2>&1; then
    echo "✅ PostgreSQL is healthy"
    
    # Get database stats
    if command -v psql &> /dev/null; then
        db_size=$(PGPASSWORD=postgres psql -h $POSTGRES_HOST -p 5432 -U postgres -d postgres -t -c "SELECT pg_size_pretty(pg_database_size('postgres'));" 2>/dev/null | xargs)
        echo "   📊 Database Size: $db_size"
        
        table_count=$(PGPASSWORD=postgres psql -h $POSTGRES_HOST -p 5432 -U postgres -d postgres -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'hfj_%';" 2>/dev/null | xargs)
        echo "   📊 HAPI Tables: $table_count"
    fi
else
    echo "❌ PostgreSQL is not responding"
fi

echo ""

# Check system resources
echo "💾 System Resources:"
echo "   Memory Usage:"
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}" $(docker-compose ps -q) 2>/dev/null || echo "   Unable to get stats"

echo ""
echo "✅ Health check completed"
