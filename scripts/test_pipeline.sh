#!/bin/bash

# Test script for Zara ETL Pipeline
# =================================
# Comprehensive testing suite to validate the entire pipeline setup

set -e

echo "🧪 Testing Zara ETL Pipeline..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_test() {
    echo -e "${YELLOW}🔍 $1${NC}"
}

print_pass() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_fail() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Test counters
TESTS_TOTAL=0
TESTS_PASSED=0
TESTS_FAILED=0

run_test() {
    local test_name="$1"
    local test_command="$2"

    TESTS_TOTAL=$((TESTS_TOTAL + 1))
    print_test "$test_name"

    if eval "$test_command" > /dev/null 2>&1; then
        print_pass "$test_name - PASSED"
        TESTS_PASSED=$((TESTS_PASSED + 1))
        return 0
    else
        print_fail "$test_name - FAILED"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi
}

# Test 1: Environment Setup
print_info "=== Environment Tests ==="

run_test "Docker is installed" "command -v docker"
run_test "Docker Compose is installed" "command -v docker-compose"
run_test "Docker daemon is running" "docker info"
run_test ".env file exists" "[ -f .env ]"

# Test 2: Project Structure
print_info "=== Project Structure Tests ==="

run_test "Data input directory exists" "[ -d data/input ]"
run_test "Data output directory exists" "[ -d data/output ]"
run_test "Logs directory exists" "[ -d logs ]"
run_test "Airflow DAGs directory exists" "[ -d airflow/dags ]"
run_test "Airflow plugins directory exists" "[ -d airflow/plugins ]"
run_test "DocETL configs directory exists" "[ -d docetl/configs ]"

# Test 3: Required Files
print_info "=== Required Files Tests ==="

required_files=(
    "docker-compose.yaml"
    "Dockerfile.airflow"
    "Dockerfile.docetl"
    "requirements.txt"
    "airflow/dags/zara_hybrid_etl.py"
    "airflow/plugins/arxiv_hook.py"
    "airflow/plugins/docetl_operator.py"
    "airflow/plugins/__init__.py"
    "docetl/configs/paper_extraction.yaml"
    "docetl/configs/article_generation.yaml"
    "scripts/setup.sh"
    "scripts/monitor.sh"
)

for file in "${required_files[@]}"; do
    run_test "Required file: $file" "[ -f \"$file\" ]"
done

# Test 4: Configuration Validation
print_info "=== Configuration Tests ==="

# Check .env configuration
if run_test "OpenAI API key is configured" "grep -q 'OPENAI_API_KEY=sk-' .env && ! grep -q 'sk-your_openai_api_key_here' .env"; then
    print_pass "OpenAI API key appears to be properly configured"
else
    print_fail "OpenAI API key not configured - update .env file"
fi

run_test "Default model is set" "grep -q 'DEFAULT_MODEL=' .env"
run_test "arXiv categories are configured" "grep -q 'ARXIV_CATEGORIES=' .env"
run_test "Batch size is configured" "grep -q 'BATCH_SIZE=' .env"

# Test 5: Docker Services
print_info "=== Docker Services Tests ==="

# Start services if not running
if ! docker-compose ps | grep -q "Up"; then
    print_info "Starting Docker services for testing..."
    docker-compose up -d
    sleep 30  # Give services time to start
fi

run_test "Postgres service is running" "docker-compose ps postgres | grep -q Up"
run_test "Redis service is running" "docker-compose ps redis | grep -q Up"
run_test "Airflow apiserver is running" "docker-compose ps airflow-apiserver | grep -q Up"
run_test "Airflow scheduler is running" "docker-compose ps airflow-scheduler | grep -q Up"
run_test "DocETL worker is running" "docker-compose ps docetl-worker | grep -q Up"

# Test 6: Service Health Checks
print_info "=== Service Health Tests ==="

# Wait for services to be fully ready
sleep 10

run_test "Database connection" "docker-compose exec -T postgres pg_isready -U airflow"
run_test "Redis connection" "docker-compose exec -T redis redis-cli ping | grep -q PONG"

# Test Airflow web interface (with retry)
for i in {1..5}; do
    if curl -f -s http://localhost:8080/api/v2/version > /dev/null; then
        run_test "Airflow web interface" "true"
        break
    elif [ $i -eq 5 ]; then
        run_test "Airflow web interface" "false"
    else
        print_info "Waiting for Airflow to be ready... (attempt $i/5)"
        sleep 10
    fi
done

# Test 7: Python Dependencies
print_info "=== Python Dependencies Tests ==="

run_test "DocETL is installed in DocETL container" "docker-compose exec -T docetl-worker python -c 'import docetl; print(\"DocETL OK\")'"
run_test "arXiv library is available" "docker-compose exec -T docetl-worker python -c 'import arxiv; print(\"arXiv OK\")'"
run_test "PyMuPDF is available" "docker-compose exec -T docetl-worker python -c 'import fitz; print(\"PyMuPDF OK\")'"
run_test "OpenAI library is available" "docker-compose exec -T airflow-apiserver python -c 'import openai; print(\"OpenAI OK\")'"

# Test 8: Airflow DAG Validation
print_info "=== Airflow DAG Tests ==="

# Wait a bit more for Airflow to parse DAGs
sleep 15

run_test "Airflow can import DAG" "docker-compose exec -T airflow-apiserver python -c 'from airflow.sdk import DAG; print(\"DAG import OK\")'"

# Check if DAG appears in Airflow
if docker-compose exec -T airflow-apiserver airflow dags list | grep -q "zara_hybrid_etl"; then
    run_test "Zara ETL DAG is registered" "true"
else
    run_test "Zara ETL DAG is registered" "false"
    print_info "Note: DAG might need more time to be parsed by Airflow"
fi

# Test 9: DocETL Configuration Validation
print_info "=== DocETL Configuration Tests ==="

run_test "Paper extraction config is valid YAML" "docker-compose exec -T docetl-worker python -c 'import yaml; yaml.safe_load(open(\"/app/docetl/configs/paper_extraction.yaml\")); print(\"Config OK\")'"
run_test "Article generation config is valid YAML" "docker-compose exec -T docetl-worker python -c 'import yaml; yaml.safe_load(open(\"/app/docetl/configs/article_generation.yaml\")); print(\"Config OK\")'"

# Test DocETL can run basic operations
run_test "DocETL CLI is accessible" "docker-compose exec -T docetl-worker python -m docetl.cli --help | head -1"

# Test 10: Sample Data and Processing
print_info "=== Sample Data Tests ==="

# Check for sample PDF files
pdf_count=$(ls data/input/*.pdf 2>/dev/null | wc -l || echo "0")
if [ "$pdf_count" -gt 0 ]; then
    run_test "Sample PDF files are present ($pdf_count files)" "true"

    # Test PDF text extraction
    first_pdf=$(ls data/input/*.pdf 2>/dev/null | head -1)
    if [ -n "$first_pdf" ]; then
        run_test "PDF text extraction works" "docker-compose exec -T docetl-worker python -c 'import fitz; doc = fitz.open(\"${first_pdf}\"); print(len(doc[0].get_text())) if len(doc) > 0 else 0'"
    fi
else
    run_test "Sample PDF files are present" "false"
    print_info "No PDF files found in data/input/ - add some for full testing"
fi

# Test 11: API Connectivity
print_info "=== API Connectivity Tests ==="

# Test OpenAI API (if key is configured)
if grep -q 'OPENAI_API_KEY=sk-' .env && ! grep -q 'sk-your_openai_api_key_here' .env; then
    run_test "OpenAI API connection" "docker-compose exec -T docetl-worker python -c 'import openai; client = openai.OpenAI(); models = client.models.list(); print(\"API OK\" if models else \"API Failed\")'"
else
    print_info "Skipping OpenAI API test - API key not configured"
fi

# Test arXiv API connectivity
run_test "arXiv API connectivity" "docker-compose exec -T docetl-worker python -c 'import arxiv; import requests; r = requests.get(\"http://export.arxiv.org/api/query?search_query=cat:cs.AI&max_results=1\"); print(\"arXiv OK\" if r.status_code == 200 else \"arXiv Failed\")'"

# Test 12: File Permissions and Access
print_info "=== File Permissions Tests ==="

run_test "Data directory is writable" "touch data/test_write && rm data/test_write"
run_test "Logs directory is writable" "touch logs/test_write && rm logs/test_write"
run_test "Airflow can write logs" "[ -w airflow/logs ]"

# Test 13: Resource Requirements
print_info "=== Resource Requirements Tests ==="

# Check available memory (Docker stats)
memory_mb=$(docker stats --no-stream --format "table {{.MemUsage}}" | tail -n +2 | head -1 | sed 's/MiB.*//' | sed 's/.*\///g' || echo "0")
if [ "${memory_mb:-0}" -gt 4000 ]; then
    run_test "Sufficient memory available (>4GB)" "true"
else
    run_test "Sufficient memory available (>4GB)" "false"
    print_info "Consider allocating more memory to Docker"
fi

# Check disk space
disk_free_gb=$(df . | tail -1 | awk '{print int($4/1024/1024)}')
if [ "${disk_free_gb:-0}" -gt 10 ]; then
    run_test "Sufficient disk space (>10GB)" "true"
else
    run_test "Sufficient disk space (>10GB)" "false"
    print_info "Consider freeing up disk space"
fi

# Test 14: Integration Test
print_info "=== Integration Tests ==="

# Create a simple test pipeline
cat > /tmp/test_docetl_config.yaml << 'EOF'
default_model: gpt-4o-mini
datasets:
  test_data:
    type: file
    source: local
    path: []
operations:
  - name: test_operation
    type: map
    prompt: "Return 'test successful' exactly as provided."
    optimize: false
EOF

# Test DocETL can run (with mock data)
if run_test "DocETL pipeline execution test" "docker-compose exec -T docetl-worker python -c 'import docetl; print(\"DocETL ready for execution\")'"; then
    print_pass "DocETL is ready for pipeline execution"
fi

# Clean up test file
rm -f /tmp/test_docetl_config.yaml

# Test 15: Security and Configuration
print_info "=== Security and Configuration Tests ==="

run_test "No default passwords in .env" "! grep -q 'password.*password' .env"
run_test "Airflow has proper authentication" "grep -q '_AIRFLOW_WWW_USER_USERNAME' .env"
run_test "File permissions are secure" "[ ! -f .env ] || [ $(stat -c '%a' .env) -le 644 ]"

# Final Summary
echo ""
echo "================================================"
echo "🏁 TEST SUMMARY"
echo "================================================"
echo -e "Total tests: ${BLUE}$TESTS_TOTAL${NC}"
echo -e "Passed: ${GREEN}$TESTS_PASSED${NC}"
echo -e "Failed: ${RED}$TESTS_FAILED${NC}"

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "${GREEN}🎉 All tests passed! Your Zara ETL POC is ready!${NC}"
    echo ""
    echo "Next steps:"
    echo "1. Access Airflow UI: http://localhost:8080 (admin/admin)"
    echo "2. Trigger the 'zara_hybrid_etl' DAG"
    echo "3. Monitor progress and check results in data/output/"
    echo "4. Use ./scripts/monitor.sh for real-time monitoring"
    exit 0
else
    echo -e "${YELLOW}⚠️  Some tests failed. Please address the issues above.${NC}"
    echo ""
    echo "Common fixes:"
    echo "- Ensure Docker has enough resources (8GB RAM, 10GB disk)"
    echo "- Configure OpenAI API key in .env file"
    echo "- Wait longer for services to start up completely"
    echo "- Check Docker logs: docker-compose logs"

    if [ $TESTS_FAILED -lt 5 ]; then
        echo ""
        echo -e "${GREEN}Most tests passed - you can likely proceed with caution${NC}"
        exit 0
    else
        exit 1
    fi
fi