#!/bin/bash

# Monitoring script for Zara ETL Pipeline
# =======================================
# Real-time monitoring dashboard for the entire pipeline

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}=== $1 ===${NC}"
}

print_info() {
    echo -e "${GREEN}$1${NC}"
}

print_warning() {
    echo -e "${YELLOW}$1${NC}"
}

print_error() {
    echo -e "${RED}$1${NC}"
}

print_status() {
    echo -e "${CYAN}$1${NC}"
}

# Function to get service status with color coding
get_service_status() {
    local service=$1
    local status=$(docker-compose ps $service 2>/dev/null | tail -n +3 | awk '{print $4}')

    if [[ "$status" == "Up" ]]; then
        echo -e "${GREEN}●${NC} Running"
    elif [[ "$status" == "Exit"* ]]; then
        echo -e "${RED}●${NC} Stopped"
    else
        echo -e "${YELLOW}●${NC} Starting"
    fi
}

# Function to get container resource usage
get_resource_usage() {
    local container=$1
    docker stats --no-stream --format "{{.CPUPerc}} {{.MemUsage}}" $container 2>/dev/null | head -1
}

# Function to get recent log entries
get_recent_logs() {
    local service=$1
    local lines=${2:-5}
    docker-compose logs --tail=$lines $service 2>/dev/null | tail -$lines
}

# Function to check DAG status
get_dag_status() {
    local dag_status=$(docker-compose exec -T airflow-webserver airflow dags state zara_hybrid_etl 2>/dev/null || echo "unknown")
    echo "$dag_status"
}

# Function to get latest DAG run info
get_latest_dag_run() {
    docker-compose exec -T airflow-webserver airflow dags list-runs -d zara_hybrid_etl --limit 1 2>/dev/null | tail -1
}

# Function to count files in directories
count_files() {
    local dir=$1
    local pattern=${2:-"*"}
    local count=$(find "$dir" -name "$pattern" -type f 2>/dev/null | wc -l)
    echo "$count"
}

# Function to get disk usage
get_disk_usage() {
    local dir=$1
    du -sh "$dir" 2>/dev/null | cut -f1 || echo "N/A"
}

# Function to check API connectivity
check_openai_api() {
    if docker-compose exec -T docetl-worker python -c "import openai; import os; client = openai.OpenAI(); models = list(client.models.list()); print('✅ Connected' if models else '❌ Failed')" 2>/dev/null; then
        echo "✅ Connected"
    else
        echo "❌ Failed"
    fi
}

# Function to display pipeline metrics
show_pipeline_metrics() {
    local latest_summary=$(ls -t data/output/pipeline_summary_*.json 2>/dev/null | head -1)

    if [ -n "$latest_summary" ]; then
        echo "Latest Pipeline Run: $(basename "$latest_summary")"
        echo "Summary:"
        if command -v jq >/dev/null 2>&1; then
            cat "$latest_summary" | jq -r '
                "Papers Processed: " + (.pipeline_run.total_papers_processed // "N/A" | tostring) +
                "\nArticles Generated: " + (.pipeline_run.articles_generated // "N/A" | tostring) +
                "\nQuality Pass Rate: " + ((.pipeline_run.quality_stats.pass_rate // 0) * 100 | tostring | .[0:4]) + "%" +
                "\nAvg Quality Score: " + ((.pipeline_run.quality_stats.average_quality_score // 0) | tostring | .[0:4])
            ' 2>/dev/null
        else
            echo "Install 'jq' for detailed pipeline metrics"
            head -10 "$latest_summary"
        fi
    else
        echo "No pipeline runs completed yet"
    fi
}

# Function to show active tasks
show_active_tasks() {
    local active_tasks=$(docker-compose exec -T airflow-webserver airflow tasks states-for-dag-run zara_hybrid_etl 2>/dev/null | grep -E "(running|queued|up_for_retry)" | wc -l)

    if [ "$active_tasks" -gt 0 ]; then
        echo -e "${YELLOW}$active_tasks${NC} tasks active"
        docker-compose exec -T airflow-webserver airflow tasks states-for-dag-run zara_hybrid_etl 2>/dev/null | grep -E "(running|queued|up_for_retry)" | head -5
    else
        echo -e "${GREEN}No active tasks${NC}"
    fi
}

# Main monitoring loop
main_monitor() {
    while true; do
        clear
        echo -e "${MAGENTA}📊 Zara ETL Pipeline Monitor - $(date)${NC}"
        echo "========================================================================"

        # System Overview
        print_header "System Status"
        printf "%-20s %s\n" "Postgres:" "$(get_service_status postgres)"
        printf "%-20s %s\n" "Redis:" "$(get_service_status redis)"
        printf "%-20s %s\n" "Airflow Webserver:" "$(get_service_status airflow-webserver)"
        printf "%-20s %s\n" "Airflow Scheduler:" "$(get_service_status airflow-scheduler)"
        printf "%-20s %s\n" "DocETL Worker:" "$(get_service_status docetl-worker)"
        echo ""

        # Resource Usage
        print_header "Resource Usage"
        echo "Container Resource Usage (CPU% / Memory):"

        services=("postgres" "redis" "zara-etl-poc-airflow-webserver-1" "zara-etl-poc-airflow-scheduler-1" "zara-etl-poc-docetl-worker-1")
        for service in "${services[@]}"; do
            local usage=$(get_resource_usage "$service")
            if [ -n "$usage" ]; then
                printf "%-25s %s\n" "$(echo $service | cut -d'-' -f4- | tr '-' ' '):" "$usage"
            fi
        done

        echo ""
        echo "Docker System Usage:"
        docker system df --format "table {{.Type}}\t{{.TotalCount}}\t{{.Size}}\t{{.Reclaimable}}" 2>/dev/null
        echo ""

        # Pipeline Status
        print_header "Pipeline Status"

        # DAG status
        local dag_state=$(get_dag_status)
        printf "%-20s %s\n" "DAG Status:" "$dag_state"

        # Latest run info
        echo "Latest DAG Run:"
        get_latest_dag_run | head -1
        echo ""

        # Active tasks
        echo "Active Tasks:"
        show_active_tasks
        echo ""

        # API Connectivity
        print_header "API Status"
        printf "%-20s %s\n" "OpenAI API:" "$(check_openai_api)"

        # Test arXiv connectivity
        if curl -s --max-time 3 "http://export.arxiv.org/api/query?search_query=cat:cs.AI&max_results=1" >/dev/null 2>&1; then
            printf "%-20s %s\n" "arXiv API:" "✅ Connected"
        else
            printf "%-20s %s\n" "arXiv API:" "❌ Failed"
        fi
        echo ""

        # Data Status
        print_header "Data Directory Status"
        printf "%-25s %s files (%s)\n" "Input Papers:" "$(count_files data/input "*.pdf")" "$(get_disk_usage data/input)"
        printf "%-25s %s files (%s)\n" "Processed Data:" "$(count_files data/processed)" "$(get_disk_usage data/processed)"
        printf "%-25s %s files (%s)\n" "Output Articles:" "$(count_files data/output)" "$(get_disk_usage data/output)"
        printf "%-25s %s files (%s)\n" "Error Logs:" "$(count_files data/errors 2>/dev/null || echo 0)" "$(get_disk_usage data/errors 2>/dev/null || echo 'N/A')"
        echo ""

        # Recent Pipeline Results
        print_header "Latest Pipeline Results"
        show_pipeline_metrics
        echo ""

        # Recent Activity Logs
        print_header "Recent Activity (last 5 lines)"
        echo -e "${CYAN}Airflow Scheduler:${NC}"
        get_recent_logs airflow-scheduler 3 | cut -c1-100
        echo ""
        echo -e "${CYAN}DocETL Worker:${NC}"
        get_recent_logs docetl-worker 3 | cut -c1-100
        echo ""

        # Quick Actions
        print_header "Quick Actions"
        echo "Access Points:"
        echo "• Airflow UI: http://localhost:8080 (admin/admin)"
        echo "• View logs: docker-compose logs -f [service]"
        echo "• Restart service: docker-compose restart [service]"
        echo ""
        echo "Useful Commands:"
        echo "• Test pipeline: ./scripts/test_pipeline.sh"
        echo "• View article: cat data/output/articles_high_quality_*.json | jq '.[0]'"
        echo "• Check errors: ls -la data/errors/"
        echo ""

        # Health Indicators
        local health_score=0
        local total_checks=5

        # Check service health
        if docker-compose ps postgres | grep -q "Up"; then health_score=$((health_score + 1)); fi
        if docker-compose ps airflow-webserver | grep -q "Up"; then health_score=$((health_score + 1)); fi
        if docker-compose ps airflow-scheduler | grep -q "Up"; then health_score=$((health_score + 1)); fi
        if docker-compose ps docetl-worker | grep -q "Up"; then health_score=$((health_score + 1)); fi
        if curl -s http://localhost:8080/health >/dev/null 2>&1; then health_score=$((health_score + 1)); fi

        local health_percentage=$(( (health_score * 100) / total_checks ))

        if [ $health_percentage -ge 80 ]; then
            print_info "🟢 System Health: $health_percentage% ($health_score/$total_checks checks passing)"
        elif [ $health_percentage -ge 60 ]; then
            print_warning "🟡 System Health: $health_percentage% ($health_score/$total_checks checks passing)"
        else
            print_error "🔴 System Health: $health_percentage% ($health_score/$total_checks checks passing)"
        fi

        echo ""
        echo "========================================================================"
        echo "Press Ctrl+C to exit, 'r' + Enter to refresh now, or wait 30 seconds..."

        # Wait for input with timeout
        read -t 30 -n 1 input
        if [[ $input == "r" ]]; then
            continue
        fi
    done
}

# Interactive mode function
interactive_mode() {
    echo "🎛️  Interactive Monitoring Mode"
    echo "Available commands:"
    echo "  1) Service status"
    echo "  2) View logs"
    echo "  3) Resource usage"
    echo "  4) Pipeline status"
    echo "  5) Test connectivity"
    echo "  6) View recent articles"
    echo "  7) Start monitoring loop"
    echo "  q) Quit"

    while true; do
        echo ""
        read -p "Enter command (1-7, q): " choice

        case $choice in
            1)
                print_header "Service Status"
                docker-compose ps
                ;;
            2)
                echo "Available services: postgres, redis, airflow-webserver, airflow-scheduler, docetl-worker"
                read -p "Which service logs to view? " service
                echo "Recent logs for $service:"
                docker-compose logs --tail=20 "$service"
                ;;
            3)
                print_header "Resource Usage"
                docker stats --no-stream
                ;;
            4)
                print_header "Pipeline Status"
                show_active_tasks
                echo ""
                show_pipeline_metrics
                ;;
            5)
                print_header "Connectivity Tests"
                echo "OpenAI API: $(check_openai_api)"
                curl -s --max-time 5 "http://export.arxiv.org/api/query?search_query=cat:cs.AI&max_results=1" >/dev/null && echo "arXiv API: ✅ Connected" || echo "arXiv API: ❌ Failed"
                curl -s http://localhost:8080/health >/dev/null && echo "Airflow UI: ✅ Available" || echo "Airflow UI: ❌ Unavailable"
                ;;
            6)
                print_header "Recent Articles"
                latest_article=$(ls -t data/output/articles_high_quality_*.json 2>/dev/null | head -1)
                if [ -n "$latest_article" ]; then
                    echo "Latest articles file: $(basename "$latest_article")"
                    if command -v jq >/dev/null 2>&1; then
                        cat "$latest_article" | jq -r '.[0] | "Title: " + .headline + "\nQuality Score: " + (.quality_score | tostring) + "\nWord Count: " + (.word_count // "N/A" | tostring)'
                    else
                        echo "First 200 characters of first article:"
                        head -c 200 "$latest_article"
                    fi
                else
                    echo "No articles generated yet"
                fi
                ;;
            7)
                main_monitor
                ;;
            q)
                echo "Goodbye!"
                exit 0
                ;;
            *)
                echo "Invalid choice. Please enter 1-7 or q."
                ;;
        esac
    done
}

# Command line argument handling
case "${1:-}" in
    --interactive|-i)
        interactive_mode
        ;;
    --once|-o)
        # Run once and exit
        clear
        main_monitor() {
            echo -e "${MAGENTA}📊 Zara ETL Pipeline Status - $(date)${NC}"
            echo "========================================================================"

            print_header "System Status"
            docker-compose ps
            echo ""

            print_header "Pipeline Status"
            show_active_tasks
            echo ""
            show_pipeline_metrics
            echo ""

            print_header "Data Status"
            printf "%-25s %s files\n" "Input Papers:" "$(count_files data/input "*.pdf")"
            printf "%-25s %s files\n" "Output Articles:" "$(count_files data/output)"

            echo ""
            echo "For continuous monitoring, run: ./scripts/monitor.sh"
        }
        main_monitor
        ;;
    --help|-h)
        echo "Zara ETL Pipeline Monitor"
        echo ""
        echo "Usage: $0 [options]"
        echo ""
        echo "Options:"
        echo "  (no args)         Start continuous monitoring (default)"
        echo "  -i, --interactive Interactive mode with menu"
        echo "  -o, --once        Show status once and exit"
        echo "  -h, --help        Show this help message"
        echo ""
        echo "Examples:"
        echo "  $0                # Start continuous monitoring"
        echo "  $0 --interactive  # Interactive menu mode"
        echo "  $0 --once         # One-time status check"
        ;;
    *)
        # Default: continuous monitoring
        echo "🚀 Starting Zara ETL Pipeline Monitor..."
        echo "Press Ctrl+C to exit"
        sleep 2
        main_monitor
        ;;
esac