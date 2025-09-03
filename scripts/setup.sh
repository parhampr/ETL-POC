#!/bin/bash

# Zara ETL POC Setup Script (Optimized)
# =====================================
# Streamlined setup for the Zara ETL POC

set -e

echo "🚀 Starting Zara ETL POC Setup..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    print_info "Checking prerequisites..."

    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi

    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi

    # Check if Docker is running
    if ! docker info &> /dev/null; then
        print_error "Docker daemon is not running. Please start Docker first."
        exit 1
    fi

    print_status "Prerequisites check passed"
}

# Create directory structure
create_directories() {
    print_info "Creating directory structure..."

    directories=(
        "data/input"
        "data/processed"
        "data/output"
        "data/errors"
        "logs/airflow"
        "logs/docetl"
        "airflow/logs"
    )

    for dir in "${directories[@]}"; do
        mkdir -p "$dir"
        print_status "Created directory: $dir"
    done
}

# Setup environment file
setup_environment() {
    print_info "Setting up environment configuration..."

    if [ ! -f ".env" ]; then
        print_error ".env file not found. Please create .env file manually."
        print_info "You can copy from .env.example if available."
        exit 1
    else
        print_status ".env file already exists"
    fi

    # Check if OpenAI API key is set
    if grep -q "OPENAI_API_KEY=sk-your_openai_api_key_here" .env 2>/dev/null; then
        print_warning "OpenAI API key not set in .env file"
        print_info "Please edit .env file and set your OpenAI API key"
    elif grep -q "OPENAI_API_KEY=sk-" .env 2>/dev/null; then
        print_status "OpenAI API key appears to be configured"
    else
        print_warning "Please ensure OPENAI_API_KEY is set in .env file"
    fi
}

# Set proper permissions
set_permissions() {
    print_info "Setting file permissions..."

    # Set Airflow UID (required for Airflow Docker)
    export AIRFLOW_UID=$(id -u)
    
    # Add AIRFLOW_UID to .env if not present
    if ! grep -q "AIRFLOW_UID=" .env 2>/dev/null; then
        echo "AIRFLOW_UID=$AIRFLOW_UID" >> .env
    fi

    # Make scripts executable
    find scripts/ -name "*.sh" -exec chmod +x {} \; 2>/dev/null || true

    # Set data directory permissions
    chmod -R 755 data/ logs/ 2>/dev/null || true

    print_status "File permissions set"
}

# Validate setup
validate_setup() {
    print_info "Validating setup..."

    # Check required files
    required_files=(
        "docker-compose.yaml"
        "Dockerfile.airflow"
        "Dockerfile.docetl"
        "requirements.txt"
        ".env"
        "airflow/dags/zara_hybrid_etl.py"
        "docetl/config/article_pipeline.yaml.j2"
    )

    missing_files=()
    for file in "${required_files[@]}"; do
        if [ ! -f "$file" ]; then
            missing_files+=("$file")
        fi
    done

    if [ ${#missing_files[@]} -eq 0 ]; then
        print_status "All required files are present"
    else
        print_error "Missing required files:"
        for file in "${missing_files[@]}"; do
            echo "  - $file"
        done
        exit 1
    fi

    # Check directory structure
    required_dirs=(
        "data/input"
        "data/processed"
        "data/output"
        "logs/airflow"
        "logs/docetl"
    )

    for dir in "${required_dirs[@]}"; do
        if [ ! -d "$dir" ]; then
            print_error "Missing directory: $dir"
            exit 1
        fi
    done

    print_status "Directory structure is valid"
}

# Main setup function
main() {
    echo "🚀 Zara ETL POC Setup Starting..."
    echo "=================================="

    # Run all setup steps
    check_prerequisites
    create_directories
    setup_environment
    set_permissions
    validate_setup

    echo ""
    print_status "Setup completed successfully! 🎉"
    echo ""
    echo "Next steps:"
    echo "1. Ensure your OpenAI API key is set in .env file"
    echo "2. Build and start services: docker-compose up --build -d"
    echo "3. Wait 2-3 minutes for services to initialize"
    echo "4. Test the setup: ./scripts/test_pipeline.sh"
    echo "5. Access Airflow UI: http://localhost:8080 (admin/admin)"
    echo "6. Add some PDF files to data/input/ directory"
    echo "7. Trigger the 'zara_hybrid_etl' DAG in Airflow UI"
    echo ""
    echo "For monitoring: ./scripts/monitor.sh"
    echo "For help: cat READM.md"
    echo ""
    print_info "Happy processing! 🔬📄"
}

# Handle script interruption
trap 'print_error "Setup interrupted by user"; exit 1' INT

# Run main function
main "$@"