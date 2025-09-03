# 🚀 Zara ETL POC - Quick Start Guide

Get the hybrid Airflow + DocETL pipeline running in under 10 minutes!

## ⚡ Super Quick Start (5 commands)

```bash
# 1. Setup environment
./scripts/setup.sh

# 2. Add your OpenAI API key to .env file
nano .env  # or vim .env

# 3. Start all services
docker-compose up -d

# 4. Wait for services to start (2-3 minutes), then test
./scripts/test_pipeline.sh

# 5. Access Airflow and trigger the DAG
open http://localhost:8080  # admin/admin
```

## 📋 Prerequisites Checklist

- [ ] Docker installed and running
- [ ] Docker Compose installed
- [ ] 8GB+ RAM available
- [ ] 10GB+ free disk space
- [ ] OpenAI API key ([get here](https://platform.openai.com/api-keys))

## 🎯 What Happens Next

1. **Services Start**: Airflow, PostgreSQL, Redis, and DocETL containers
2. **Database Init**: Airflow database gets initialized automatically
3. **UI Available**: Airflow web interface at http://localhost:8080
4. **Pipeline Ready**: `zara_hybrid_etl` DAG appears in Airflow
5. **Trigger & Watch**: Click "Trigger DAG" and monitor progress

## 📊 Expected Results

After triggering the pipeline, you'll get:

- ✅ **10 arXiv papers** downloaded from cs.AI, cs.CL, cs.LG categories
- ✅ **Generated articles** in `data/output/articles_high_quality_YYYYMMDD_HHMMSS.json`
- ✅ **Quality scores** for each article (0-1 scale)
- ✅ **Processing summary** with statistics and performance metrics

## 🔧 Configuration Options

Edit `.env` file to customize:

```bash
# Number of papers to process
ARXIV_MAX_RESULTS=10

# arXiv categories to search
ARXIV_CATEGORIES=cs.AI,cs.CL,cs.LG,cs.CV

# Processing batch size
BATCH_SIZE=5

# Quality threshold (0-1)
QUALITY_THRESHOLD=0.7

# LLM model to use
DEFAULT_MODEL=gpt-4o-mini
```

## 📈 Monitoring & Debugging

### Real-time Monitoring
```bash
./scripts/monitor.sh  # Live dashboard with status and logs
```

### Check Service Status
```bash
docker-compose ps  # Service status
docker-compose logs -f  # Live logs
```

### Debug Individual Components

**Test DocETL directly:**
```bash
docker-compose exec docetl-worker python -c "import docetl; print('OK')"
```

**Check Airflow connection:**
```bash
curl http://localhost:8080/health
```

**Inspect generated articles:**
```bash
cat data/output/articles_high_quality_*.json | jq '.[0]'  # First article
```

## 🐛 Common Issues & Solutions

### Services Won't Start
```bash
# Check Docker resources
docker system df
docker system prune  # If needed

# Check port conflicts
lsof -i :8080  # Airflow
lsof -i :5432  # PostgreSQL
```

### API Key Issues
```bash
# Verify API key is set
grep OPENAI_API_KEY .env

# Test API access
docker-compose exec docetl-worker python -c "
import openai
client = openai.OpenAI()
print('API Key works!')
"
```

### Pipeline Fails
```bash
# Check Airflow logs
# Go to: http://localhost:8080 -> DAGs -> zara_hybrid_etl -> Graph View
# Click on failed task -> Logs tab

# Check detailed error logs
ls data/errors/  # Error files with timestamps
```

### Low Quality Scores
```bash
# Review failed articles
cat data/output/articles_failed_quality_*.json | jq '.[0]'

# Adjust quality threshold
# Edit .env: QUALITY_THRESHOLD=0.6
docker-compose restart
```

## 📁 Output Structure

```
data/output/
├── articles_high_quality_20250102_143022.json  # Ready to publish
├── articles_failed_quality_20250102_143022.json  # Need improvement  
└── pipeline_summary_20250102_143022.json  # Execution stats
```

## 🎨 Sample Article Output

```json
{
  "headline": "AI Model Achieves 95% Accuracy in Medical Diagnosis",
  "subtitle": "New deep learning approach outperforms doctors in detecting rare diseases",
  "article_body": "Researchers have developed...",
  "pull_quotes": [
    "This could revolutionize early disease detection",
    "The model identified patterns invisible to human doctors"
  ],
  "key_takeaways": [
    "95% accuracy vs 78% for human doctors",
    "Works on X-ray, MRI, and CT scan images", 
    "Could be deployed in hospitals within 2 years"
  ],
  "quality_score": 0.87
}
```

## ⏱️ Performance Expectations

| Metric | Expected Value |
|--------|----------------|
| Papers Processed | 10 papers |
| Processing Time | 30-45 minutes |
| Articles Generated | 7-8 articles |
| Quality Pass Rate | 70-80% |
| Cost per Article | $0.50-$2.00 |

## 🚀 What's Next?

After successful POC:

1. **Scale Up**: Increase `ARXIV_MAX_RESULTS` to 100+
2. **Add Models**: Include Anthropic Claude, local models
3. **Enhance Quality**: Fine-tune scoring thresholds
4. **Add Features**: Image generation, CMS integration
5. **Production Deploy**: Move to cloud infrastructure

## 🆘 Need Help?

1. **Check Logs**: `./scripts/monitor.sh`
2. **Run Tests**: `./scripts/test_pipeline.sh`
3. **Restart Services**: `docker-compose restart`
4. **Full Reset**: `docker-compose down && docker-compose up -d`

## 🎉 Success Criteria

Your POC is successful when you see:

- ✅ All services running (`docker-compose ps`)
- ✅ Airflow UI accessible (http://localhost:8080)
- ✅ Pipeline completes without errors
- ✅ Articles generated in `data/output/`
- ✅ Quality scores above threshold

**Ready to impress your client!** 🌟