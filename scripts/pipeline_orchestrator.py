"""
Complete Pipeline Orchestrator - Runs entire ETL pipeline
"""
import subprocess
import logging
import json
import os
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline_orchestrator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PipelineOrchestrator:
    def __init__(self):
        self.execution_id = f"PIPE_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.start_time = datetime.now()
        self.steps_results = {}
    
    def run_step(self, step_name, script_path):
        """Execute a pipeline step"""
        logger.info(f"\n{'='*50}")
        logger.info(f"Running: {step_name}")
        logger.info(f"{'='*50}")
        
        try:
            result = subprocess.run(
                ['python', script_path],
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode == 0:
                logger.info(f"✓ {step_name} completed successfully")
                self.steps_results[step_name] = {'status': 'success', 'duration_seconds': 0}
                return True
            else:
                logger.error(f"✗ {step_name} failed")
                logger.error(result.stderr)
                self.steps_results[step_name] = {'status': 'failed', 'error': result.stderr}
                return False
        except Exception as e:
            logger.error(f"✗ {step_name} error: {str(e)}")
            self.steps_results[step_name] = {'status': 'failed', 'error': str(e)}
            return False
    
    def execute(self):
        """Execute complete pipeline"""
        logger.info(f"Pipeline Execution ID: {self.execution_id}")
        
        pipeline_steps = [
            ("Data Generation", "scripts/data_generation/generate_data.py"),
            ("Data Ingestion", "scripts/ingestion/ingest_to_staging.py"),
            ("Data Quality Checks", "scripts/quality_checks/validate_data.py"),
            ("Staging to Production", "scripts/transformation/staging_to_production.py"),
            ("Warehouse Load", "scripts/transformation/load_warehouse.py"),
        ]
        
        for step_name, script_path in pipeline_steps:
            success = self.run_step(step_name, script_path)
            if not success:
                logger.error(f"Pipeline stopped at {step_name}")
                break
        
        self.generate_report()
        return all(r['status'] == 'success' for r in self.steps_results.values())
    
    def generate_report(self):
        """Generate execution report"""
        os.makedirs('data/processed', exist_ok=True)
        
        report = {
            'pipeline_execution_id': self.execution_id,
            'start_time': self.start_time.isoformat(),
            'end_time': datetime.now().isoformat(),
            'total_duration_seconds': (datetime.now() - self.start_time).total_seconds(),
            'status': 'success' if all(r['status'] == 'success' for r in self.steps_results.values()) else 'failed',
            'steps_executed': self.steps_results
        }
        
        with open('data/processed/pipeline_execution_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"\n{'='*50}")
        logger.info(f"Pipeline {'COMPLETED' if report['status'] == 'success' else 'FAILED'}")
        logger.info(f"Report saved to data/processed/pipeline_execution_report.json")
        logger.info(f"{'='*50}\n")

def main():
    orchestrator = PipelineOrchestrator()
    success = orchestrator.execute()
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())
