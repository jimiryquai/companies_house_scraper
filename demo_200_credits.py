#!/usr/bin/env python3
"""Demonstration Script for 200 Credit Usage.

This script demonstrates the Snov.io integration by processing a small batch
of companies to show the complete workflow and credit consumption.

Usage:
    python demo_200_credits.py --max-companies 10
"""

import asyncio
import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime
from typing import Any, Optional

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from enrichment.credit_aware_workflow import CreditAwareWorkflowCoordinator
from enrichment.credit_manager import CreditManager
from enrichment.enrichment_state_manager import EnrichmentStateManager
from enrichment.snov_client import SnovioClient
from streaming.queue_manager import PriorityQueueManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
DB_PATH = "companies.db"


class CreditDemonstration:
    """Demonstrates credit usage with real companies."""
    
    def __init__(self):
        """Initialize demonstration components."""
        self.snov_client = SnovioClient()
        self.credit_manager = CreditManager()
        self.queue_manager = PriorityQueueManager()
        self.state_manager = EnrichmentStateManager()
        self.workflow_coordinator = CreditAwareWorkflowCoordinator(
            credit_manager=self.credit_manager,
            snov_client=self.snov_client,
            queue_manager=self.queue_manager,
            enrichment_state_manager=self.state_manager,
        )
        
        self.results = {
            "started_at": datetime.now().isoformat(),
            "initial_balance": 0,
            "final_balance": 0,
            "total_credits_consumed": 0,
            "companies_processed": [],
            "domains_found": 0,
            "emails_found": 0,
            "errors": [],
        }
    
    async def get_strike_off_companies(self, limit: int = 10) -> list[dict[str, str]]:
        """Get strike-off companies from database.
        
        Args:
            limit: Maximum number of companies to fetch
            
        Returns:
            List of companies with number and name
        """
        try:
            with sqlite3.connect(DB_PATH) as conn:
                cursor = conn.cursor()
                
                # Get companies that haven't been processed yet
                cursor.execute("""
                    SELECT c.company_number, c.company_name
                    FROM companies c
                    WHERE c.company_status_detail LIKE '%proposal to strike off%'
                    AND NOT EXISTS (
                        SELECT 1 FROM company_domains d 
                        WHERE d.company_number = c.company_number
                    )
                    ORDER BY RANDOM()
                    LIMIT ?
                """, (limit,))
                
                companies = []
                for row in cursor.fetchall():
                    companies.append({
                        "company_number": row[0],
                        "company_name": row[1],
                    })
                
                logger.info(f"Found {len(companies)} unprocessed strike-off companies")
                return companies
                
        except Exception as e:
            logger.error(f"Failed to get companies: {e}")
            return []
    
    async def process_company(self, company: dict[str, str]) -> dict[str, Any]:
        """Process a single company through the workflow.
        
        Args:
            company: Company data with number and name
            
        Returns:
            Processing result
        """
        company_number = company["company_number"]
        company_name = company["company_name"]
        
        logger.info(f"Processing: {company_name} ({company_number})")
        
        try:
            # Execute workflow
            result = await self.workflow_coordinator.execute_workflow(
                company_number=company_number,
                company_name=company_name,
                company_data={"status": "active-proposal-to-strike-off"},
            )
            
            # Track results
            company_result = {
                "company_number": company_number,
                "company_name": company_name,
                "status": result["final_status"],
                "credits_consumed": result["total_credits_consumed"],
                "domain_found": False,
                "emails_found": 0,
            }
            
            # Check for domain
            for op in result.get("snov_operations", []):
                if op.get("operation") == "domain_search" and op.get("domain_found"):
                    company_result["domain_found"] = True
                    company_result["domain"] = op.get("domain_found")
                    self.results["domains_found"] += 1
                elif op.get("operation") == "email_search" and op.get("emails_found", 0) > 0:
                    company_result["emails_found"] = op.get("emails_found", 0)
                    self.results["emails_found"] += company_result["emails_found"]
            
            logger.info(f"  Status: {result['final_status']}")
            logger.info(f"  Credits consumed: {result['total_credits_consumed']}")
            if company_result["domain_found"]:
                logger.info(f"  Domain: {company_result.get('domain')}")
            if company_result["emails_found"] > 0:
                logger.info(f"  Emails found: {company_result['emails_found']}")
            
            return company_result
            
        except Exception as e:
            logger.error(f"  Failed to process: {e}")
            self.results["errors"].append(f"{company_number}: {str(e)}")
            return {
                "company_number": company_number,
                "company_name": company_name,
                "status": "error",
                "error": str(e),
                "credits_consumed": 0,
            }
    
    async def run_demonstration(self, max_companies: int = 10) -> dict[str, Any]:
        """Run the credit demonstration.
        
        Args:
            max_companies: Maximum companies to process
            
        Returns:
            Demonstration results
        """
        logger.info("=" * 60)
        logger.info("SNOV.IO CREDIT DEMONSTRATION")
        logger.info("=" * 60)
        
        # Check initial balance
        try:
            initial_balance = await self.credit_manager.check_available_credits()
            self.results["initial_balance"] = initial_balance
            logger.info(f"Initial credit balance: {initial_balance}")
        except Exception as e:
            logger.error(f"Failed to check initial balance: {e}")
            self.results["initial_balance"] = "Error"
        
        # Get companies to process
        companies = await self.get_strike_off_companies(max_companies)
        if not companies:
            logger.error("No companies to process")
            return self.results
        
        logger.info(f"Processing {len(companies)} companies...")
        logger.info("-" * 60)
        
        # Process each company
        for i, company in enumerate(companies, 1):
            logger.info(f"\n[{i}/{len(companies)}] Company: {company['company_name'][:50]}...")
            
            # Check if we still have credits
            try:
                current_balance = await self.credit_manager.check_available_credits()
                if current_balance <= 0:
                    logger.warning("No credits remaining - stopping")
                    break
            except Exception:
                pass  # Continue anyway
            
            # Process company
            result = await self.process_company(company)
            self.results["companies_processed"].append(result)
            self.results["total_credits_consumed"] += result.get("credits_consumed", 0)
            
            # Small delay between companies
            if i < len(companies):
                await asyncio.sleep(2)
        
        # Check final balance
        try:
            final_balance = await self.credit_manager.check_available_credits()
            self.results["final_balance"] = final_balance
            logger.info(f"\nFinal credit balance: {final_balance}")
        except Exception as e:
            logger.error(f"Failed to check final balance: {e}")
            self.results["final_balance"] = "Error"
        
        self.results["completed_at"] = datetime.now().isoformat()
        return self.results
    
    def print_summary(self) -> None:
        """Print demonstration summary."""
        logger.info("")
        logger.info("=" * 60)
        logger.info("DEMONSTRATION SUMMARY")
        logger.info("=" * 60)
        
        logger.info(f"Companies Processed: {len(self.results['companies_processed'])}")
        logger.info(f"Domains Found: {self.results['domains_found']}")
        logger.info(f"Emails Found: {self.results['emails_found']}")
        logger.info(f"Total Credits Consumed: {self.results['total_credits_consumed']}")
        
        if isinstance(self.results["initial_balance"], int) and isinstance(self.results["final_balance"], int):
            actual_consumption = self.results["initial_balance"] - self.results["final_balance"]
            logger.info(f"Actual Credit Change: {actual_consumption}")
        
        # Success rate
        successful = sum(
            1 for c in self.results["companies_processed"] 
            if c.get("status") in ["completed", "no_domain_found", "no_officers_found"]
        )
        if self.results["companies_processed"]:
            success_rate = successful / len(self.results["companies_processed"]) * 100
            logger.info(f"Success Rate: {success_rate:.1f}%")
        
        # Breakdown by status
        logger.info("\nStatus Breakdown:")
        status_counts = {}
        for company in self.results["companies_processed"]:
            status = company.get("status", "unknown")
            status_counts[status] = status_counts.get(status, 0) + 1
        
        for status, count in sorted(status_counts.items()):
            logger.info(f"  {status}: {count}")
        
        # Save detailed results
        results_file = f"demo_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_file, "w") as f:
            json.dump(self.results, f, indent=2)
        logger.info(f"\nDetailed results saved to: {results_file}")
        
        # Show some successful examples
        logger.info("\nExample Results:")
        shown = 0
        for company in self.results["companies_processed"]:
            if company.get("domain_found") or company.get("emails_found", 0) > 0:
                logger.info(f"  {company['company_name'][:40]}...")
                if company.get("domain"):
                    logger.info(f"    Domain: {company['domain']}")
                if company.get("emails_found", 0) > 0:
                    logger.info(f"    Emails: {company['emails_found']}")
                shown += 1
                if shown >= 3:
                    break


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Demonstrate Snov.io integration with 200 credits"
    )
    parser.add_argument(
        "--max-companies",
        type=int,
        default=10,
        help="Maximum number of companies to process (default: 10)",
    )
    parser.add_argument(
        "--no-confirm",
        action="store_true",
        help="Skip confirmation prompt",
    )
    
    args = parser.parse_args()
    
    if not args.no_confirm:
        logger.warning("=" * 60)
        logger.warning("CREDIT USAGE DEMONSTRATION")
        logger.warning(f"This will process up to {args.max_companies} companies")
        logger.warning("This WILL consume Snov.io credits!")
        logger.warning("=" * 60)
        response = input("Continue? (yes/no): ")
        if response.lower() != "yes":
            logger.info("Demonstration cancelled.")
            return
    
    # Run demonstration
    demo = CreditDemonstration()
    await demo.run_demonstration(max_companies=args.max_companies)
    demo.print_summary()


if __name__ == "__main__":
    asyncio.run(main())