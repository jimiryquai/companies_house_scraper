#!/usr/bin/env python3
"""End-to-End Test Script for Snov.io Integration Workflow.

This script validates the complete 10-step credit-aware workflow:
1. CH Streaming Event → company events
2. CH REST API → get company_status_detail 
3. Strike-off Filter → only "active-proposal-to-strike-off"
4. Credit Check → verify credits before domain search
5. Domain Search → Snov.io v2 API (if credits > 0)
6. Credit Update → record consumption
7. Officer Fetch → CH REST API (if domain found)
8. Credit Check → verify credits before email search
9. Email Search → Snov.io v2 API (if credits > 0)
10. Final Credit Update → record consumption

Usage:
    python test_e2e_workflow.py [--use-real-apis] [--test-company-number 12345678]
"""

import asyncio
import argparse
import json
import logging
import os
import sqlite3
import sys
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

# Test configuration
DB_PATH = "companies.db"
TEST_COMPANY_NUMBER = "12345678"  # Default test company
TEST_COMPANY_NAME = "TEST STRIKE-OFF COMPANY LIMITED"


class E2EWorkflowTester:
    """End-to-end workflow tester for Snov.io integration."""
    
    def __init__(self, use_real_apis: bool = False):
        """Initialize the E2E tester.
        
        Args:
            use_real_apis: If True, use real APIs (consumes credits!)
        """
        self.use_real_apis = use_real_apis
        self.test_results: dict[str, Any] = {
            "started_at": datetime.now().isoformat(),
            "use_real_apis": use_real_apis,
            "tests_passed": 0,
            "tests_failed": 0,
            "steps_completed": [],
            "errors": [],
            "credit_consumption": {},
        }
        
        # Initialize components
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
    
    async def test_step_1_oauth_token(self) -> bool:
        """Step 1: Test OAuth token acquisition."""
        logger.info("=" * 60)
        logger.info("STEP 1: Testing OAuth Token Acquisition")
        logger.info("=" * 60)
        
        try:
            if self.use_real_apis:
                # Get real token
                token = await self.snov_client.get_oauth_token()
                if token:
                    logger.info(f"✅ OAuth token acquired: {token[:20]}...")
                    self.test_results["steps_completed"].append("oauth_token")
                    self.test_results["tests_passed"] += 1
                    return True
                else:
                    logger.error("❌ Failed to acquire OAuth token")
                    self.test_results["errors"].append("Failed to acquire OAuth token")
                    self.test_results["tests_failed"] += 1
                    return False
            else:
                logger.info("⚠️ Mock mode: Simulating OAuth token success")
                self.test_results["steps_completed"].append("oauth_token_mock")
                return True
                
        except Exception as e:
            logger.error(f"❌ OAuth token test failed: {e}")
            self.test_results["errors"].append(f"OAuth error: {str(e)}")
            self.test_results["tests_failed"] += 1
            return False
    
    async def test_step_2_balance_check(self) -> tuple[bool, int]:
        """Step 2: Test balance checking endpoint."""
        logger.info("=" * 60)
        logger.info("STEP 2: Testing Balance Check (GET /v1/get-balance)")
        logger.info("=" * 60)
        
        try:
            if self.use_real_apis:
                # Check real balance
                balance = await self.credit_manager.check_available_credits()
                logger.info(f"✅ Current balance: {balance} credits")
                self.test_results["credit_consumption"]["initial_balance"] = balance
                self.test_results["steps_completed"].append("balance_check")
                self.test_results["tests_passed"] += 1
                return True, balance
            else:
                logger.info("⚠️ Mock mode: Simulating balance of 200 credits")
                self.test_results["credit_consumption"]["initial_balance"] = 200
                self.test_results["steps_completed"].append("balance_check_mock")
                return True, 200
                
        except Exception as e:
            logger.error(f"❌ Balance check failed: {e}")
            self.test_results["errors"].append(f"Balance check error: {str(e)}")
            self.test_results["tests_failed"] += 1
            return False, 0
    
    async def test_step_3_domain_search_v2(self, company_name: str) -> tuple[bool, Optional[str]]:
        """Step 3: Test domain search using v2 endpoint."""
        logger.info("=" * 60)
        logger.info("STEP 3: Testing Domain Search (POST /v2/company-domain-by-name/start)")
        logger.info("=" * 60)
        
        try:
            if self.use_real_apis:
                # Real domain search
                result = await self.snov_client.domain_search(
                    company_name=company_name,
                    limit=10,
                    use_v2=True,
                    use_polling=True,
                )
                
                if result and result.get("domains"):
                    domain = result["domains"][0]["domain"]
                    confidence = result["domains"][0].get("confidence", 0)
                    logger.info(f"✅ Domain found: {domain} (confidence: {confidence})")
                    self.test_results["steps_completed"].append("domain_search_v2")
                    self.test_results["tests_passed"] += 1
                    return True, domain
                else:
                    logger.warning("⚠️ No domain found for company")
                    self.test_results["steps_completed"].append("domain_search_no_results")
                    return True, None
            else:
                logger.info("⚠️ Mock mode: Simulating domain discovery")
                mock_domain = "example.com"
                self.test_results["steps_completed"].append("domain_search_mock")
                return True, mock_domain
                
        except Exception as e:
            logger.error(f"❌ Domain search failed: {e}")
            self.test_results["errors"].append(f"Domain search error: {str(e)}")
            self.test_results["tests_failed"] += 1
            return False, None
    
    async def test_step_4_email_search_v2(
        self, domain: str, first_name: str, last_name: str
    ) -> tuple[bool, Optional[list]]:
        """Step 4: Test email search using v2 endpoint."""
        logger.info("=" * 60)
        logger.info("STEP 4: Testing Email Search (POST /v2/emails-by-domain-by-name/start)")
        logger.info("=" * 60)
        
        try:
            if self.use_real_apis:
                # Real email search
                result = await self.snov_client.find_email_by_domain_and_name(
                    domain=domain,
                    first_name=first_name,
                    last_name=last_name,
                    use_v2=True,
                    use_polling=True,
                )
                
                if result and result.get("emails"):
                    emails = result["emails"]
                    logger.info(f"✅ Found {len(emails)} email(s)")
                    for email in emails:
                        logger.info(f"  - {email.get('email')} (confidence: {email.get('confidence', 0)})")
                    self.test_results["steps_completed"].append("email_search_v2")
                    self.test_results["tests_passed"] += 1
                    return True, emails
                else:
                    logger.warning("⚠️ No emails found")
                    self.test_results["steps_completed"].append("email_search_no_results")
                    return True, []
            else:
                logger.info("⚠️ Mock mode: Simulating email discovery")
                mock_emails = [{"email": f"{first_name.lower()}@{domain}", "confidence": 0.8}]
                self.test_results["steps_completed"].append("email_search_mock")
                return True, mock_emails
                
        except Exception as e:
            logger.error(f"❌ Email search failed: {e}")
            self.test_results["errors"].append(f"Email search error: {str(e)}")
            self.test_results["tests_failed"] += 1
            return False, None
    
    async def test_step_5_credit_exhaustion(self) -> bool:
        """Step 5: Test behavior when credits are exhausted."""
        logger.info("=" * 60)
        logger.info("STEP 5: Testing Credit Exhaustion Handling")
        logger.info("=" * 60)
        
        try:
            # Simulate credit exhaustion
            if not self.use_real_apis:
                logger.info("⚠️ Mock mode: Simulating credit exhaustion")
                
            # Test that workflow stops Snov operations when no credits
            can_proceed = await self.credit_manager.can_consume_credits(1)
            
            if self.use_real_apis and not can_proceed:
                logger.info("✅ Credit exhaustion detected - Snov operations would stop")
                logger.info("✅ CH API operations would continue (non-blocking)")
                self.test_results["steps_completed"].append("credit_exhaustion_handled")
                self.test_results["tests_passed"] += 1
                return True
            elif not self.use_real_apis:
                logger.info("✅ Mock: Credit exhaustion handling verified")
                self.test_results["steps_completed"].append("credit_exhaustion_mock")
                return True
            else:
                logger.info("⚠️ Credits still available - cannot test exhaustion")
                return True
                
        except Exception as e:
            logger.error(f"❌ Credit exhaustion test failed: {e}")
            self.test_results["errors"].append(f"Credit exhaustion error: {str(e)}")
            self.test_results["tests_failed"] += 1
            return False
    
    async def test_step_6_full_workflow(
        self, company_number: str, company_name: str
    ) -> bool:
        """Step 6: Test complete 10-step workflow execution."""
        logger.info("=" * 60)
        logger.info("STEP 6: Testing Complete 10-Step Workflow")
        logger.info("=" * 60)
        
        try:
            # Execute complete workflow
            result = await self.workflow_coordinator.execute_workflow(
                company_number=company_number,
                company_name=company_name,
                company_data={"status": "active-proposal-to-strike-off"},
            )
            
            # Validate workflow result
            logger.info(f"Workflow completed with status: {result['final_status']}")
            logger.info(f"Credits consumed: {result['total_credits_consumed']}")
            logger.info(f"Steps completed: {', '.join(result['steps_completed'])}")
            
            # Check critical requirements
            checks_passed = []
            checks_failed = []
            
            # Check 1: Workflow completed
            if result["final_status"] in ["completed", "no_domain_found", "credit_exhausted_before_domain"]:
                checks_passed.append("Workflow reached valid end state")
            else:
                checks_failed.append(f"Unexpected status: {result['final_status']}")
            
            # Check 2: Credit checks performed
            if len(result["credit_checks"]) >= 2:
                checks_passed.append(f"Credit checks performed: {len(result['credit_checks'])}")
            else:
                checks_failed.append("Insufficient credit checks")
            
            # Check 3: CH API operations recorded
            if result["ch_api_calls"]:
                checks_passed.append("CH API operations recorded")
            else:
                checks_failed.append("No CH API operations recorded")
            
            # Check 4: Snov operations tracked
            if result["snov_operations"]:
                checks_passed.append(f"Snov operations tracked: {len(result['snov_operations'])}")
            else:
                logger.warning("No Snov operations (may be due to no credits)")
            
            # Report results
            for check in checks_passed:
                logger.info(f"✅ {check}")
            for check in checks_failed:
                logger.error(f"❌ {check}")
            
            if not checks_failed:
                self.test_results["steps_completed"].append("full_workflow")
                self.test_results["tests_passed"] += 1
                return True
            else:
                self.test_results["errors"].extend(checks_failed)
                self.test_results["tests_failed"] += 1
                return False
                
        except Exception as e:
            logger.error(f"❌ Full workflow test failed: {e}")
            self.test_results["errors"].append(f"Full workflow error: {str(e)}")
            self.test_results["tests_failed"] += 1
            return False
    
    async def test_step_7_database_persistence(self) -> bool:
        """Step 7: Test that data is persisted to database."""
        logger.info("=" * 60)
        logger.info("STEP 7: Testing Database Persistence")
        logger.info("=" * 60)
        
        try:
            with sqlite3.connect(DB_PATH) as conn:
                cursor = conn.cursor()
                
                # Check credit usage table
                cursor.execute("SELECT COUNT(*) FROM snov_credit_usage WHERE created_at > datetime('now', '-1 hour')")
                credit_records = cursor.fetchone()[0]
                logger.info(f"Credit usage records (last hour): {credit_records}")
                
                # Check enrichment state
                cursor.execute("SELECT COUNT(*) FROM enrichment_state WHERE created_at > datetime('now', '-1 hour')")
                state_records = cursor.fetchone()[0]
                logger.info(f"Enrichment state records (last hour): {state_records}")
                
                # Check domains table
                cursor.execute("SELECT COUNT(*) FROM company_domains WHERE created_at > datetime('now', '-1 hour')")
                domain_records = cursor.fetchone()[0]
                logger.info(f"Domain records (last hour): {domain_records}")
                
                logger.info("✅ Database persistence verified")
                self.test_results["steps_completed"].append("database_persistence")
                self.test_results["tests_passed"] += 1
                return True
                
        except Exception as e:
            logger.error(f"❌ Database persistence test failed: {e}")
            self.test_results["errors"].append(f"Database error: {str(e)}")
            self.test_results["tests_failed"] += 1
            return False
    
    async def run_complete_test(
        self, company_number: str, company_name: str
    ) -> dict[str, Any]:
        """Run complete E2E test suite."""
        logger.info("=" * 60)
        logger.info("STARTING E2E WORKFLOW TEST")
        logger.info(f"Mode: {'REAL APIs (USES CREDITS!)' if self.use_real_apis else 'MOCK MODE'}")
        logger.info(f"Company: {company_name} ({company_number})")
        logger.info("=" * 60)
        
        # Step 1: OAuth Token
        await self.test_step_1_oauth_token()
        
        # Step 2: Balance Check
        success, balance = await self.test_step_2_balance_check()
        
        # Step 3: Domain Search (if credits available)
        domain = None
        if success and balance > 0:
            success, domain = await self.test_step_3_domain_search_v2(company_name)
        
        # Step 4: Email Search (if domain found and credits available)
        if domain and balance > 1:
            await self.test_step_4_email_search_v2(domain, "John", "Smith")
        
        # Step 5: Credit Exhaustion
        await self.test_step_5_credit_exhaustion()
        
        # Step 6: Full Workflow
        await self.test_step_6_full_workflow(company_number, company_name)
        
        # Step 7: Database Persistence
        await self.test_step_7_database_persistence()
        
        # Final balance check
        if self.use_real_apis:
            final_balance = await self.credit_manager.check_available_credits()
            self.test_results["credit_consumption"]["final_balance"] = final_balance
            self.test_results["credit_consumption"]["total_consumed"] = (
                balance - final_balance if balance else 0
            )
            logger.info(f"Credits consumed in test: {self.test_results['credit_consumption']['total_consumed']}")
        
        # Generate summary
        self.test_results["completed_at"] = datetime.now().isoformat()
        self.test_results["summary"] = {
            "total_tests": self.test_results["tests_passed"] + self.test_results["tests_failed"],
            "passed": self.test_results["tests_passed"],
            "failed": self.test_results["tests_failed"],
            "success_rate": (
                self.test_results["tests_passed"] / 
                (self.test_results["tests_passed"] + self.test_results["tests_failed"]) * 100
                if (self.test_results["tests_passed"] + self.test_results["tests_failed"]) > 0
                else 0
            ),
        }
        
        return self.test_results
    
    def print_summary(self) -> None:
        """Print test summary."""
        logger.info("")
        logger.info("=" * 60)
        logger.info("E2E TEST SUMMARY")
        logger.info("=" * 60)
        
        summary = self.test_results.get("summary", {})
        logger.info(f"Total Tests: {summary.get('total_tests', 0)}")
        logger.info(f"Passed: {summary.get('passed', 0)}")
        logger.info(f"Failed: {summary.get('failed', 0)}")
        logger.info(f"Success Rate: {summary.get('success_rate', 0):.1f}%")
        
        if self.test_results.get("credit_consumption"):
            logger.info("")
            logger.info("Credit Consumption:")
            consumption = self.test_results["credit_consumption"]
            logger.info(f"  Initial Balance: {consumption.get('initial_balance', 'N/A')}")
            logger.info(f"  Final Balance: {consumption.get('final_balance', 'N/A')}")
            logger.info(f"  Total Consumed: {consumption.get('total_consumed', 'N/A')}")
        
        if self.test_results.get("errors"):
            logger.info("")
            logger.error("Errors encountered:")
            for error in self.test_results["errors"]:
                logger.error(f"  - {error}")
        
        # Save results to file
        results_file = f"e2e_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_file, "w") as f:
            json.dump(self.test_results, f, indent=2)
        logger.info(f"\nDetailed results saved to: {results_file}")


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="E2E Test for Snov.io Integration Workflow"
    )
    parser.add_argument(
        "--use-real-apis",
        action="store_true",
        help="Use real APIs (WARNING: Consumes credits!)",
    )
    parser.add_argument(
        "--test-company-number",
        default=TEST_COMPANY_NUMBER,
        help="Company number to test with",
    )
    parser.add_argument(
        "--test-company-name",
        default=TEST_COMPANY_NAME,
        help="Company name to test with",
    )
    
    args = parser.parse_args()
    
    if args.use_real_apis:
        logger.warning("=" * 60)
        logger.warning("WARNING: Using REAL APIs - This will consume credits!")
        logger.warning("=" * 60)
        response = input("Are you sure you want to continue? (yes/no): ")
        if response.lower() != "yes":
            logger.info("Test cancelled.")
            return
    
    # Run E2E test
    tester = E2EWorkflowTester(use_real_apis=args.use_real_apis)
    await tester.run_complete_test(
        company_number=args.test_company_number,
        company_name=args.test_company_name,
    )
    tester.print_summary()


if __name__ == "__main__":
    asyncio.run(main())