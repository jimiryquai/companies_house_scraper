#!/usr/bin/env python3
"""Test script for the complete enrichment workflow.

This script tests the end-to-end integration between streaming API
and Snov.io enrichment workflow.
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.enrichment.enrichment_coordinator import EnrichmentCoordinator


async def test_basic_initialization() -> bool:
    """Test that coordinator can be initialized properly.
    
    Returns:
        True if initialization succeeds
    """
    print("🔧 Testing enrichment coordinator initialization...")
    
    try:
        coordinator = EnrichmentCoordinator(database_path="companies.db")
        await coordinator.initialize()
        
        status = coordinator.get_status()
        print(f"✅ Coordinator initialized: {status['initialized']}")
        print(f"📊 Components available: {sum(status['components'].values())}/8")
        
        # Print component status
        for component, available in status['components'].items():
            status_icon = "✅" if available else "❌"
            print(f"   {status_icon} {component}")
            
        return status['initialized']
        
    except Exception as e:
        print(f"❌ Initialization failed: {e}")
        return False


async def test_strike_off_processing() -> bool:
    """Test processing a strike-off company.
    
    Returns:
        True if processing succeeds
    """
    print("\n🏢 Testing strike-off company processing...")
    
    try:
        coordinator = EnrichmentCoordinator(database_path="companies.db")
        await coordinator.initialize()
        
        # Test with a sample company that should have officers
        test_company_number = "12345678"  # This should exist in your 10K dataset
        test_company_name = "Test Company Ltd"
        
        result = await coordinator.process_strike_off_company(
            company_number=test_company_number,
            company_name=test_company_name
        )
        
        if result:
            print(f"✅ Successfully initiated enrichment for {test_company_number}")
        else:
            print(f"⚠️ Enrichment skipped for {test_company_number} (expected if no officers)")
            
        return True
        
    except Exception as e:
        print(f"❌ Strike-off processing failed: {e}")
        return False


async def test_queue_processor() -> bool:
    """Test that queue processor can start and stop.
    
    Returns:
        True if queue processor works
    """
    print("\n🔄 Testing queue processor...")
    
    try:
        coordinator = EnrichmentCoordinator(database_path="companies.db")
        await coordinator.initialize()
        
        # Test starting processor (this will run continuously until stopped)
        print("📤 Starting queue processor (will run for 2 seconds)...")
        
        # Start processing in background
        processing_task = asyncio.create_task(coordinator.start_processing())
        
        # Let it run briefly
        await asyncio.sleep(2.0)
        
        # Stop processing
        await coordinator.stop_processing()
        
        # Cancel the background task
        processing_task.cancel()
        try:
            await processing_task
        except asyncio.CancelledError:
            pass
            
        print("✅ Queue processor started and stopped successfully")
        return True
        
    except Exception as e:
        print(f"❌ Queue processor test failed: {e}")
        return False


async def main() -> None:
    """Run all tests."""
    print("🧪 ENRICHMENT WORKFLOW TEST SUITE")
    print("=" * 50)
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    tests = [
        ("Basic Initialization", test_basic_initialization),
        ("Strike-off Processing", test_strike_off_processing), 
        ("Queue Processor", test_queue_processor),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = await test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Test {test_name} crashed: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("📋 TEST RESULTS SUMMARY")
    print("=" * 50)
    
    passed = 0
    for test_name, result in results:
        status_icon = "✅" if result else "❌"
        print(f"{status_icon} {test_name}")
        if result:
            passed += 1
    
    print(f"\n📊 Tests passed: {passed}/{len(tests)}")
    
    if passed == len(tests):
        print("🎉 All tests passed! Enrichment workflow is ready.")
    else:
        print("⚠️ Some tests failed. Check the logs above for details.")


if __name__ == "__main__":
    asyncio.run(main())