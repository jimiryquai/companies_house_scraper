#!/usr/bin/env python3
"""Direct test of Companies House API calls without queue/async complexity."""

import asyncio
import base64
import yaml
import aiohttp
from aiohttp import ClientTimeout


async def test_ch_api():
    """Test direct Companies House API call."""
    # Load config
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    rest_api_key = config.get('api', {}).get('key', '')
    if not rest_api_key:
        print("❌ No REST API key found in config.yaml")
        return
    
    print(f"✅ Using API key: {rest_api_key[:10]}...")
    
    # Test company (use one that was recently queued)
    company_number = "05880363"  # From recent logs
    
    # Setup HTTP session
    timeout = ClientTimeout(total=30)
    auth_string = base64.b64encode(f"{rest_api_key}:".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth_string}",
        "User-Agent": "companies-house-scraper/1.0",
        "Accept": "application/json",
    }
    
    url = f"https://api.company-information.service.gov.uk/company/{company_number}"
    
    print(f"🔍 Testing Companies House API call to: {url}")
    
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        try:
            async with session.get(url) as response:
                print(f"📡 HTTP Status: {response.status}")
                
                if response.status == 200:
                    data = await response.json()
                    company_name = data.get('company_name', 'Unknown')
                    company_status = data.get('company_status', 'Unknown')
                    company_status_detail = data.get('company_status_detail', 'None')
                    
                    print(f"🏢 Company: {company_name}")
                    print(f"📊 Status: {company_status}")
                    print(f"📋 Status Detail: {company_status_detail}")
                    
                    # Check for strike-off
                    if company_status_detail and any(indicator in company_status_detail.lower() 
                                                   for indicator in ["strike off", "struck off", "proposal to strike off"]):
                        print(f"🎯 STRIKE-OFF DETECTED!")
                    else:
                        print("📈 Active company (no strike-off)")
                        
                elif response.status == 404:
                    print("❌ Company not found")
                elif response.status == 401:
                    print("❌ Unauthorized - check API key")
                elif response.status == 429:
                    print("⚠️ Rate limited")
                else:
                    print(f"❌ Unexpected status: {response.status}")
                    print(await response.text())
                    
        except Exception as e:
            print(f"💥 Exception during API call: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    print("🚀 Starting Companies House API test...")
    asyncio.run(test_ch_api())
    print("✅ Test complete")