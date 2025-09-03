#!/usr/bin/env python3
"""Test Snov.io API connection and domain search functionality."""

import asyncio
import os
import aiohttp
import json
from urllib.parse import quote

async def test_snov_api():
    """Test Snov.io API connection."""
    client_id = os.getenv('SNOV_CLIENT_ID')
    client_secret = os.getenv('SNOV_CLIENT_SECRET')
    
    if not client_id or not client_secret:
        print("❌ Missing SNOV_CLIENT_ID or SNOV_CLIENT_SECRET")
        return
    
    print(f"✅ Using Snov.io credentials:")
    print(f"   Client ID: {client_id[:10]}...")
    print(f"   Client Secret: {client_secret[:10]}...")
    
    # Test OAuth token generation
    print("\n🔐 Testing OAuth token generation...")
    token_url = "https://api.snov.io/v1/oauth/access_token"
    
    async with aiohttp.ClientSession() as session:
        token_data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        }
        
        try:
            async with session.post(token_url, data=token_data) as response:
                if response.status == 200:
                    token_response = await response.json()
                    access_token = token_response.get('access_token')
                    print(f"✅ OAuth token obtained: {access_token[:20]}...")
                    
                    # Test domain search
                    print("\n🔍 Testing domain search for 'embersbushcraft.com'...")
                    domain = "embersbushcraft.com"  # Related to EMBERS BUSHCRAFT LIMITED
                    search_url = f"https://api.snov.io/v2/domain-emails-with-info?domain={quote(domain)}&type=all&limit=10"
                    
                    headers = {
                        "Authorization": f"Bearer {access_token}",
                        "Content-Type": "application/json"
                    }
                    
                    async with session.get(search_url, headers=headers) as search_response:
                        print(f"📡 Domain search response status: {search_response.status}")
                        
                        if search_response.status == 200:
                            search_data = await search_response.json()
                            print(f"✅ Domain search successful!")
                            print(f"📊 Response: {json.dumps(search_data, indent=2)}")
                        else:
                            error_text = await search_response.text()
                            print(f"❌ Domain search failed: {error_text}")
                    
                else:
                    error_text = await response.text()
                    print(f"❌ OAuth token failed: {response.status} - {error_text}")
                    
        except Exception as e:
            print(f"💥 Exception during API test: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_snov_api())