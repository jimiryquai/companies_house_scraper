# Companies House Scraper

A comprehensive tool for scraping and monitoring Companies House data with real-time streaming API integration.

## Features

- Real-time Companies House streaming API integration
- Queue-based API rate limiting for bulletproof compliance
- Strike-off company detection and monitoring
- Officer data collection and management
- SQLite database with streaming metadata
- Health monitoring and error alerting
- Circuit breaker pattern for API resilience

## Current Status

This repository contains a working Companies House scraper with:
- ✅ All database tests passing (21/21 fixed)
- ✅ Health monitoring tests rewritten for actual APIs
- ✅ Queue-based architecture for rate limiting
- ✅ Company Processing State Manager
- ✅ Emergency safeguards against API violations

## Architecture

### Streaming API Integration

![Streaming Architecture](./streaming_architecture.svg)

### Queue Decision Flow

![Queue Decision Flow](./queue_decision_flow.svg)

## Recent Updates

- ✅ **Task 6.2.1 Completed**: Emergency safeguards and queue-only architecture
- ✅ **Database Triggers**: Bulletproof state management enforcement
- ✅ **API Protection**: Zero direct API calls, all requests go through queue
- ✅ **Cloud Ready**: Bulletproof rate limiting for autonomous deployment
