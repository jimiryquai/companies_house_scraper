#!/usr/bin/env python3
"""Tests for credit-aware workflow coordinator with v2 API endpoints.

This module tests the complete 10-step workflow with proper credit checking
before and after each Snov.io operation, ensuring non-blocking CH API processing
and accurate credit consumption tracking with v2 endpoints.
"""

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from src.enrichment.credit_aware_workflow import (
    CreditAwareWorkflowCoordinator,
    CreditAwareWorkflowError,
)
from src.enrichment.credit_manager import CreditManager
from src.enrichment.enrichment_state_manager import EnrichmentStateManager
from src.enrichment.snov_client import SnovioClient
from src.streaming.queue_manager import PriorityQueueManager, QueuedRequest, RequestPriority


class TestCreditAwareWorkflowCoordinator:
    """Test suite for credit-aware workflow coordinator."""

    @pytest.fixture
    def mock_credit_manager(self) -> Mock:
        """Create mock credit manager."""
        manager = Mock(spec=CreditManager)
        manager.can_consume_credits = AsyncMock(return_value=True)
        manager.check_available_credits = AsyncMock(return_value=100)
        manager.consume_credits = AsyncMock(return_value=True)
        return manager

    @pytest.fixture
    def mock_snov_client(self) -> Mock:
        """Create mock Snov.io client."""
        client = Mock(spec=SnovioClient)
        client.get_operation_credit_cost = Mock(return_value=1)
        client.domain_search = AsyncMock(
            return_value={
                "domains": [
                    {"domain": "example.com", "confidence": 0.9}
                ]
            }
        )
        client.find_email_by_domain_and_name = AsyncMock(
            return_value={
                "emails": [
                    {"email": "john@example.com", "confidence": 0.85}
                ]
            }
        )
        return client

    @pytest.fixture
    def mock_queue_manager(self) -> Mock:
        """Create mock queue manager."""
        manager = Mock(spec=PriorityQueueManager)
        manager.enqueue = AsyncMock(return_value=True)
        return manager

    @pytest.fixture
    def mock_state_manager(self) -> Mock:
        """Create mock enrichment state manager."""
        manager = Mock(spec=EnrichmentStateManager)
        manager.create_enrichment_state = AsyncMock(return_value=True)
        manager.update_enrichment_state = AsyncMock(return_value=True)
        return manager

    @pytest.fixture
    def workflow_coordinator(
        self,
        mock_credit_manager: Mock,
        mock_snov_client: Mock,
        mock_queue_manager: Mock,
        mock_state_manager: Mock,
    ) -> CreditAwareWorkflowCoordinator:
        """Create workflow coordinator with mocked dependencies."""
        return CreditAwareWorkflowCoordinator(
            credit_manager=mock_credit_manager,
            snov_client=mock_snov_client,
            queue_manager=mock_queue_manager,
            enrichment_state_manager=mock_state_manager,
        )

    @pytest.mark.asyncio
    async def test_workflow_uses_v2_domain_search(
        self,
        workflow_coordinator: CreditAwareWorkflowCoordinator,
        mock_snov_client: Mock,
    ) -> None:
        """Test that domain search uses v2 API endpoints."""
        # Execute workflow
        result = await workflow_coordinator.execute_workflow(
            company_number="12345678",
            company_name="Test Company Ltd",
            company_data={},
        )

        # Verify v2 domain search was called with correct parameters
        mock_snov_client.domain_search.assert_called_once_with(
            company_name="Test Company Ltd",
            limit=10,
            use_v2=True,
            use_polling=True,
        )

        # Check result contains domain search info
        assert result["company_number"] == "12345678"
        assert result["company_name"] == "Test Company Ltd"
        assert "domain_search" in str(result["snov_operations"])
        assert result["final_status"] == "completed"

    @pytest.mark.asyncio
    async def test_workflow_uses_v2_email_search(
        self,
        workflow_coordinator: CreditAwareWorkflowCoordinator,
        mock_snov_client: Mock,
    ) -> None:
        """Test that email search uses v2 API endpoints."""
        # Execute workflow
        result = await workflow_coordinator.execute_workflow(
            company_number="12345678",
            company_name="Test Company Ltd",
            company_data={},
        )

        # Verify v2 email search was called with correct parameters
        mock_snov_client.find_email_by_domain_and_name.assert_called_with(
            domain="example.com",
            first_name="Example",
            last_name="Officer",
            use_v2=True,
            use_polling=True,
        )

        # Check result contains email search info
        assert "email_search" in str(result["snov_operations"])
        # Total credits should be 0 in tests since mock doesn't simulate consumption
        assert result["total_credits_consumed"] >= 0

    @pytest.mark.asyncio
    async def test_workflow_credit_checks_before_operations(
        self,
        workflow_coordinator: CreditAwareWorkflowCoordinator,
        mock_credit_manager: Mock,
    ) -> None:
        """Test that credit checks happen before each Snov.io operation."""
        # Execute workflow
        await workflow_coordinator.execute_workflow(
            company_number="12345678",
            company_name="Test Company Ltd",
            company_data={},
        )

        # Verify credit checks were performed
        # Should be called at least twice (before domain search and before email search)
        assert mock_credit_manager.can_consume_credits.call_count >= 2
        assert mock_credit_manager.check_available_credits.call_count >= 2

    @pytest.mark.asyncio
    async def test_workflow_stops_snov_operations_when_no_credits(
        self,
        workflow_coordinator: CreditAwareWorkflowCoordinator,
        mock_credit_manager: Mock,
        mock_snov_client: Mock,
    ) -> None:
        """Test that Snov.io operations stop when credits exhausted."""
        # Configure credit manager to return no credits available
        mock_credit_manager.can_consume_credits.return_value = False

        # Execute workflow
        result = await workflow_coordinator.execute_workflow(
            company_number="12345678",
            company_name="Test Company Ltd",
            company_data={},
        )

        # Verify Snov.io operations were NOT called
        mock_snov_client.domain_search.assert_not_called()
        mock_snov_client.find_email_by_domain_and_name.assert_not_called()

        # Check workflow marked as credit exhausted
        assert result["final_status"] == "credit_exhausted_before_domain"
        assert result["total_credits_consumed"] == 0

    @pytest.mark.asyncio
    async def test_workflow_continues_ch_api_when_no_credits(
        self,
        workflow_coordinator: CreditAwareWorkflowCoordinator,
        mock_credit_manager: Mock,
        mock_queue_manager: Mock,
    ) -> None:
        """Test that CH API continues even when Snov.io credits exhausted."""
        # Configure credit manager to return no credits available
        mock_credit_manager.can_consume_credits.return_value = False

        # Execute workflow
        result = await workflow_coordinator.execute_workflow(
            company_number="12345678",
            company_name="Test Company Ltd",
            company_data={},
        )

        # Verify CH API operations were still queued
        mock_queue_manager.enqueue.assert_called_once()
        
        # Check the queued request
        call_args = mock_queue_manager.enqueue.call_args
        request = call_args[0][0]
        assert isinstance(request, QueuedRequest)
        assert request.endpoint == "companies/12345678/officers"
        assert request.priority == RequestPriority.HIGH

        # Verify CH API call was recorded
        assert len(result["ch_api_calls"]) > 0
        assert result["ch_api_calls"][0]["operation"] == "fetch_officers"

    @pytest.mark.asyncio
    async def test_workflow_handles_no_domain_found(
        self,
        workflow_coordinator: CreditAwareWorkflowCoordinator,
        mock_snov_client: Mock,
    ) -> None:
        """Test workflow handles case when no domain is found."""
        # Configure Snov client to return no domains
        mock_snov_client.domain_search.return_value = {"domains": []}

        # Execute workflow
        result = await workflow_coordinator.execute_workflow(
            company_number="12345678",
            company_name="Test Company Ltd",
            company_data={},
        )

        # Verify email search was NOT attempted
        mock_snov_client.find_email_by_domain_and_name.assert_not_called()

        # Check workflow status
        assert result["final_status"] == "no_domain_found"
        # Total credits should be 0 since no actual consumption in mock
        assert result["total_credits_consumed"] == 0

    @pytest.mark.asyncio
    async def test_workflow_tracks_credit_consumption(
        self,
        workflow_coordinator: CreditAwareWorkflowCoordinator,
        mock_credit_manager: Mock,
    ) -> None:
        """Test workflow accurately tracks credit consumption."""
        # Configure credit manager to simulate consumption
        # Start with 100, then 99 after domain search, then 98 after email search
        mock_credit_manager.check_available_credits.side_effect = [
            100,  # Pre-domain check
            99,   # Post-domain check
            99,   # Pre-email check
            98,   # Post-email check
        ]

        # Execute workflow
        result = await workflow_coordinator.execute_workflow(
            company_number="12345678",
            company_name="Test Company Ltd",
            company_data={},
        )

        # Check credit tracking
        credit_checks = result["credit_checks"]
        assert len(credit_checks) >= 4  # Pre/post for both operations

        # Verify total consumption
        assert result["total_credits_consumed"] == 2

    @pytest.mark.asyncio
    async def test_workflow_handles_zero_credit_consumption(
        self,
        workflow_coordinator: CreditAwareWorkflowCoordinator,
        mock_credit_manager: Mock,
        mock_snov_client: Mock,
    ) -> None:
        """Test workflow handles operations that consume 0 credits."""
        # Configure to return empty results (no consumption)
        mock_snov_client.domain_search.return_value = {"domains": []}
        
        # Balance doesn't change (operation found no results)
        mock_credit_manager.check_available_credits.return_value = 100

        # Execute workflow
        result = await workflow_coordinator.execute_workflow(
            company_number="12345678",
            company_name="Test Company Ltd",
            company_data={},
        )

        # Check that 0 credits were consumed
        assert result["total_credits_consumed"] == 0
        assert result["final_status"] == "no_domain_found"

    @pytest.mark.asyncio
    async def test_workflow_handles_api_errors_gracefully(
        self,
        workflow_coordinator: CreditAwareWorkflowCoordinator,
        mock_snov_client: Mock,
    ) -> None:
        """Test workflow handles API errors gracefully."""
        # Configure Snov client to raise an error
        mock_snov_client.domain_search.side_effect = Exception("API Error")

        # Execute workflow
        result = await workflow_coordinator.execute_workflow(
            company_number="12345678",
            company_name="Test Company Ltd",
            company_data={},
        )

        # Check error handling - workflow continues despite domain search error
        # Since domain search fails, workflow marks it as no domain found
        assert result["final_status"] == "no_domain_found"
        # Check that error was recorded in operations
        assert any("error" in str(op) for op in result["snov_operations"])

    @pytest.mark.asyncio
    async def test_workflow_statistics_tracking(
        self,
        workflow_coordinator: CreditAwareWorkflowCoordinator,
    ) -> None:
        """Test workflow tracks statistics correctly."""
        # Execute multiple workflows
        await workflow_coordinator.execute_workflow(
            company_number="12345678",
            company_name="Test Company Ltd",
            company_data={},
        )
        
        await workflow_coordinator.execute_workflow(
            company_number="87654321",
            company_name="Another Company Ltd",
            company_data={},
        )

        # Check statistics
        stats = workflow_coordinator.get_workflow_statistics()
        assert stats["workflows_started"] == 2
        assert stats["workflows_completed"] == 2
        assert stats["domain_searches_completed"] == 2
        assert stats["email_searches_completed"] == 2
        assert stats["success_rate"] == 100.0

    @pytest.mark.asyncio
    async def test_workflow_v2_endpoint_configuration(
        self,
        workflow_coordinator: CreditAwareWorkflowCoordinator,
        mock_snov_client: Mock,
    ) -> None:
        """Test that workflow properly configures v2 endpoints."""
        # Execute workflow
        await workflow_coordinator.execute_workflow(
            company_number="12345678",
            company_name="Test Company Ltd",
            company_data={},
        )

        # Verify all v2 parameters are set correctly
        domain_call = mock_snov_client.domain_search.call_args
        assert domain_call.kwargs["use_v2"] is True
        assert domain_call.kwargs["use_polling"] is True

        email_call = mock_snov_client.find_email_by_domain_and_name.call_args
        assert email_call.kwargs["use_v2"] is True
        assert email_call.kwargs["use_polling"] is True