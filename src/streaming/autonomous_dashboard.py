"""Autonomous operation dashboard for cloud deployment status.

This module provides a comprehensive dashboard for monitoring autonomous
operation status in cloud deployment environments, with real-time metrics,
health indicators, and operational visibility.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List

from .api_usage_monitor import RealTimeAPIUsageMonitor
from .company_state_manager import CompanyStateManager
from .comprehensive_metrics_collector import ComprehensiveMetricsCollector
from .queue_manager import PriorityQueueManager
from .rate_limit_monitor import RateLimitMonitor
from .recovery_manager import ServiceRecoveryManager

logger = logging.getLogger(__name__)


class AutonomousOperationDashboard:
    """Comprehensive autonomous operation dashboard for cloud deployment monitoring.

    Provides real-time visibility into:
    - System health and operational status
    - API compliance and rate limit monitoring
    - Queue depth and processing throughput
    - Error rates and recovery status
    - Performance metrics and trends
    - Autonomous operation recommendations
    """

    def __init__(
        self,
        metrics_collector: ComprehensiveMetricsCollector,
        api_usage_monitor: RealTimeAPIUsageMonitor,
        rate_limit_monitor: RateLimitMonitor,
        state_manager: CompanyStateManager,
        recovery_manager: ServiceRecoveryManager,
        queue_manager: PriorityQueueManager,
        refresh_interval_seconds: int = 10,
    ) -> None:
        """Initialize the autonomous operation dashboard.

        Args:
            metrics_collector: Comprehensive metrics collector
            api_usage_monitor: API usage monitor
            rate_limit_monitor: Rate limit monitor
            state_manager: Company state manager
            recovery_manager: Recovery manager
            queue_manager: Queue manager
            refresh_interval_seconds: Dashboard refresh interval
        """
        self.metrics_collector = metrics_collector
        self.api_usage_monitor = api_usage_monitor
        self.rate_limit_monitor = rate_limit_monitor
        self.state_manager = state_manager
        self.recovery_manager = recovery_manager
        self.queue_manager = queue_manager

        self.refresh_interval = refresh_interval_seconds
        self.start_time = datetime.now()

        logger.info("AutonomousOperationDashboard initialized")

    async def get_dashboard_data(self) -> Dict[str, Any]:
        """Get complete dashboard data with real-time metrics.

        Returns:
            Comprehensive dashboard data dictionary
        """
        # Collect current metrics
        metrics = self.metrics_collector.get_comprehensive_metrics()
        api_stats = self.api_usage_monitor.get_current_statistics()
        rate_summary = self.rate_limit_monitor.get_violation_summary()
        recovery_status = self.recovery_manager.get_recovery_status()
        queue_status = self.queue_manager.get_queue_status()

        # Get processing statistics
        try:
            processing_stats = await self.state_manager.get_processing_statistics()
        except Exception as e:
            logger.debug("Error getting processing stats: %s", e)
            processing_stats = {"total_companies": 0, "by_state": {}}

        # Calculate uptime
        uptime_seconds = (datetime.now() - self.start_time).total_seconds()

        return {
            "timestamp": datetime.now().isoformat(),
            "uptime_seconds": uptime_seconds,
            "refresh_interval": self.refresh_interval,
            # System Overview
            "system_overview": {
                "status": self._get_overall_system_status(metrics, api_stats, rate_summary),
                "health_score": metrics["system_performance"]["health"]["overall_score"],
                "uptime_hours": round(uptime_seconds / 3600, 2),
                "components_healthy": f"{metrics['system_performance']['health']['components_healthy']}/{metrics['system_performance']['health']['components_total']}",
                "active_alerts": metrics["system_performance"]["alerts"]["active_alerts"],
                "critical_alerts": metrics["system_performance"]["alerts"]["critical_alerts"],
            },
            # API Compliance Dashboard
            "api_compliance": {
                "current_usage": {
                    "calls_in_window": api_stats["current_window"]["calls"],
                    "usage_percentage": round(api_stats["current_window"]["usage_percentage"], 1),
                    "rate_limit": api_stats["current_window"]["rate_limit"],
                    "usage_level": api_stats["current_window"]["usage_level"],
                    "time_remaining_minutes": round(
                        api_stats["current_window"].get("time_remaining_minutes", 0), 1
                    ),
                },
                "compliance_status": {
                    "level": rate_summary["status"]["level"],
                    "violations_current_window": rate_summary["current_window"]["violations"],
                    "violations_total": rate_summary["totals"]["total_violations"],
                    "last_violation": rate_summary["status"].get("last_violation"),
                },
                "performance": {
                    "avg_response_time_ms": round(
                        api_stats["performance"]["avg_response_time_ms"], 1
                    ),
                    "success_rate": round(api_stats["performance"]["success_rate"] * 100, 2),
                    "error_rate": round((1 - api_stats["performance"]["success_rate"]) * 100, 2),
                },
                "predictions": {
                    "projected_next_window": api_stats["predictions"][
                        "projected_calls_next_window"
                    ],
                    "risk_level": api_stats["predictions"]["risk_level"],
                },
            },
            # Queue Operations Dashboard
            "queue_operations": {
                "queue_status": {
                    "total_queued": queue_status["total_queued"],
                    "utilization_percentage": round(queue_status["utilization"] * 100, 1),
                    "max_capacity": queue_status["max_capacity"],
                },
                "priority_breakdown": {
                    "high_priority": queue_status["queues"].get("HIGH", {}).get("depth", 0),
                    "medium_priority": queue_status["queues"].get("MEDIUM", {}).get("depth", 0),
                    "low_priority": queue_status["queues"].get("LOW", {}).get("depth", 0),
                    "background": queue_status["queues"].get("BACKGROUND", {}).get("depth", 0),
                },
                "processing_metrics": {
                    "total_processed": queue_status["metrics"]["total_processed"],
                    "success_rate": round(queue_status["metrics"]["success_rate"] * 100, 2),
                    "avg_processing_time": round(queue_status["metrics"]["avg_processing_time"], 3),
                    "processing_rate_per_minute": round(
                        metrics["queue_metrics"]["performance"]["processing_rate_per_minute"], 1
                    ),
                },
                "queue_health": {
                    "oldest_request_age_seconds": round(
                        metrics["queue_metrics"]["performance"]["oldest_request_age_seconds"], 1
                    ),
                    "health_score": round(metrics["queue_metrics"]["health_score"], 3),
                },
            },
            # Company Processing Dashboard
            "company_processing": {
                "totals": {
                    "total_companies": processing_stats.get("total_companies", 0),
                    "pipeline_backlog": metrics["processing_metrics"]["backlog"][
                        "pipeline_backlog"
                    ],
                },
                "state_breakdown": processing_stats.get("by_state", {}),
                "processing_rates": {
                    "companies_per_minute": round(
                        metrics["processing_metrics"]["rates"]["companies_per_minute"], 2
                    ),
                    "efficiency_score": round(
                        metrics["processing_metrics"]["efficiency"]["pipeline_efficiency_score"], 3
                    ),
                },
            },
            # Recovery Status Dashboard
            "recovery_status": {
                "is_recovering": recovery_status["is_recovering"],
                "emergency_stop": recovery_status["emergency_stop"],
                "last_recovery": self._get_last_recovery_info(recovery_status),
                "recovery_readiness": self._assess_recovery_readiness(metrics, api_stats),
            },
            # Operational Recommendations
            "recommendations": self._generate_operational_recommendations(
                metrics, api_stats, rate_summary, queue_status
            ),
            # Trends and Historical Data
            "trends": self._calculate_trends(),
            # Alert Summary
            "alerts": self._get_alert_summary(rate_summary),
            # Resource Usage
            "resources": {
                "memory_usage_mb": metrics["system_performance"].get("memory_usage_mb", 0),
                "cpu_usage_percentage": metrics["system_performance"].get(
                    "cpu_usage_percentage", 0
                ),
            },
        }

    def _get_overall_system_status(
        self, metrics: Dict[str, Any], api_stats: Dict[str, Any], rate_summary: Dict[str, Any]
    ) -> str:
        """Determine overall system status."""
        # Check for critical issues
        if rate_summary["status"]["level"] == "EMERGENCY":
            return "CRITICAL"

        if metrics["system_performance"]["alerts"]["critical_alerts"] > 0:
            return "CRITICAL"

        # Check for warnings
        if rate_summary["status"]["level"] in ["CRITICAL", "WARNING"]:
            return "WARNING"

        if api_stats["current_window"]["usage_level"] in ["high", "critical"]:
            return "WARNING"

        if metrics["queue_metrics"]["utilization"]["percentage"] > 80:
            return "WARNING"

        # Check overall health
        health_score = metrics["system_performance"]["health"]["overall_score"]
        if health_score >= 0.9:
            return "HEALTHY"
        if health_score >= 0.7:
            return "DEGRADED"
        return "WARNING"

    def _get_last_recovery_info(self, recovery_status: Dict[str, Any]) -> Dict[str, Any]:
        """Get information about the last recovery."""
        metrics = recovery_status.get("metrics", {})

        if not metrics:
            return {"status": "none", "message": "No recovery history"}

        return {
            "status": metrics.get("status", "unknown"),
            "start_time": metrics.get("start_time"),
            "total_duration": metrics.get("total_duration"),
            "companies_recovered": metrics.get("companies_recovered", 0),
            "companies_failed": metrics.get("companies_failed", 0),
        }

    def _assess_recovery_readiness(
        self, metrics: Dict[str, Any], api_stats: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Assess system readiness for recovery if needed."""
        queue_utilization = metrics["queue_metrics"]["utilization"]["percentage"]
        api_usage = api_stats["current_window"]["usage_percentage"]
        health_score = metrics["system_performance"]["health"]["overall_score"]

        readiness_score = (
            (100 - queue_utilization) / 100 * 0.4  # Lower queue utilization = better
            + (100 - api_usage) / 100 * 0.4  # Lower API usage = better
            + health_score * 0.2  # Higher health = better
        )

        if readiness_score >= 0.8:
            readiness_level = "EXCELLENT"
        elif readiness_score >= 0.6:
            readiness_level = "GOOD"
        elif readiness_score >= 0.4:
            readiness_level = "FAIR"
        else:
            readiness_level = "POOR"

        return {
            "score": round(readiness_score, 3),
            "level": readiness_level,
            "factors": {
                "queue_utilization": round(queue_utilization, 1),
                "api_usage": round(api_usage, 1),
                "health_score": round(health_score, 3),
            },
        }

    def _generate_operational_recommendations(
        self,
        metrics: Dict[str, Any],
        api_stats: Dict[str, Any],
        rate_summary: Dict[str, Any],
        queue_status: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Generate operational recommendations."""
        recommendations = []

        # API Usage Recommendations
        usage_percentage = api_stats["current_window"]["usage_percentage"]
        if usage_percentage > 95:
            recommendations.append(
                {
                    "priority": "CRITICAL",
                    "category": "API_USAGE",
                    "message": f"API usage at {usage_percentage:.1f}% - immediate throttling required",
                    "action": "Enable emergency throttling and review request patterns",
                }
            )
        elif usage_percentage > 80:
            recommendations.append(
                {
                    "priority": "HIGH",
                    "category": "API_USAGE",
                    "message": f"API usage at {usage_percentage:.1f}% - monitor closely",
                    "action": "Consider implementing request prioritization",
                }
            )

        # Queue Recommendations
        queue_utilization = queue_status["utilization"] * 100
        if queue_utilization > 90:
            recommendations.append(
                {
                    "priority": "HIGH",
                    "category": "QUEUE_CAPACITY",
                    "message": f"Queue utilization at {queue_utilization:.1f}% - approaching capacity",
                    "action": "Increase processing rate or queue capacity",
                }
            )
        elif queue_utilization > 75:
            recommendations.append(
                {
                    "priority": "MEDIUM",
                    "category": "QUEUE_CAPACITY",
                    "message": f"Queue utilization at {queue_utilization:.1f}% - monitor growth",
                    "action": "Review queue processing efficiency",
                }
            )

        # Rate Limit Violations
        violations = rate_summary["current_window"]["violations"]
        if violations > 0:
            recommendations.append(
                {
                    "priority": "HIGH",
                    "category": "RATE_LIMITS",
                    "message": f"{violations} rate limit violations in current window",
                    "action": "Review API usage patterns and implement backoff strategies",
                }
            )

        # Performance Recommendations
        health_score = metrics["system_performance"]["health"]["overall_score"]
        if health_score < 0.7:
            recommendations.append(
                {
                    "priority": "MEDIUM",
                    "category": "PERFORMANCE",
                    "message": f"System health score low: {health_score:.2f}",
                    "action": "Investigate component health and resource usage",
                }
            )

        # Processing Efficiency
        success_rate = queue_status["metrics"]["success_rate"]
        if success_rate < 0.95:
            recommendations.append(
                {
                    "priority": "MEDIUM",
                    "category": "RELIABILITY",
                    "message": f"Processing success rate: {success_rate:.1%}",
                    "action": "Investigate and resolve processing failures",
                }
            )

        return recommendations

    def _calculate_trends(self) -> Dict[str, Any]:
        """Calculate trends from historical data."""
        # This would typically analyze historical metrics
        # For now, return placeholder trend data
        return {
            "api_usage_trend": "stable",
            "queue_utilization_trend": "increasing",
            "processing_rate_trend": "stable",
            "error_rate_trend": "decreasing",
        }

    def _get_alert_summary(self, rate_summary: Dict[str, Any]) -> Dict[str, Any]:
        """Get summary of active alerts."""
        return {
            "total_active": len(rate_summary.get("status", {}).get("active_alerts", 0)),
            "by_level": {
                "emergency": 1 if rate_summary["status"]["level"] == "EMERGENCY" else 0,
                "critical": 1 if rate_summary["status"]["level"] == "CRITICAL" else 0,
                "warning": 1 if rate_summary["status"]["level"] == "WARNING" else 0,
                "info": 0,
            },
            "recent_alerts": rate_summary.get("recent_alerts", [])[:5],  # Last 5 alerts
        }

    def get_dashboard_json(self) -> str:
        """Get dashboard data as JSON string.

        Returns:
            JSON formatted dashboard data
        """
        try:
            dashboard_data = asyncio.run(self.get_dashboard_data())
            return json.dumps(dashboard_data, indent=2, default=str)
        except Exception as e:
            logger.error("Error generating dashboard JSON: %s", e)
            return json.dumps(
                {"error": str(e), "timestamp": datetime.now().isoformat(), "status": "ERROR"},
                indent=2,
            )

    def get_status_summary(self) -> Dict[str, Any]:
        """Get concise status summary for health checks.

        Returns:
            Concise status summary
        """
        try:
            dashboard_data = asyncio.run(self.get_dashboard_data())

            return {
                "timestamp": dashboard_data["timestamp"],
                "status": dashboard_data["system_overview"]["status"],
                "health_score": dashboard_data["system_overview"]["health_score"],
                "api_usage_percentage": dashboard_data["api_compliance"]["current_usage"][
                    "usage_percentage"
                ],
                "queue_utilization_percentage": dashboard_data["queue_operations"]["queue_status"][
                    "utilization_percentage"
                ],
                "active_alerts": dashboard_data["system_overview"]["active_alerts"],
                "uptime_hours": dashboard_data["system_overview"]["uptime_hours"],
                "recommendations_count": len(dashboard_data["recommendations"]),
            }
        except Exception as e:
            logger.error("Error generating status summary: %s", e)
            return {"timestamp": datetime.now().isoformat(), "status": "ERROR", "error": str(e)}

    def export_for_monitoring_platform(self, platform: str = "generic") -> Dict[str, Any]:
        """Export dashboard data for external monitoring platforms.

        Args:
            platform: Target monitoring platform

        Returns:
            Platform-specific monitoring data
        """
        try:
            dashboard_data = asyncio.run(self.get_dashboard_data())

            if platform == "datadog":
                return {
                    "series": [
                        {
                            "metric": "companies_house.system.health_score",
                            "points": [
                                [
                                    datetime.now().timestamp(),
                                    dashboard_data["system_overview"]["health_score"],
                                ]
                            ],
                            "tags": ["service:companies-house-streaming"],
                        },
                        {
                            "metric": "companies_house.api.usage_percentage",
                            "points": [
                                [
                                    datetime.now().timestamp(),
                                    dashboard_data["api_compliance"]["current_usage"][
                                        "usage_percentage"
                                    ],
                                ]
                            ],
                            "tags": ["service:companies-house-streaming"],
                        },
                        {
                            "metric": "companies_house.queue.utilization_percentage",
                            "points": [
                                [
                                    datetime.now().timestamp(),
                                    dashboard_data["queue_operations"]["queue_status"][
                                        "utilization_percentage"
                                    ],
                                ]
                            ],
                            "tags": ["service:companies-house-streaming"],
                        },
                    ]
                }

            if platform == "newrelic":
                return {
                    "metrics": [
                        {
                            "name": "Custom/CompaniesHouse/SystemHealthScore",
                            "value": dashboard_data["system_overview"]["health_score"],
                            "timestamp": datetime.now().timestamp(),
                        },
                        {
                            "name": "Custom/CompaniesHouse/APIUsagePercentage",
                            "value": dashboard_data["api_compliance"]["current_usage"][
                                "usage_percentage"
                            ],
                            "timestamp": datetime.now().timestamp(),
                        },
                    ]
                }

            # generic
            return {
                "service": "companies-house-streaming",
                "dashboard_data": dashboard_data,
                "export_timestamp": datetime.now().isoformat(),
            }

        except Exception as e:
            logger.error("Error exporting for monitoring platform: %s", e)
            return {"error": str(e), "timestamp": datetime.now().isoformat()}
