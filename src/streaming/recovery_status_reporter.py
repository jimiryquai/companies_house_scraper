"""Automated recovery status reporting for operational visibility.

Provides comprehensive reporting on error recovery operations, system self-healing,
and automated remediation actions for cloud deployment monitoring and operations teams.
"""

import asyncio
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .config import StreamingConfig
from .recovery_manager import RecoveryPhase, RecoveryStatus, ServiceRecoveryManager
from .structured_logger import LogContext, LogLevel, StructuredLogger
from .unattended_operation_logger import (
    DiagnosticCategory,
    OperationalSeverity,
    UnattendedOperationLogger,
)


class RecoveryEventType(Enum):
    """Types of recovery events."""

    RECOVERY_INITIATED = "recovery_initiated"
    RECOVERY_PROGRESS = "recovery_progress"
    RECOVERY_COMPLETED = "recovery_completed"
    RECOVERY_FAILED = "recovery_failed"
    RECOVERY_TIMEOUT = "recovery_timeout"
    MANUAL_INTERVENTION_REQUIRED = "manual_intervention_required"
    SYSTEM_STABILIZED = "system_stabilized"
    DEGRADED_OPERATION = "degraded_operation"


class RecoveryCategory(Enum):
    """Categories of recovery operations."""

    CONNECTION_RECOVERY = "connection_recovery"
    RATE_LIMIT_RECOVERY = "rate_limit_recovery"
    DATABASE_RECOVERY = "database_recovery"
    QUEUE_RECOVERY = "queue_recovery"
    API_RECOVERY = "api_recovery"
    STREAMING_RECOVERY = "streaming_recovery"
    SYSTEM_RECOVERY = "system_recovery"
    DATA_INTEGRITY_RECOVERY = "data_integrity_recovery"


@dataclass
class RecoveryAction:
    """Individual recovery action taken by the system."""

    action_id: str
    action_type: str
    description: str
    timestamp: datetime
    duration_seconds: Optional[float] = None
    success: bool = True
    error_message: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = {
            "action_id": self.action_id,
            "action_type": self.action_type,
            "description": self.description,
            "timestamp": self.timestamp.isoformat(),
            "success": self.success,
        }

        if self.duration_seconds is not None:
            result["duration_seconds"] = self.duration_seconds
        if self.error_message:
            result["error_message"] = self.error_message
        if self.parameters:
            result["parameters"] = self.parameters

        return result


@dataclass
class RecoveryIncident:
    """Complete recovery incident with all related actions and outcomes."""

    incident_id: str
    category: RecoveryCategory
    trigger_event: str
    trigger_timestamp: datetime
    initial_symptoms: List[str]
    actions_taken: List[RecoveryAction] = field(default_factory=list)
    current_status: RecoveryStatus = RecoveryStatus.IN_PROGRESS
    current_phase: RecoveryPhase = RecoveryPhase.VALIDATION
    completion_timestamp: Optional[datetime] = None
    total_duration_seconds: Optional[float] = None
    success: bool = False
    final_outcome: Optional[str] = None
    lessons_learned: List[str] = field(default_factory=list)
    manual_intervention_required: bool = False
    escalation_level: int = 0

    def add_action(self, action: RecoveryAction) -> None:
        """Add recovery action to incident."""
        self.actions_taken.append(action)

    def complete_incident(
        self, success: bool, final_outcome: str, lessons_learned: Optional[List[str]] = None
    ) -> None:
        """Mark incident as completed."""
        self.completion_timestamp = datetime.now()
        self.total_duration_seconds = (
            self.completion_timestamp - self.trigger_timestamp
        ).total_seconds()
        self.current_status = RecoveryStatus.COMPLETED if success else RecoveryStatus.FAILED
        self.success = success
        self.final_outcome = final_outcome
        if lessons_learned:
            self.lessons_learned.extend(lessons_learned)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = {
            "incident_id": self.incident_id,
            "category": self.category.value,
            "trigger_event": self.trigger_event,
            "trigger_timestamp": self.trigger_timestamp.isoformat(),
            "initial_symptoms": self.initial_symptoms,
            "actions_taken": [action.to_dict() for action in self.actions_taken],
            "current_status": self.current_status.value,
            "current_phase": self.current_phase.value,
            "success": self.success,
            "manual_intervention_required": self.manual_intervention_required,
            "escalation_level": self.escalation_level,
        }

        if self.completion_timestamp:
            result["completion_timestamp"] = self.completion_timestamp.isoformat()
        if self.total_duration_seconds is not None:
            result["total_duration_seconds"] = self.total_duration_seconds
        if self.final_outcome:
            result["final_outcome"] = self.final_outcome
        if self.lessons_learned:
            result["lessons_learned"] = self.lessons_learned

        return result


@dataclass
class RecoveryStatusSummary:
    """Summary of current recovery status across all systems."""

    timestamp: datetime
    active_incidents: int
    completed_incidents_24h: int
    failed_recoveries_24h: int
    average_recovery_time_minutes: float
    system_health_score: float  # 0.0 to 1.0
    current_degradations: List[str]
    recent_interventions: List[str]
    stability_trend: str  # "improving", "stable", "degrading"
    next_maintenance_window: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = {
            "timestamp": self.timestamp.isoformat(),
            "active_incidents": self.active_incidents,
            "completed_incidents_24h": self.completed_incidents_24h,
            "failed_recoveries_24h": self.failed_recoveries_24h,
            "average_recovery_time_minutes": self.average_recovery_time_minutes,
            "system_health_score": self.system_health_score,
            "current_degradations": self.current_degradations,
            "recent_interventions": self.recent_interventions,
            "stability_trend": self.stability_trend,
        }

        if self.next_maintenance_window:
            result["next_maintenance_window"] = self.next_maintenance_window.isoformat()

        return result


class RecoveryStatusReporter:
    """Automated recovery status reporting system."""

    def __init__(
        self,
        config: StreamingConfig,
        base_logger: StructuredLogger,
        operational_logger: Optional[UnattendedOperationLogger] = None,
        recovery_manager: Optional[ServiceRecoveryManager] = None,
    ):
        """Initialize recovery status reporter."""
        self.config = config
        self.base_logger = base_logger
        self.operational_logger = operational_logger
        self.recovery_manager = recovery_manager

        # State tracking
        self._active_incidents: Dict[str, RecoveryIncident] = {}
        self._completed_incidents: List[RecoveryIncident] = []
        self._incident_counter = 0

        # Reporting configuration
        self.report_file = Path(f"logs/recovery_reports_{datetime.now().strftime('%Y%m%d')}.jsonl")
        self.report_file.parent.mkdir(exist_ok=True)

        # Background reporting
        self._reporting_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

        # Recovery patterns and trends
        self._recovery_patterns: Dict[str, List[float]] = {}
        self._escalation_history: List[Tuple[datetime, str, int]] = []

    async def start(self) -> None:
        """Start the recovery status reporter."""
        await self._log_operational_event(
            severity=OperationalSeverity.INFO,
            event_type=RecoveryEventType.SYSTEM_STABILIZED.value,
            message="Recovery status reporter started",
        )

        # Start background reporting
        self._reporting_task = asyncio.create_task(self._background_reporting())

    async def stop(self) -> None:
        """Stop the recovery status reporter."""
        self._shutdown_event.set()

        if self._reporting_task:
            await self._reporting_task

        await self._log_operational_event(
            severity=OperationalSeverity.INFO,
            event_type=RecoveryEventType.SYSTEM_STABILIZED.value,
            message="Recovery status reporter stopped",
            details={
                "total_incidents_tracked": len(self._completed_incidents),
                "active_incidents_at_shutdown": len(self._active_incidents),
            },
        )

    async def report_recovery_initiated(
        self,
        category: RecoveryCategory,
        trigger_event: str,
        symptoms: List[str],
        incident_id: Optional[str] = None,
    ) -> str:
        """Report that a recovery operation has been initiated."""
        if not incident_id:
            self._incident_counter += 1
            incident_id = f"REC-{datetime.now().strftime('%Y%m%d')}-{self._incident_counter:04d}"

        incident = RecoveryIncident(
            incident_id=incident_id,
            category=category,
            trigger_event=trigger_event,
            trigger_timestamp=datetime.now(),
            initial_symptoms=symptoms,
            current_status=RecoveryStatus.IN_PROGRESS,
            current_phase=RecoveryPhase.VALIDATION,
        )

        self._active_incidents[incident_id] = incident

        await self._log_operational_event(
            severity=OperationalSeverity.WARNING,
            event_type=RecoveryEventType.RECOVERY_INITIATED.value,
            message=f"Recovery initiated for {category.value}",
            details={
                "incident_id": incident_id,
                "trigger_event": trigger_event,
                "symptoms": symptoms,
                "category": category.value,
            },
        )

        await self._write_recovery_report(incident, RecoveryEventType.RECOVERY_INITIATED)

        return incident_id

    async def report_recovery_action(
        self,
        incident_id: str,
        action_type: str,
        description: str,
        success: bool = True,
        duration_seconds: Optional[float] = None,
        error_message: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Report a recovery action taken."""
        if incident_id not in self._active_incidents:
            await self.base_logger.warning(
                f"Recovery action reported for unknown incident: {incident_id}"
            )
            return

        action = RecoveryAction(
            action_id=f"{incident_id}-{len(self._active_incidents[incident_id].actions_taken):03d}",
            action_type=action_type,
            description=description,
            timestamp=datetime.now(),
            duration_seconds=duration_seconds,
            success=success,
            error_message=error_message,
            parameters=parameters,
        )

        self._active_incidents[incident_id].add_action(action)

        severity = OperationalSeverity.INFO if success else OperationalSeverity.WARNING

        await self._log_operational_event(
            severity=severity,
            event_type=RecoveryEventType.RECOVERY_PROGRESS.value,
            message=f"Recovery action taken: {description}",
            details={
                "incident_id": incident_id,
                "action_type": action_type,
                "success": success,
                "duration_seconds": duration_seconds,
                "error_message": error_message,
            },
        )

    async def report_phase_transition(
        self,
        incident_id: str,
        new_phase: RecoveryPhase,
        phase_details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Report transition to a new recovery phase."""
        if incident_id not in self._active_incidents:
            return

        old_phase = self._active_incidents[incident_id].current_phase
        self._active_incidents[incident_id].current_phase = new_phase

        await self._log_operational_event(
            severity=OperationalSeverity.INFO,
            event_type=RecoveryEventType.RECOVERY_PROGRESS.value,
            message=f"Recovery phase transition: {old_phase.value} -> {new_phase.value}",
            details={
                "incident_id": incident_id,
                "old_phase": old_phase.value,
                "new_phase": new_phase.value,
                **(phase_details or {}),
            },
        )

    async def report_recovery_completed(
        self,
        incident_id: str,
        success: bool,
        final_outcome: str,
        lessons_learned: Optional[List[str]] = None,
    ) -> None:
        """Report that a recovery operation has completed."""
        if incident_id not in self._active_incidents:
            return

        incident = self._active_incidents[incident_id]
        incident.complete_incident(success, final_outcome, lessons_learned)

        # Move to completed incidents
        self._completed_incidents.append(incident)
        del self._active_incidents[incident_id]

        # Track recovery patterns
        category_key = incident.category.value
        if category_key not in self._recovery_patterns:
            self._recovery_patterns[category_key] = []
        if incident.total_duration_seconds:
            self._recovery_patterns[category_key].append(incident.total_duration_seconds)
            # Keep only last 50 recovery times for pattern analysis
            if len(self._recovery_patterns[category_key]) > 50:
                self._recovery_patterns[category_key] = self._recovery_patterns[category_key][-50:]

        severity = OperationalSeverity.INFO if success else OperationalSeverity.ERROR
        event_type = (
            RecoveryEventType.RECOVERY_COMPLETED if success else RecoveryEventType.RECOVERY_FAILED
        )

        await self._log_operational_event(
            severity=severity,
            event_type=event_type.value,
            message=f"Recovery {'completed successfully' if success else 'failed'}: {final_outcome}",
            details={
                "incident_id": incident_id,
                "success": success,
                "total_duration_seconds": incident.total_duration_seconds,
                "actions_count": len(incident.actions_taken),
                "final_outcome": final_outcome,
                "lessons_learned": lessons_learned or [],
            },
        )

        await self._write_recovery_report(incident, event_type)

    async def report_manual_intervention_required(
        self,
        incident_id: str,
        intervention_reason: str,
        escalation_level: int = 1,
        contact_info: Optional[Dict[str, str]] = None,
    ) -> None:
        """Report that manual intervention is required."""
        if incident_id not in self._active_incidents:
            return

        incident = self._active_incidents[incident_id]
        incident.manual_intervention_required = True
        incident.escalation_level = escalation_level

        # Track escalation
        self._escalation_history.append((datetime.now(), incident_id, escalation_level))

        await self._log_operational_event(
            severity=OperationalSeverity.CRITICAL,
            event_type=RecoveryEventType.MANUAL_INTERVENTION_REQUIRED.value,
            message=f"Manual intervention required: {intervention_reason}",
            details={
                "incident_id": incident_id,
                "intervention_reason": intervention_reason,
                "escalation_level": escalation_level,
                "contact_info": contact_info or {},
                "incident_age_minutes": (
                    datetime.now() - incident.trigger_timestamp
                ).total_seconds()
                / 60,
            },
        )

    async def get_recovery_status_summary(self) -> RecoveryStatusSummary:
        """Get comprehensive recovery status summary."""
        now = datetime.now()
        last_24h = now - timedelta(hours=24)

        # Count recent completed incidents
        recent_completed = [
            incident
            for incident in self._completed_incidents
            if incident.completion_timestamp and incident.completion_timestamp >= last_24h
        ]

        completed_24h = len(recent_completed)
        failed_24h = len([i for i in recent_completed if not i.success])

        # Calculate average recovery time
        recovery_times = [
            i.total_duration_seconds
            for i in recent_completed
            if i.total_duration_seconds is not None
        ]
        avg_recovery_time = (
            (sum(recovery_times) / len(recovery_times) / 60) if recovery_times else 0.0
        )

        # Calculate system health score
        health_score = self._calculate_health_score()

        # Identify current degradations
        degradations = []
        for incident in self._active_incidents.values():
            if incident.current_status == RecoveryStatus.IN_PROGRESS:
                degradations.append(f"{incident.category.value}: {incident.trigger_event}")

        # Recent interventions
        recent_interventions = []
        recent_escalations = [
            (timestamp, incident_id, level)
            for timestamp, incident_id, level in self._escalation_history
            if timestamp >= last_24h
        ]
        for timestamp, incident_id, level in recent_escalations[-5:]:  # Last 5
            recent_interventions.append(f"L{level} escalation for {incident_id}")

        # Stability trend analysis
        stability_trend = self._analyze_stability_trend()

        return RecoveryStatusSummary(
            timestamp=now,
            active_incidents=len(self._active_incidents),
            completed_incidents_24h=completed_24h,
            failed_recoveries_24h=failed_24h,
            average_recovery_time_minutes=avg_recovery_time,
            system_health_score=health_score,
            current_degradations=degradations,
            recent_interventions=recent_interventions,
            stability_trend=stability_trend,
        )

    async def generate_recovery_report(self, incident_id: Optional[str] = None) -> Dict[str, Any]:
        """Generate detailed recovery report."""
        if incident_id:
            # Report on specific incident
            incident = self._active_incidents.get(incident_id) or next(
                (i for i in self._completed_incidents if i.incident_id == incident_id), None
            )

            if not incident:
                return {"error": f"Incident {incident_id} not found"}

            return {
                "report_type": "incident_report",
                "incident": incident.to_dict(),
                "generated_at": datetime.now().isoformat(),
            }
        # Generate summary report
        status_summary = await self.get_recovery_status_summary()

        # Add recovery patterns analysis
        pattern_analysis = {}
        for category, times in self._recovery_patterns.items():
            if times:
                pattern_analysis[category] = {
                    "average_time_minutes": sum(times) / len(times) / 60,
                    "min_time_minutes": min(times) / 60,
                    "max_time_minutes": max(times) / 60,
                    "sample_count": len(times),
                }

        return {
            "report_type": "summary_report",
            "status_summary": status_summary.to_dict(),
            "active_incidents": [i.to_dict() for i in self._active_incidents.values()],
            "recent_completed": [
                i.to_dict()
                for i in self._completed_incidents[-10:]  # Last 10
            ],
            "recovery_patterns": pattern_analysis,
            "generated_at": datetime.now().isoformat(),
        }

    def _calculate_health_score(self) -> float:
        """Calculate overall system health score (0.0 to 1.0)."""
        base_score = 1.0

        # Deduct for active incidents
        active_penalty = len(self._active_incidents) * 0.1
        base_score -= min(active_penalty, 0.5)  # Max 50% penalty for active incidents

        # Deduct for recent failures
        now = datetime.now()
        recent_failures = [
            i
            for i in self._completed_incidents[-20:]  # Last 20 incidents
            if not i.success
            and i.completion_timestamp
            and (now - i.completion_timestamp) <= timedelta(hours=24)
        ]
        failure_penalty = len(recent_failures) * 0.05
        base_score -= min(failure_penalty, 0.3)  # Max 30% penalty for recent failures

        # Deduct for manual interventions
        recent_escalations = [
            level
            for timestamp, _, level in self._escalation_history
            if (now - timestamp) <= timedelta(hours=24)
        ]
        escalation_penalty = sum(recent_escalations) * 0.02
        base_score -= min(escalation_penalty, 0.2)  # Max 20% penalty for escalations

        return max(base_score, 0.0)

    def _analyze_stability_trend(self) -> str:
        """Analyze system stability trend."""
        now = datetime.now()

        # Compare last 6 hours vs previous 6 hours
        last_6h = now - timedelta(hours=6)
        prev_6h = now - timedelta(hours=12)

        recent_incidents = len(
            [i for i in self._completed_incidents if i.trigger_timestamp >= last_6h]
        )

        previous_incidents = len(
            [i for i in self._completed_incidents if prev_6h <= i.trigger_timestamp < last_6h]
        )

        if recent_incidents < previous_incidents:
            return "improving"
        if recent_incidents > previous_incidents:
            return "degrading"
        return "stable"

    async def _log_operational_event(
        self,
        severity: OperationalSeverity,
        event_type: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log operational event through operational logger if available."""
        if self.operational_logger:
            await self.operational_logger.log_operational_event(
                severity=severity,
                category=DiagnosticCategory.ERROR_RECOVERY,
                event_type=event_type,
                message=message,
                details=details,
            )
        else:
            # Fallback to structured logger
            log_level = {
                OperationalSeverity.INFO: LogLevel.INFO,
                OperationalSeverity.WARNING: LogLevel.WARNING,
                OperationalSeverity.ERROR: LogLevel.ERROR,
                OperationalSeverity.CRITICAL: LogLevel.CRITICAL,
            }.get(severity, LogLevel.INFO)

            await self.base_logger.log(
                log_level, message, LogContext(operation="recovery_reporting"), details
            )

    async def _write_recovery_report(
        self, incident: RecoveryIncident, event_type: RecoveryEventType
    ) -> None:
        """Write recovery report to dedicated report file."""
        try:
            report_entry = {
                "timestamp": datetime.now().isoformat(),
                "event_type": event_type.value,
                "incident": incident.to_dict(),
            }

            with open(self.report_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(report_entry, separators=(",", ":")) + "\n")

        except Exception as e:
            await self.base_logger.error(
                f"Failed to write recovery report: {e}",
                extra_data={"error": str(e), "incident_id": incident.incident_id},
            )

    async def _background_reporting(self) -> None:
        """Background task for periodic status reporting."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(600)  # 10-minute intervals

                # Generate and log periodic status summary
                summary = await self.get_recovery_status_summary()

                await self._log_operational_event(
                    severity=OperationalSeverity.INFO,
                    event_type="periodic_recovery_status",
                    message="Periodic recovery status report",
                    details={
                        "active_incidents": summary.active_incidents,
                        "system_health_score": summary.system_health_score,
                        "stability_trend": summary.stability_trend,
                        "average_recovery_time_minutes": summary.average_recovery_time_minutes,
                    },
                )

                # Check for long-running incidents that may need escalation
                await self._check_incident_escalation()

            except asyncio.CancelledError:
                break
            except Exception as e:
                await self._log_operational_event(
                    severity=OperationalSeverity.ERROR,
                    event_type="background_reporting_error",
                    message=f"Background reporting encountered error: {e}",
                    details={"error": str(e)},
                )

    async def _check_incident_escalation(self) -> None:
        """Check if any incidents need escalation due to duration."""
        now = datetime.now()

        for incident in self._active_incidents.values():
            incident_age = (now - incident.trigger_timestamp).total_seconds() / 3600  # hours

            # Escalation thresholds
            if incident_age > 4 and incident.escalation_level == 0:  # 4 hours
                await self.report_manual_intervention_required(
                    incident.incident_id,
                    f"Incident running for {incident_age:.1f} hours without resolution",
                    escalation_level=1,
                )
            elif incident_age > 8 and incident.escalation_level == 1:  # 8 hours
                await self.report_manual_intervention_required(
                    incident.incident_id,
                    f"Critical: Incident running for {incident_age:.1f} hours",
                    escalation_level=2,
                )
