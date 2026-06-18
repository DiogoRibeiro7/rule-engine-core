from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable, Dict, List, Optional


@dataclass
class RuleSpec:
    rule_id: str
    description: str
    sinks: List[Any]
    fn: Callable
    duration: Optional[timedelta] = None
    slide: Optional[timedelta] = None
    cron: Optional[str] = None
    timezone: str = "UTC"
    lookback: Optional[timedelta] = None


EVENT_RULES: List[RuleSpec] = []
WINDOW_RULES: List[RuleSpec] = []
SCHEDULED_RULES: List[RuleSpec] = []
