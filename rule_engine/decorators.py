from __future__ import annotations

from datetime import timedelta
from typing import Any, Callable, List, Optional

from .registry import EVENT_RULES, SCHEDULED_RULES, WINDOW_RULES, RuleSpec


def event_rule(rule_id: str, description: str = "", sinks: Optional[List[Any]] = None):
    sinks = sinks or []

    def decorator(fn: Callable):
        EVENT_RULES.append(RuleSpec(rule_id=rule_id, description=description, sinks=sinks, fn=fn))
        return fn

    return decorator


def window_rule(
    rule_id: str,
    description: str = "",
    duration: Optional[timedelta] = None,
    slide: Optional[timedelta] = None,
    sinks: Optional[List[Any]] = None,
):
    sinks = sinks or []

    def decorator(fn: Callable):
        WINDOW_RULES.append(
            RuleSpec(
                rule_id=rule_id,
                description=description,
                sinks=sinks,
                fn=fn,
                duration=duration,
                slide=slide,
            )
        )
        return fn

    return decorator


def scheduled_rule(
    rule_id: str,
    description: str = "",
    cron: str = "",
    timezone: str = "UTC",
    lookback: Optional[timedelta] = None,
    sinks: Optional[List[Any]] = None,
):
    sinks = sinks or []

    def decorator(fn: Callable):
        SCHEDULED_RULES.append(
            RuleSpec(
                rule_id=rule_id,
                description=description,
                sinks=sinks,
                fn=fn,
                cron=cron,
                timezone=timezone,
                lookback=lookback,
            )
        )
        return fn

    return decorator
