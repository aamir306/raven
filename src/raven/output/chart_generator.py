"""
Chart Generator — Stage 7.3
==============================
Generates chart configuration (Vega-Lite compatible) from the
detected chart type and result data.

The frontend (React) consumes these configs to render charts.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class ChartGenerator:
    """Generate Vega-Lite chart configuration from detection results."""

    async def generate(
        self,
        chart_type: str,
        df: Any,
        x_axis: str | None = None,
        y_axis: str | None = None,
        title: str = "",
    ) -> dict:
        """
        Generate chart configuration from detection + data.

        Args:
            chart_type: Detected chart type (BAR, LINE, PIE, etc.).
            df: Result DataFrame.
            x_axis: Detected X axis column.
            y_axis: Detected Y axis column.
            title: Chart title.

        Returns:
            Vega-Lite-compatible chart spec dict.
        """
        chart_type = chart_type.upper()

        if chart_type == "KPI":
            return self._kpi_config(df, title)
        elif chart_type == "TABLE":
            return self._table_config(df, title)
        elif chart_type in ("BAR", "LINE", "SCATTER"):
            return self._xy_config(chart_type, df, x_axis, y_axis, title)
        elif chart_type == "PIE":
            return self._pie_config(df, x_axis, y_axis, title)
        elif chart_type == "HEATMAP":
            return self._heatmap_config(df, x_axis, y_axis, title)
        else:
            return self._table_config(df, title)

    # ── Chart Config Builders ──────────────────────────────────────────

    @staticmethod
    def _kpi_config(df: Any, title: str) -> dict:
        """Single-value KPI display."""
        value = None
        if df is not None and len(df) > 0:
            value = df.iloc[0, 0]
            if hasattr(value, "item"):
                value = value.item()  # numpy → native Python
        return {
            "type": "KPI",
            "title": title,
            "value": value,
            "columns": list(df.columns) if df is not None else [],
        }

    @staticmethod
    def _table_config(df: Any, title: str) -> dict:
        """Table display (no chart)."""
        return {
            "type": "TABLE",
            "title": title,
            "columns": list(df.columns) if df is not None else [],
            "row_count": len(df) if df is not None else 0,
        }

    @staticmethod
    def _xy_config(
        chart_type: str, df: Any,
        x_axis: str | None, y_axis: str | None, title: str,
    ) -> dict:
        """BAR, LINE, SCATTER chart configuration."""
        cols = list(df.columns) if df is not None else []
        x = x_axis or (cols[0] if cols else None)
        y = y_axis or (cols[1] if len(cols) > 1 else None)

        return {
            "type": chart_type,
            "title": title,
            "encoding": {
                "x": {"field": x, "type": "nominal" if chart_type == "BAR" else "temporal"},
                "y": {"field": y, "type": "quantitative"},
            },
            "mark": chart_type.lower(),
        }

    @staticmethod
    def _pie_config(
        df: Any, x_axis: str | None, y_axis: str | None, title: str,
    ) -> dict:
        """PIE chart configuration."""
        cols = list(df.columns) if df is not None else []
        return {
            "type": "PIE",
            "title": title,
            "encoding": {
                "theta": {"field": y_axis or (cols[1] if len(cols) > 1 else None), "type": "quantitative"},
                "color": {"field": x_axis or (cols[0] if cols else None), "type": "nominal"},
            },
            "mark": "arc",
        }

    @staticmethod
    def _heatmap_config(
        df: Any, x_axis: str | None, y_axis: str | None, title: str,
    ) -> dict:
        """HEATMAP chart configuration."""
        cols = list(df.columns) if df is not None else []
        return {
            "type": "HEATMAP",
            "title": title,
            "encoding": {
                "x": {"field": x_axis or (cols[0] if cols else None), "type": "nominal"},
                "y": {"field": y_axis or (cols[1] if len(cols) > 1 else None), "type": "nominal"},
                "color": {"field": cols[2] if len(cols) > 2 else None, "type": "quantitative"},
            },
            "mark": "rect",
        }
