import React from 'react';
import { Database } from 'lucide-react';

export default function SummaryTab({ result }) {
  const { summary, chart_type, chart_config, debug = {} } = result;

  const tablesUsed = debug.selected_tables || [];

  if (chart_type === 'KPI') {
    const value = chart_config?.value ?? chart_config?.data?.[0]?.value ?? '—';
    const title = chart_config?.title ?? 'Result';
    return (
      <div>
        <div className="kpi-display">
          <div className="kpi-title">{title}</div>
          <div className="kpi-value">
            {typeof value === 'number' ? value.toLocaleString() : value}
          </div>
        </div>
        {summary && <div className="summary-content"><p>{summary}</p></div>}
        {tablesUsed.length > 0 && <TablesUsed tables={tablesUsed} />}
      </div>
    );
  }

  return (
    <div className="summary-content">
      {summary ? (
        <p>{summary}</p>
      ) : (
        <p style={{ color: 'var(--text-muted)' }}>
          Query executed successfully. Switch to Data or Chart tab for details.
        </p>
      )}
      {tablesUsed.length > 0 && <TablesUsed tables={tablesUsed} />}
    </div>
  );
}

function TablesUsed({ tables }) {
  return (
    <div className="tables-used">
      <span className="tables-used-label">
        <Database size={12} /> Tables used:
      </span>
      {tables.map((t, i) => (
        <span key={i} className="tables-used-pill">{t}</span>
      ))}
    </div>
  );
}
