import React from 'react';

export default function SummaryTab({ result }) {
  const { summary, chart_type, chart_config } = result;

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
    </div>
  );
}
