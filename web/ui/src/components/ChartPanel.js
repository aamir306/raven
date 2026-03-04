import React from 'react';

export default function ChartPanel({ config, chartType }) {
  // Vega-Lite spec display (Phase 3: integrate with vega-embed or plotly)
  if (!config || Object.keys(config).length === 0) return null;

  return (
    <div className="section">
      <div className="section-header">
        <span>Chart ({chartType})</span>
      </div>
      <div className="section-body chart-panel">
        {chartType === 'KPI' ? (
          <KPIDisplay config={config} />
        ) : (
          <div style={{ textAlign: 'center', padding: '24px' }}>
            <p style={{ color: 'var(--text-dim)', marginBottom: '12px' }}>
              Chart type: <strong>{chartType}</strong>
            </p>
            <pre style={{
              textAlign: 'left',
              fontSize: '0.8rem',
              maxHeight: '200px',
              overflow: 'auto',
              background: 'var(--bg)',
              padding: '12px',
              borderRadius: '6px',
            }}>
              {JSON.stringify(config, null, 2)}
            </pre>
            <p style={{ color: 'var(--text-dim)', fontSize: '0.8rem', marginTop: '8px' }}>
              Interactive chart rendering available in Phase 3
            </p>
          </div>
        )}
      </div>
    </div>
  );
}

function KPIDisplay({ config }) {
  const value = config.value ?? config.data?.[0]?.value ?? '—';
  const title = config.title ?? 'KPI';

  return (
    <div style={{
      textAlign: 'center',
      padding: '32px',
    }}>
      <div style={{
        fontSize: '0.85rem',
        color: 'var(--text-dim)',
        textTransform: 'uppercase',
        letterSpacing: '1px',
        marginBottom: '8px',
      }}>
        {title}
      </div>
      <div style={{
        fontSize: '3rem',
        fontWeight: 800,
        color: 'var(--accent)',
      }}>
        {typeof value === 'number' ? value.toLocaleString() : value}
      </div>
    </div>
  );
}
