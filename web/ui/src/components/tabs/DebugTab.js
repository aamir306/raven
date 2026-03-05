import React, { useMemo } from 'react';
import { Database, Search, BookOpen, Clock, DollarSign, Layers, GitBranch } from 'lucide-react';

export default function DebugTab({ result }) {
  const { timings = {}, cost, debug = {}, difficulty, confidence } = result;

  const maxTime = useMemo(() => {
    const vals = Object.values(timings).filter(v => typeof v === 'number');
    return Math.max(...vals, 1);
  }, [timings]);

  const stageEntries = useMemo(() => {
    return Object.entries(timings)
      .filter(([k]) => k !== 'total')
      .map(([name, time]) => ({ name, time }));
  }, [timings]);

  return (
    <div>
      {/* Stats Grid */}
      <div className="debug-grid">
        <div className="debug-card">
          <div className="debug-card-label">
            <Database size={11} style={{ marginRight: 4 }} /> Tables Selected
          </div>
          <div className="debug-card-value">
            {debug.selected_tables?.length ?? 0}
          </div>
          {debug.selected_tables?.length > 0 && (
            <div style={{
              marginTop: 6, fontSize: '0.75rem', color: 'var(--text-muted)',
              wordBreak: 'break-all',
            }}>
              {debug.selected_tables.join(', ')}
            </div>
          )}
        </div>

        <div className="debug-card">
          <div className="debug-card-label">
            <Layers size={11} style={{ marginRight: 4 }} /> Candidates Generated
          </div>
          <div className="debug-card-value">
            {debug.candidates_count ?? (difficulty === 'COMPLEX' ? 3 : 1)}
          </div>
        </div>

        <div className="debug-card">
          <div className="debug-card-label">
            <Search size={11} style={{ marginRight: 4 }} /> Entity Matches
          </div>
          <div className="debug-card-value">{debug.entity_matches ?? 0}</div>
        </div>

        <div className="debug-card">
          <div className="debug-card-label">
            <BookOpen size={11} style={{ marginRight: 4 }} /> Glossary Matches
          </div>
          <div className="debug-card-value">{debug.glossary_matches ?? 0}</div>
        </div>

        <div className="debug-card">
          <div className="debug-card-label">
            <GitBranch size={11} style={{ marginRight: 4 }} /> Probe Evidence
          </div>
          <div className="debug-card-value">{debug.probe_count ?? 0}</div>
        </div>

        <div className="debug-card">
          <div className="debug-card-label">
            <DollarSign size={11} style={{ marginRight: 4 }} /> Total Cost
          </div>
          <div className="debug-card-value">
            ${(cost ?? 0).toFixed(4)}
          </div>
        </div>
      </div>

      {/* Stage Timings */}
      {stageEntries.length > 0 && (
        <div className="debug-stages">
          <div style={{
            fontSize: '0.75rem', textTransform: 'uppercase', letterSpacing: '1px',
            color: 'var(--text-muted)', fontWeight: 600, marginBottom: 8,
          }}>
            <Clock size={11} style={{ marginRight: 4 }} />
            Stage Latency Breakdown
          </div>
          {stageEntries.map(({ name, time }) => (
            <div key={name} className="debug-stage-row">
              <span className="debug-stage-name">{name.replace(/_/g, ' ')}</span>
              <div className="debug-stage-bar">
                <div
                  className="debug-stage-bar-fill"
                  style={{ width: `${(time / maxTime) * 100}%` }}
                />
              </div>
              <span className="debug-stage-time">{time.toFixed(2)}s</span>
            </div>
          ))}

          {timings.total != null && (
            <div className="debug-stage-row" style={{ fontWeight: 600 }}>
              <span className="debug-stage-name" style={{ color: 'var(--text)' }}>
                Total
              </span>
              <div className="debug-stage-bar" />
              <span className="debug-stage-time">{timings.total.toFixed(2)}s</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
