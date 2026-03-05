import React, { useState } from 'react';
import { GitBranch, Check, ChevronDown, ChevronUp, Code, BarChart2 } from 'lucide-react';

/* Compact diff for two SQL strings — highlights differences */
function SimpleDiff({ sql1, sql2 }) {
  const lines1 = (sql1 || '').split('\n');
  const lines2 = (sql2 || '').split('\n');
  const maxLen = Math.max(lines1.length, lines2.length);

  return (
    <div className="sql-diff">
      {Array.from({ length: maxLen }, (_, i) => {
        const l1 = lines1[i] || '';
        const l2 = lines2[i] || '';
        const isDiff = l1.trim() !== l2.trim();
        return (
          <div key={i} className={`diff-row ${isDiff ? 'diff-changed' : ''}`}>
            <span className="diff-line-num">{i + 1}</span>
            <span className="diff-content">{l2 || l1}</span>
          </div>
        );
      })}
    </div>
  );
}

export default function CandidateComparison({ candidates, winnerIndex, onSelectCandidate }) {
  const [expanded, setExpanded] = useState(false);
  const [selected, setSelected] = useState(winnerIndex ?? 0);

  if (!candidates || candidates.length <= 1) return null;

  const winner = candidates[winnerIndex ?? 0];

  const handleSelect = (idx) => {
    setSelected(idx);
    if (onSelectCandidate) onSelectCandidate(idx);
  };

  return (
    <div className="candidate-comparison">
      {/* Toggle bar */}
      <button
        className="candidate-toggle"
        onClick={() => setExpanded(!expanded)}
      >
        <GitBranch size={14} />
        <span>{candidates.length} SQL candidates generated</span>
        {expanded ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
      </button>

      {expanded && (
        <div className="candidate-grid">
          {candidates.map((cand, idx) => {
            const isWinner = idx === (winnerIndex ?? 0);
            const isSelected = idx === selected;
            return (
              <div
                key={idx}
                className={`candidate-card ${isSelected ? 'candidate-selected' : ''}`}
              >
                {/* Card header */}
                <div className="candidate-card-header">
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <span className="candidate-label">
                      Candidate {idx + 1}
                    </span>
                    {isWinner && (
                      <span className="badge badge-success" style={{ fontSize: 10 }}>
                        Winner
                      </span>
                    )}
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    {cand.confidence && (
                      <span className={`badge badge-${
                        cand.confidence >= 0.8 ? 'success' : cand.confidence >= 0.5 ? 'warning' : 'error'
                      }`} style={{ fontSize: 10 }}>
                        {(cand.confidence * 100).toFixed(0)}%
                      </span>
                    )}
                    <button
                      className={`btn-select ${isSelected ? 'btn-selected' : ''}`}
                      onClick={() => handleSelect(idx)}
                      title="Prefer this candidate"
                    >
                      <Check size={12} />
                      {isSelected ? 'Selected' : 'Select'}
                    </button>
                  </div>
                </div>

                {/* SQL preview */}
                <div className="candidate-sql">
                  <div style={{ display: 'flex', alignItems: 'center', gap: 4, marginBottom: 6 }}>
                    <Code size={12} />
                    <span style={{ fontSize: 11, fontWeight: 600 }}>SQL</span>
                  </div>
                  <pre className="candidate-sql-code">
                    {(cand.sql || cand.query || '').trim().substring(0, 500)}
                    {((cand.sql || cand.query || '').length > 500) ? '\n...' : ''}
                  </pre>
                </div>

                {/* Diff against winner */}
                {!isWinner && (
                  <div className="candidate-diff-section">
                    <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>
                      Diff vs. Winner
                    </span>
                    <SimpleDiff sql1={winner?.sql || winner?.query || ''} sql2={cand.sql || cand.query || ''} />
                  </div>
                )}

                {/* Stats */}
                <div className="candidate-stats">
                  <div className="candidate-stat">
                    <BarChart2 size={11} />
                    <span>{cand.row_count ?? '?'} rows</span>
                  </div>
                  {cand.execution_time && (
                    <div className="candidate-stat">
                      <span>{cand.execution_time}s</span>
                    </div>
                  )}
                  {cand.cost && (
                    <div className="candidate-stat">
                      <span>${typeof cand.cost === 'number' ? cand.cost.toFixed(4) : cand.cost}</span>
                    </div>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
