import React, { useState } from 'react';
import { Target, X, Eye, Edit3, Layers } from 'lucide-react';

/**
 * FocusBanner — Shows active focus context in the chat area.
 * Displays focus name, table count, rules, and provides Clear/View actions.
 * Communicates the tiered architecture: focus tables are prioritized, not hard-blocked.
 */
export default function FocusBanner({ focus, onClear, onViewTables }) {
  const [expanded, setExpanded] = useState(false);

  if (!focus) return null;

  const tableCount = focus.table_count || focus.tables?.length || 0;
  const ruleCount = focus.rule_count || focus.business_rules?.length || 0;
  const queryCount = focus.query_count || focus.verified_queries?.length || 0;

  return (
    <div className="focus-banner">
      <div className="focus-banner-main">
        <Target size={14} className="focus-banner-icon" />
        <div className="focus-banner-info">
          <span className="focus-banner-name">{focus.name}</span>
          <span className="focus-banner-stats">
            {tableCount} priority tables
            {ruleCount > 0 && ` · ${ruleCount} rules`}
            {queryCount > 0 && ` · ${queryCount} verified queries`}
          </span>
        </div>
        <div className="focus-banner-actions">
          <button
            className="focus-banner-btn"
            onClick={() => setExpanded(!expanded)}
            title="View focus details"
          >
            <Eye size={12} /> {expanded ? 'Hide' : 'Details'}
          </button>
          <button
            className="focus-banner-btn focus-banner-btn-clear"
            onClick={onClear}
            title="Clear focus"
          >
            <X size={12} /> Clear
          </button>
        </div>
      </div>

      <div className="focus-banner-hint">
        <Layers size={11} />
        <span>Priority tables boosted 5×. All 1,200+ tables still available at normal weight.</span>
      </div>

      {expanded && (
        <div className="focus-banner-details">
          {focus.tables?.length > 0 && (
            <div className="focus-banner-detail-section">
              <strong>Priority Tables:</strong>
              <div className="focus-banner-pills">
                {focus.tables.slice(0, 20).map(t => (
                  <span key={t} className="focus-banner-pill">{t}</span>
                ))}
                {focus.tables.length > 20 && (
                  <span className="focus-banner-pill focus-banner-pill-more">
                    +{focus.tables.length - 20} more
                  </span>
                )}
              </div>
            </div>
          )}
          {focus.business_rules?.length > 0 && (
            <div className="focus-banner-detail-section">
              <strong>Business Rules:</strong>
              <ul className="focus-banner-rules">
                {focus.business_rules.map((r, i) => (
                  <li key={i}>{r.rule || r}</li>
                ))}
              </ul>
            </div>
          )}
          {focus.verified_queries?.length > 0 && (
            <div className="focus-banner-detail-section">
              <strong>Verified Queries:</strong>
              <ul className="focus-banner-rules">
                {focus.verified_queries.slice(0, 5).map((q, i) => (
                  <li key={i}>{q.question || q}</li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
