import React, { useState, useMemo } from 'react';
import {
  ChevronDown, ChevronRight, CheckCircle2, SkipForward, Brain,
  Database, Search, BookOpen, GitBranch, Code2, ShieldCheck, Play, Sparkles
} from 'lucide-react';

const STAGE_ICONS = {
  router: Brain,
  context_retrieval: Search,
  schema_selection: Database,
  probe_execution: GitBranch,
  sql_generation: Code2,
  validation: ShieldCheck,
  execution: Play,
  response: Sparkles,
};

const STAGE_LABELS = {
  router: 'Understood As',
  context_retrieval: 'Found Context',
  schema_selection: 'Selected Tables',
  probe_execution: 'Probe Evidence',
  sql_generation: 'Generated SQL',
  validation: 'Validation',
  execution: 'Execution',
  response: 'Prepared Response',
};

export default function ThinkingTab({ result }) {
  const { timings = {}, debug = {}, difficulty, confidence, cost } = result;
  const [expanded, setExpanded] = useState({});

  const stages = useMemo(() => {
    const items = [];
    const stageKeys = [
      'router', 'context_retrieval', 'schema_selection', 'probe_execution',
      'sql_generation', 'validation', 'execution', 'response',
    ];

    for (const key of stageKeys) {
      const time = timings[key];
      const skipped = time == null || time === 0;
      const isProbe = key === 'probe_execution';
      const isValidation = key === 'validation';
      const isSimple = difficulty === 'SIMPLE';

      if ((isProbe || isValidation) && isSimple && skipped) {
        items.push({ key, time: null, skipped: true, reason: 'Skipped — SIMPLE query' });
        continue;
      }

      const details = buildStageDetail(key, result);
      items.push({ key, time, skipped: false, details });
    }

    return items;
  }, [timings, result, difficulty]);

  const toggle = (key) => {
    setExpanded(prev => ({ ...prev, [key]: !prev[key] }));
  };

  return (
    <div className="thinking-tab">
      <div className="thinking-header">
        <span className="thinking-title">Pipeline Trace</span>
        <span className="thinking-meta">
          {Object.keys(timings).filter(k => k !== 'total').length} stages
          {timings.total != null && ` · ${timings.total.toFixed(1)}s total`}
          {cost != null && ` · $${cost.toFixed(3)}`}
        </span>
      </div>

      <div className="thinking-stages">
        {stages.map(({ key, time, skipped, reason, details }, idx) => {
          const Icon = STAGE_ICONS[key] || Brain;
          const label = STAGE_LABELS[key] || key.replace(/_/g, ' ');
          const isOpen = expanded[key];

          return (
            <div key={key} className={`thinking-stage ${skipped ? 'skipped' : ''}`}>
              <div
                className="thinking-stage-header"
                onClick={() => !skipped && details && toggle(key)}
              >
                <span className="thinking-stage-toggle">
                  {skipped ? (
                    <SkipForward size={13} />
                  ) : details ? (
                    isOpen ? <ChevronDown size={14} /> : <ChevronRight size={14} />
                  ) : (
                    <CheckCircle2 size={13} />
                  )}
                </span>
                <span className="thinking-stage-num">{idx + 1}.</span>
                <Icon size={14} className="thinking-stage-icon" />
                <span className="thinking-stage-label">{label}</span>
                {skipped ? (
                  <span className="thinking-stage-skip">{reason}</span>
                ) : (
                  time != null && <span className="thinking-stage-time">{time.toFixed(1)}s</span>
                )}
              </div>
              {isOpen && details && (
                <div className="thinking-stage-body">{details}</div>
              )}
            </div>
          );
        })}
      </div>

      {debug.raw_context && (
        <details className="thinking-raw">
          <summary>Raw Debug Payload</summary>
          <pre>{JSON.stringify(debug, null, 2)}</pre>
        </details>
      )}
    </div>
  );
}

function buildStageDetail(key, result) {
  const { debug = {}, difficulty, confidence } = result;

  switch (key) {
    case 'router':
      return (
        <div className="thinking-detail">
          <div className="thinking-detail-row">
            <span className="thinking-detail-label">Classification:</span>
            <span className={`badge badge-difficulty`}>{difficulty || 'unknown'}</span>
          </div>
          <div className="thinking-detail-row">
            <span className="thinking-detail-label">Confidence:</span>
            <span className={`badge badge-confidence-${confidence || 'LOW'}`}>{confidence || 'LOW'}</span>
          </div>
          {debug.rewritten_question && (
            <div className="thinking-detail-row">
              <span className="thinking-detail-label">Rewritten:</span>
              <span className="thinking-detail-value">"{debug.rewritten_question}"</span>
            </div>
          )}
        </div>
      );

    case 'context_retrieval':
      return (
        <div className="thinking-detail">
          {debug.keywords?.length > 0 && (
            <div className="thinking-detail-row">
              <span className="thinking-detail-label">Keywords:</span>
              <span className="thinking-detail-value">{debug.keywords.join(', ')}</span>
            </div>
          )}
          <div className="thinking-detail-row">
            <span className="thinking-detail-label">Entity matches:</span>
            <span className="thinking-detail-value">{debug.entity_matches ?? 0}</span>
          </div>
          <div className="thinking-detail-row">
            <span className="thinking-detail-label">Glossary matches:</span>
            <span className="thinking-detail-value">{debug.glossary_matches ?? 0}</span>
          </div>
          {debug.similar_query && (
            <div className="thinking-detail-row">
              <span className="thinking-detail-label">Similar query:</span>
              <span className="thinking-detail-value">
                "{debug.similar_query}" (sim: {(debug.similar_query_sim ?? 0).toFixed(2)})
              </span>
            </div>
          )}
        </div>
      );

    case 'schema_selection':
      return debug.selected_tables?.length > 0 ? (
        <div className="thinking-detail">
          {debug.selected_tables.map((t, i) => (
            <div key={i} className="thinking-detail-row">
              <span className="thinking-table-pill">
                <Database size={11} /> {t}
              </span>
            </div>
          ))}
          {debug.selected_columns?.length > 0 && (
            <div className="thinking-detail-row">
              <span className="thinking-detail-label">Columns:</span>
              <span className="thinking-detail-value">
                {debug.selected_columns.length} selected
              </span>
            </div>
          )}
        </div>
      ) : null;

    case 'probe_execution':
      return debug.probe_count > 0 ? (
        <div className="thinking-detail">
          <div className="thinking-detail-row">
            <span className="thinking-detail-label">Probes run:</span>
            <span className="thinking-detail-value">{debug.probe_count}</span>
          </div>
          {debug.probe_evidence?.map((p, i) => (
            <div key={i} className="thinking-detail-row">
              <span className="thinking-detail-value" style={{ fontSize: '0.78rem' }}>
                → {p}
              </span>
            </div>
          ))}
        </div>
      ) : null;

    case 'sql_generation':
      return (
        <div className="thinking-detail">
          <div className="thinking-detail-row">
            <span className="thinking-detail-label">Candidates:</span>
            <span className="thinking-detail-value">
              {debug.candidates_count ?? (difficulty === 'COMPLEX' ? 3 : 1)}
            </span>
          </div>
          {debug.generator_used && (
            <div className="thinking-detail-row">
              <span className="thinking-detail-label">Generator:</span>
              <span className="thinking-detail-value">{debug.generator_used}</span>
            </div>
          )}
          {debug.winner_index != null && (
            <div className="thinking-detail-row">
              <span className="thinking-detail-label">Winner:</span>
              <span className="thinking-detail-value">Candidate {debug.winner_index + 1}</span>
            </div>
          )}
        </div>
      );

    case 'validation':
      return debug.validation_errors ? (
        <div className="thinking-detail">
          <div className="thinking-detail-row">
            <span className="thinking-detail-label">Errors caught:</span>
            <span className="thinking-detail-value">{debug.validation_errors}</span>
          </div>
          {debug.validation_fixes?.length > 0 && debug.validation_fixes.map((fix, i) => (
            <div key={i} className="thinking-detail-row">
              <span className="thinking-detail-value" style={{ fontSize: '0.78rem' }}>
                ✓ Fixed: {fix}
              </span>
            </div>
          ))}
        </div>
      ) : null;

    case 'execution':
      return (
        <div className="thinking-detail">
          <div className="thinking-detail-row">
            <span className="thinking-detail-label">Rows returned:</span>
            <span className="thinking-detail-value">{result.row_count ?? 0}</span>
          </div>
          {result.chart_type && (
            <div className="thinking-detail-row">
              <span className="thinking-detail-label">Chart type:</span>
              <span className="thinking-detail-value">{result.chart_type}</span>
            </div>
          )}
        </div>
      );

    default:
      return null;
  }
}
