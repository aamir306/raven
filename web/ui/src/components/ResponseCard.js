import React, { useState, useMemo, useCallback, lazy, Suspense } from 'react';
import { Tabs } from 'antd';
import {
  FileText, BarChart3, Table2, Code2, Bug, Brain,
  CheckCircle, Clock, DollarSign, Zap, GitBranch, Database,
  Copy, Check, Download, Image, Share2
} from 'lucide-react';
import SummaryTab from './tabs/SummaryTab';
import ChartTab from './tabs/ChartTab';
import DataTab from './tabs/DataTab';
import SQLTab from './tabs/SQLTab';
import DebugTab from './tabs/DebugTab';
import ThinkingTab from './tabs/ThinkingTab';
import FeedbackBar from './FeedbackBar';
import CandidateComparison from './CandidateComparison';
import QueryRefinement from './QueryRefinement';

const SchemaExplorer = lazy(() => import('./SchemaExplorer'));

const TAB_CONFIG = [
  { key: 'summary',  label: 'Summary',  icon: <FileText size={14} /> },
  { key: 'chart',    label: 'Chart',    icon: <BarChart3 size={14} /> },
  { key: 'data',     label: 'Data',     icon: <Table2 size={14} /> },
  { key: 'sql',      label: 'SQL',      icon: <Code2 size={14} /> },
  { key: 'thinking', label: 'Thinking', icon: <Brain size={14} /> },
  { key: 'debug',    label: 'Debug',    icon: <Bug size={14} /> },
];

export default function ResponseCard({ result, visibleTabs, onFeedback, onRerun, theme }) {
  const [activeTab, setActiveTab] = useState('summary');
  const [showSchema, setShowSchema] = useState(false);
  const [copiedSQL, setCopiedSQL] = useState(false);

  const totalTime = useMemo(() => {
    const t = result.timings?.total;
    return t != null ? t.toFixed(1) : '—';
  }, [result.timings]);

  const cost = useMemo(() => {
    return result.cost != null ? `$${result.cost.toFixed(3)}` : '';
  }, [result.cost]);

  const tabs = TAB_CONFIG.filter(t => visibleTabs.includes(t.key));

  const handleCandidateSelect = (idx) => {
    console.log('User preferred candidate:', idx);
  };

  const handleRefine = (refinement) => {
    if (onRerun && refinement.type !== 'clear') {
      let suffix = '';
      if (refinement.type === 'date_range') {
        suffix = ` between ${refinement.start} and ${refinement.end}`;
      } else if (refinement.type === 'filters') {
        suffix = ' ' + refinement.filters.map(f => f.label.toLowerCase()).join(', ');
      }
      if (suffix && result.original_question) {
        onRerun(result.original_question + suffix);
      }
    }
  };

  const handleCopySQL = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(result.sql || '');
    } catch {
      const ta = document.createElement('textarea');
      ta.value = result.sql || '';
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
    }
    setCopiedSQL(true);
    setTimeout(() => setCopiedSQL(false), 2000);
  }, [result.sql]);

  const handleDownloadCSV = useCallback(() => {
    if (!result.data?.length) return;
    const cols = Object.keys(result.data[0]);
    const header = cols.join(',');
    const rows = result.data.map(row =>
      cols.map(c => {
        const v = row[c];
        if (v == null) return '';
        const s = String(v);
        return s.includes(',') || s.includes('"') || s.includes('\n')
          ? `"${s.replace(/"/g, '""')}"` : s;
      }).join(',')
    );
    const blob = new Blob([[header, ...rows].join('\n')], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = 'raven_results.csv'; a.click();
    URL.revokeObjectURL(url);
  }, [result.data]);

  const handleShareLink = useCallback(() => {
    const params = new URLSearchParams({ q: result.question || '' });
    const link = `${window.location.origin}?${params}`;
    navigator.clipboard.writeText(link).catch(() => {});
  }, [result.question]);

  const tabItems = tabs.map(t => ({
    key: t.key,
    label: (
      <span style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
        {t.icon} {t.label}
      </span>
    ),
    children: (
      <div className="tab-content">
        {t.key === 'summary'  && <SummaryTab result={result} />}
        {t.key === 'chart'    && <ChartTab result={result} theme={theme} />}
        {t.key === 'data'     && <DataTab result={result} />}
        {t.key === 'sql'      && <SQLTab result={result} theme={theme} onRerun={onRerun} />}
        {t.key === 'thinking' && <ThinkingTab result={result} />}
        {t.key === 'debug'    && (
          <div>
            <DebugTab result={result} />
            {result.debug?.selected_tables?.length > 0 && (
              <button
                className="btn-secondary"
                onClick={() => setShowSchema(true)}
                style={{ marginTop: 12 }}
              >
                <Database size={14} /> View Schema Explorer
              </button>
            )}
          </div>
        )}
      </div>
    ),
  }));

  return (
    <div className="response-card">
      {/* Header badges */}
      <div className="response-card-header">
        <span className={`badge badge-confidence-${result.confidence || 'LOW'}`}>
          {result.confidence || 'LOW'}
        </span>

        {result.verified && (
          <span className="badge badge-verified">
            <CheckCircle size={11} /> Verified
          </span>
        )}

        <span className="badge badge-difficulty">
          {result.difficulty}
        </span>

        {result.cached && (
          <span className="badge badge-cached">
            <Zap size={11} /> Cached
          </span>
        )}

        <div className="response-meta">
          <span title="Total latency"><Clock size={12} /> {totalTime}s</span>
          {cost && <span title="LLM cost"><DollarSign size={12} /> {cost}</span>}
          {result.row_count > 0 && (
            <span>{result.row_count.toLocaleString()} rows</span>
          )}
        </div>
      </div>

      {/* Candidate comparison (Analyst/Engineer) */}
      {visibleTabs.includes('debug') && result.debug?.candidates && (
        <CandidateComparison
          candidates={result.debug.candidates}
          winnerIndex={result.debug.winner_index ?? 0}
          onSelectCandidate={handleCandidateSelect}
        />
      )}

      {/* Tabs */}
      <Tabs
        className="response-tabs"
        activeKey={activeTab}
        onChange={setActiveTab}
        items={tabItems}
        size="small"
      />

      {/* Query refinement */}
      <QueryRefinement
        result={result}
        debug={result.debug}
        onRefine={handleRefine}
      />

      {/* Action bar */}
      <div className="action-bar">
        <button className="action-bar-btn" onClick={handleCopySQL} title="Copy SQL">
          {copiedSQL ? <><Check size={13} /> Copied</> : <><Copy size={13} /> Copy SQL</>}
        </button>
        {result.data?.length > 0 && (
          <button className="action-bar-btn" onClick={handleDownloadCSV} title="Download CSV">
            <Download size={13} /> CSV
          </button>
        )}
        <button className="action-bar-btn" onClick={handleShareLink} title="Copy share link">
          <Share2 size={13} /> Share
        </button>
      </div>

      {/* Feedback */}
      <FeedbackBar
        queryId={result.query_id}
        onFeedback={onFeedback}
      />

      {/* Schema Explorer overlay */}
      {showSchema && (
        <Suspense fallback={<div style={{ padding: 20 }}>Loading schema explorer…</div>}>
          <SchemaExplorer
            debug={result.debug}
            onClose={() => setShowSchema(false)}
          />
        </Suspense>
      )}
    </div>
  );
}
