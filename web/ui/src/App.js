import React, { useState, useRef, useEffect } from 'react';
import QueryInput from './components/QueryInput';
import SQLDisplay from './components/SQLDisplay';
import DataTable from './components/DataTable';
import ChartPanel from './components/ChartPanel';
import Summary from './components/Summary';
import FeedbackPanel from './components/FeedbackPanel';
import './App.css';

const API_BASE = process.env.REACT_APP_API_URL || '';

function App() {
  const [question, setQuestion] = useState('');
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [history, setHistory] = useState([]);
  const resultRef = useRef(null);

  const handleSubmit = async (q) => {
    const text = q || question;
    if (!text.trim()) return;

    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const resp = await fetch(`${API_BASE}/api/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: text }),
      });

      if (!resp.ok) {
        const detail = await resp.json().catch(() => ({}));
        throw new Error(detail.detail || `HTTP ${resp.status}`);
      }

      const data = await resp.json();
      setResult(data);
      setHistory(prev => [{ question: text, ...data }, ...prev].slice(0, 20));
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleFeedback = async (feedback, correction) => {
    if (!result?.query_id) return;
    try {
      await fetch(`${API_BASE}/api/feedback`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          query_id: result.query_id,
          feedback,
          correction_sql: correction || null,
        }),
      });
    } catch (err) {
      console.error('Feedback failed:', err);
    }
  };

  useEffect(() => {
    if (result && resultRef.current) {
      resultRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [result]);

  return (
    <div className="app">
      <header className="header">
        <h1 className="logo">RAVEN</h1>
        <span className="subtitle">Retrieval-Augmented Validated Engine for Natural-language SQL</span>
      </header>

      <main className="main">
        <QueryInput
          value={question}
          onChange={setQuestion}
          onSubmit={() => handleSubmit()}
          loading={loading}
        />

        {error && <div className="error-banner">{error}</div>}

        {loading && (
          <div className="loading">
            <div className="spinner" />
            <span>Generating SQL...</span>
          </div>
        )}

        {result && (
          <div ref={resultRef} className="results">
            {result.status === 'ambiguous' && (
              <div className="ambiguous-banner">{result.message}</div>
            )}

            {result.sql && (
              <>
                <div className="result-header">
                  <ConfidenceBadge confidence={result.confidence} />
                  <span className="difficulty-tag">{result.difficulty}</span>
                  <span className="timing">
                    {Object.values(result.timings || {}).reduce((a, b) => a + b, 0).toFixed(1)}s
                  </span>
                </div>

                <SQLDisplay sql={result.sql} />

                {result.summary && <Summary text={result.summary} />}

                {result.data?.length > 0 && (
                  <DataTable data={result.data} />
                )}

                {result.chart_config && Object.keys(result.chart_config).length > 0 && (
                  <ChartPanel config={result.chart_config} chartType={result.chart_type} />
                )}

                <FeedbackPanel
                  onFeedback={handleFeedback}
                  queryId={result.query_id}
                />
              </>
            )}
          </div>
        )}

        {history.length > 1 && (
          <div className="history">
            <h3>Recent queries</h3>
            {history.slice(1).map((h, i) => (
              <button
                key={i}
                className="history-item"
                onClick={() => {
                  setQuestion(h.question);
                  handleSubmit(h.question);
                }}
              >
                {h.question}
              </button>
            ))}
          </div>
        )}
      </main>
    </div>
  );
}

function ConfidenceBadge({ confidence }) {
  const colors = { HIGH: '#22c55e', MEDIUM: '#f59e0b', LOW: '#ef4444' };
  return (
    <span
      className="confidence-badge"
      style={{ backgroundColor: colors[confidence] || colors.LOW }}
    >
      {confidence}
    </span>
  );
}

export default App;
