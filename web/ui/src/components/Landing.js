import React from 'react';
import { Search, Database, Sparkles } from 'lucide-react';

export default function Landing({ suggestions, onSelect }) {
  return (
    <div className="landing">
      <div className="landing-logo">RAVEN</div>
      <div className="landing-subtitle">
        Retrieval-Augmented Validated Engine for Natural-language SQL
      </div>

      <div className="landing-steps">
        <div className="landing-step">
          <div className="landing-step-icon"><Search size={20} /></div>
          <div className="landing-step-label">Ask in plain English</div>
        </div>
        <div className="landing-step">
          <div className="landing-step-icon"><Database size={20} /></div>
          <div className="landing-step-label">RAVEN generates SQL</div>
        </div>
        <div className="landing-step">
          <div className="landing-step-icon"><Sparkles size={20} /></div>
          <div className="landing-step-label">Get data + insights</div>
        </div>
      </div>

      {suggestions.length > 0 && (
        <>
          <div style={{
            fontSize: '0.82rem',
            color: 'var(--text-muted)',
            marginBottom: 16,
            fontWeight: 600,
            letterSpacing: '0.5px',
          }}>
            Try asking...
          </div>
          <div className="suggestion-grid">
            {suggestions.map((s, i) => (
              <button
                key={i}
                className="suggestion-card"
                onClick={() => onSelect(s.question)}
              >
                <div className="suggestion-category">{s.category}</div>
                <div className="suggestion-text">{s.question}</div>
              </button>
            ))}
          </div>
        </>
      )}
    </div>
  );
}
