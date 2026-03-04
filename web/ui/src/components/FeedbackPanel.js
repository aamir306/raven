import React, { useState } from 'react';

export default function FeedbackPanel({ onFeedback, queryId }) {
  const [selected, setSelected] = useState(null);
  const [showCorrection, setShowCorrection] = useState(false);
  const [correction, setCorrection] = useState('');
  const [submitted, setSubmitted] = useState(false);

  const handleClick = (type) => {
    setSelected(type);
    if (type === 'thumbs_down') {
      setShowCorrection(true);
    } else {
      onFeedback('thumbs_up');
      setSubmitted(true);
    }
  };

  const submitCorrection = () => {
    onFeedback('thumbs_down', correction);
    setSubmitted(true);
  };

  if (submitted) {
    return (
      <div className="feedback-bar">
        <span style={{ color: 'var(--text-dim)', fontSize: '0.85rem' }}>
          Thanks for your feedback!
        </span>
      </div>
    );
  }

  return (
    <div>
      <div className="feedback-bar">
        <span style={{ color: 'var(--text-dim)', fontSize: '0.85rem' }}>
          Was this helpful?
        </span>
        <button
          className={`feedback-btn ${selected === 'thumbs_up' ? 'active-up' : ''}`}
          onClick={() => handleClick('thumbs_up')}
        >
          👍 Yes
        </button>
        <button
          className={`feedback-btn ${selected === 'thumbs_down' ? 'active-down' : ''}`}
          onClick={() => handleClick('thumbs_down')}
        >
          👎 No
        </button>
      </div>

      {showCorrection && (
        <div style={{ marginTop: '8px' }}>
          <textarea
            value={correction}
            onChange={(e) => setCorrection(e.target.value)}
            placeholder="Paste the correct SQL or describe what's wrong..."
            rows={3}
            style={{
              width: '100%',
              background: 'var(--surface)',
              border: '1px solid var(--border)',
              borderRadius: '6px',
              padding: '10px 14px',
              color: 'var(--text)',
              fontFamily: "'SF Mono', 'Fira Code', monospace",
              fontSize: '0.85rem',
              resize: 'vertical',
              outline: 'none',
            }}
          />
          <button
            className="feedback-btn"
            onClick={submitCorrection}
            style={{ marginTop: '8px' }}
          >
            Submit correction
          </button>
        </div>
      )}
    </div>
  );
}
