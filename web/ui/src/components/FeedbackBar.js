import React, { useState } from 'react';
import { ThumbsUp, ThumbsDown, MessageSquare } from 'lucide-react';

export default function FeedbackBar({ queryId, onFeedback }) {
  const [selected, setSelected] = useState(null);
  const [showCorrection, setShowCorrection] = useState(false);
  const [correction, setCorrection] = useState('');
  const [submitted, setSubmitted] = useState(false);

  const handleClick = (type) => {
    if (submitted) return;
    setSelected(type);
    if (type === 'thumbs_up') {
      onFeedback(queryId, 'thumbs_up');
      setSubmitted(true);
    } else {
      setShowCorrection(true);
    }
  };

  const submitCorrection = () => {
    onFeedback(queryId, 'thumbs_down', correction);
    setSubmitted(true);
    setShowCorrection(false);
  };

  if (submitted) {
    return (
      <div className="feedback-section">
        <span className="feedback-label" style={{ color: 'var(--success)' }}>
          Thanks for your feedback!
        </span>
      </div>
    );
  }

  return (
    <>
      <div className="feedback-section">
        <span className="feedback-label">Was this helpful?</span>
        <button
          className={`feedback-btn ${selected === 'thumbs_up' ? 'selected-up' : ''}`}
          onClick={() => handleClick('thumbs_up')}
        >
          <ThumbsUp size={13} /> Yes
        </button>
        <button
          className={`feedback-btn ${selected === 'thumbs_down' ? 'selected-down' : ''}`}
          onClick={() => handleClick('thumbs_down')}
        >
          <ThumbsDown size={13} /> No
        </button>
      </div>

      {showCorrection && (
        <div className="feedback-correction">
          <textarea
            value={correction}
            onChange={(e) => setCorrection(e.target.value)}
            placeholder="Paste the correct SQL or describe what's wrong..."
            rows={3}
          />
          <button
            className="sql-action-btn primary"
            onClick={submitCorrection}
            style={{ marginTop: 8 }}
          >
            <MessageSquare size={13} /> Submit Correction
          </button>
        </div>
      )}
    </>
  );
}
