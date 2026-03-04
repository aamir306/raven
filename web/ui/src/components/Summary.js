import React from 'react';

export default function Summary({ text }) {
  if (!text) return null;

  return (
    <div className="section">
      <div className="section-header">
        <span>Summary</span>
      </div>
      <div className="section-body">
        <p className="summary-text">{text}</p>
      </div>
    </div>
  );
}
