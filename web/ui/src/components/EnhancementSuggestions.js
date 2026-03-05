import React, { useState } from 'react';
import { Lightbulb, Plus, FileText, MessageSquare, AlertTriangle, Check, X, Edit3 } from 'lucide-react';

const API_BASE = process.env.REACT_APP_API_URL || '';

const SUGGESTION_ICONS = {
  add_table: <Plus size={13} />,
  add_rule: <FileText size={13} />,
  add_query: <MessageSquare size={13} />,
  add_note: <AlertTriangle size={13} />,
};

const SUGGESTION_LABELS = {
  add_table: 'Add table',
  add_rule: 'Add business rule',
  add_query: 'Save as verified query',
  add_note: 'Add column note',
};

/**
 * EnhancementSuggestions — Shows Living Document enhancement suggestions
 * below a response card when focus mode is active and the pipeline
 * detected something worth adding to the focus document.
 */
export default function EnhancementSuggestions({ suggestions, focusName, focusId }) {
  const [dismissed, setDismissed] = useState(new Set());
  const [accepted, setAccepted] = useState(new Set());

  if (!suggestions || suggestions.length === 0 || !focusId) return null;

  const handleAccept = async (suggestion, index) => {
    try {
      await fetch(`${API_BASE}/api/focus/suggestions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          document_id: focusId,
          suggestion_type: suggestion.type,
          suggestion_data: suggestion.data,
        }),
      });
      setAccepted(prev => new Set([...prev, index]));
    } catch (e) {
      console.error('Enhancement suggestion failed:', e);
    }
  };

  const handleDismiss = (index) => {
    setDismissed(prev => new Set([...prev, index]));
  };

  const visible = suggestions.filter((_, i) => !dismissed.has(i) && !accepted.has(i));
  if (visible.length === 0) return null;

  return (
    <div className="enhancement-suggestions">
      <div className="enhancement-suggestions-header">
        <Lightbulb size={14} />
        <span>Enhance "{focusName}"</span>
      </div>

      {suggestions.map((s, i) => {
        if (dismissed.has(i)) return null;
        if (accepted.has(i)) {
          return (
            <div key={i} className="enhancement-suggestion accepted">
              <Check size={13} /> Added to focus document
            </div>
          );
        }

        return (
          <div key={i} className="enhancement-suggestion">
            <div className="enhancement-suggestion-icon">
              {SUGGESTION_ICONS[s.type] || <Plus size={13} />}
            </div>
            <div className="enhancement-suggestion-body">
              <div className="enhancement-suggestion-label">
                {SUGGESTION_LABELS[s.type] || s.type}: <strong>{s.data?.table || s.data?.column || s.data?.rule || 'item'}</strong>
              </div>
              {s.reason && (
                <div className="enhancement-suggestion-reason">{s.reason}</div>
              )}
            </div>
            <div className="enhancement-suggestion-actions">
              <button
                className="enhancement-btn accept"
                onClick={() => handleAccept(s, i)}
                title="Accept"
              >
                <Check size={12} /> Accept
              </button>
              <button
                className="enhancement-btn dismiss"
                onClick={() => handleDismiss(i)}
                title="Skip"
              >
                <X size={12} /> Skip
              </button>
            </div>
          </div>
        );
      })}
    </div>
  );
}
