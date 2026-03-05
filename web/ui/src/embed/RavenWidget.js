import React, { useState, useRef, useEffect, useCallback } from 'react';
import { MessageCircle, X, Send, Loader2, ThumbsUp, ThumbsDown } from 'lucide-react';

/**
 * RavenWidget — Embeddable chat widget for RAVEN Text-to-SQL
 *
 * Usage:
 *   <RavenWidget apiUrl="https://your-api.com" token="optional-auth-token" />
 *
 * Can be embedded in any React app or via iframe.
 * Renders a floating chat button that expands into a sidebar panel.
 */

const WIDGET_STYLES = `
.raven-widget-fab {
  position: fixed; bottom: 24px; right: 24px; z-index: 10000;
  width: 56px; height: 56px; border-radius: 50%;
  background: linear-gradient(135deg, #6366f1, #8b5cf6);
  border: none; cursor: pointer; color: #fff;
  display: flex; align-items: center; justify-content: center;
  box-shadow: 0 4px 20px rgba(99,102,241,.4);
  transition: transform .2s, box-shadow .2s;
}
.raven-widget-fab:hover { transform: scale(1.08); box-shadow: 0 6px 28px rgba(99,102,241,.5); }

.raven-widget-panel {
  position: fixed; bottom: 24px; right: 24px; z-index: 10001;
  width: 380px; height: 560px; max-height: 80vh;
  background: #1a1a2e; border-radius: 16px;
  border: 1px solid rgba(255,255,255,.1);
  display: flex; flex-direction: column;
  box-shadow: 0 20px 60px rgba(0,0,0,.4);
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Inter', Roboto, sans-serif;
  animation: raven-slide-up .25s ease;
  overflow: hidden;
}

@keyframes raven-slide-up {
  from { opacity: 0; transform: translateY(20px); }
  to { opacity: 1; transform: translateY(0); }
}

.raven-widget-header {
  display: flex; align-items: center; justify-content: space-between;
  padding: 14px 16px; border-bottom: 1px solid rgba(255,255,255,.08);
  background: linear-gradient(135deg, #6366f1, #8b5cf6);
}
.raven-widget-header h3 { margin: 0; font-size: 15px; font-weight: 600; color: #fff; }
.raven-widget-header p { margin: 2px 0 0; font-size: 11px; color: rgba(255,255,255,.7); }
.raven-widget-close {
  background: rgba(255,255,255,.15); border: none; color: #fff;
  width: 28px; height: 28px; border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  cursor: pointer; transition: background .15s;
}
.raven-widget-close:hover { background: rgba(255,255,255,.25); }

.raven-widget-messages {
  flex: 1; overflow-y: auto; padding: 16px;
  display: flex; flex-direction: column; gap: 12px;
}

.raven-msg { max-width: 90%; animation: raven-fade-in .2s ease; }
.raven-msg-user {
  align-self: flex-end; background: #6366f1; color: #fff;
  padding: 8px 14px; border-radius: 14px 14px 4px 14px;
  font-size: 13px; line-height: 1.4;
}
.raven-msg-bot {
  align-self: flex-start; background: rgba(255,255,255,.06);
  border: 1px solid rgba(255,255,255,.08);
  padding: 12px 14px; border-radius: 4px 14px 14px 14px;
  font-size: 13px; color: #e2e8f0; line-height: 1.5;
}

@keyframes raven-fade-in { from { opacity: 0; transform: translateY(8px); } to { opacity: 1; transform: translateY(0); } }

.raven-msg-sql {
  margin-top: 8px; padding: 8px 10px;
  background: rgba(0,0,0,.3); border-radius: 6px;
  font-family: 'SF Mono', 'Fira Code', monospace;
  font-size: 11px; color: #a5b4fc;
  white-space: pre-wrap; word-break: break-word;
  max-height: 120px; overflow-y: auto;
}

.raven-msg-summary { color: #e2e8f0; margin-bottom: 6px; }

.raven-msg-meta {
  display: flex; gap: 8px; margin-top: 8px;
  font-size: 10px; color: rgba(255,255,255,.4);
}

.raven-msg-feedback {
  display: flex; gap: 6px; margin-top: 6px;
}
.raven-msg-feedback button {
  background: none; border: 1px solid rgba(255,255,255,.1);
  color: rgba(255,255,255,.5); border-radius: 4px;
  padding: 3px 6px; cursor: pointer; font-size: 11px;
  display: flex; align-items: center; gap: 3px;
  transition: all .15s;
}
.raven-msg-feedback button:hover { border-color: #6366f1; color: #6366f1; }
.raven-msg-feedback button.active { background: rgba(99,102,241,.2); border-color: #6366f1; color: #6366f1; }

.raven-typing {
  display: flex; gap: 4px; padding: 8px 14px;
  align-self: flex-start;
}
.raven-typing span {
  width: 6px; height: 6px; border-radius: 50%;
  background: rgba(255,255,255,.3);
  animation: raven-bounce .6s infinite alternate;
}
.raven-typing span:nth-child(2) { animation-delay: .15s; }
.raven-typing span:nth-child(3) { animation-delay: .3s; }
@keyframes raven-bounce { from { transform: translateY(0); } to { transform: translateY(-6px); } }

.raven-widget-input {
  display: flex; gap: 8px; padding: 12px 16px;
  border-top: 1px solid rgba(255,255,255,.08);
  background: rgba(0,0,0,.2);
}
.raven-widget-input input {
  flex: 1; padding: 8px 12px; border-radius: 8px;
  border: 1px solid rgba(255,255,255,.1);
  background: rgba(255,255,255,.05);
  color: #e2e8f0; font-size: 13px; outline: none;
  transition: border-color .15s;
}
.raven-widget-input input::placeholder { color: rgba(255,255,255,.3); }
.raven-widget-input input:focus { border-color: #6366f1; }
.raven-widget-send {
  width: 36px; height: 36px; border-radius: 8px;
  background: #6366f1; border: none; color: #fff;
  display: flex; align-items: center; justify-content: center;
  cursor: pointer; transition: background .15s;
}
.raven-widget-send:hover { background: #4f46e5; }
.raven-widget-send:disabled { opacity: .4; cursor: not-allowed; }

.raven-widget-welcome {
  text-align: center; padding: 24px 16px; color: rgba(255,255,255,.5);
}
.raven-widget-welcome h4 { color: #e2e8f0; font-size: 14px; margin: 8px 0 4px; }
.raven-widget-welcome p { font-size: 12px; line-height: 1.5; }

.raven-suggestions {
  display: flex; flex-wrap: wrap; gap: 6px; padding: 0 16px 12px;
}
.raven-suggestion-btn {
  background: rgba(99,102,241,.1); border: 1px solid rgba(99,102,241,.2);
  color: #a5b4fc; border-radius: 16px; padding: 4px 12px;
  font-size: 11px; cursor: pointer; transition: all .15s;
}
.raven-suggestion-btn:hover { background: rgba(99,102,241,.2); border-color: #6366f1; }

@media (max-width: 480px) {
  .raven-widget-panel { width: calc(100vw - 16px); right: 8px; bottom: 8px; height: 70vh; }
}
`;

export default function RavenWidget({
  apiUrl = '/api/query',
  token = null,
  title = 'Ask RAVEN',
  subtitle = 'AI-powered SQL assistant',
  suggestions = ['How many active users this month?', 'Show revenue trend by quarter'],
  contextTables = [],
  onQueryResult = null,
}) {
  const [open, setOpen] = useState(false);
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [feedbackGiven, setFeedbackGiven] = useState({});
  const messagesEnd = useRef(null);
  const inputRef = useRef(null);

  // Inject styles once
  useEffect(() => {
    if (document.getElementById('raven-widget-styles')) return;
    const style = document.createElement('style');
    style.id = 'raven-widget-styles';
    style.textContent = WIDGET_STYLES;
    document.head.appendChild(style);
    return () => style.remove();
  }, []);

  // Auto-scroll
  useEffect(() => {
    messagesEnd.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, loading]);

  // Focus input when opened
  useEffect(() => {
    if (open) setTimeout(() => inputRef.current?.focus(), 200);
  }, [open]);

  const sendQuery = useCallback(async (question) => {
    if (!question.trim()) return;

    setMessages(prev => [...prev, { role: 'user', text: question }]);
    setInput('');
    setLoading(true);

    try {
      const headers = { 'Content-Type': 'application/json' };
      if (token) headers['Authorization'] = `Bearer ${token}`;

      const body = { question: question.trim() };
      if (contextTables.length > 0) body.context_tables = contextTables;

      const resp = await fetch(apiUrl, {
        method: 'POST',
        headers,
        body: JSON.stringify(body),
      });

      const data = await resp.json();

      setMessages(prev => [...prev, {
        role: 'bot',
        result: data,
        text: data.summary || data.nl_answer || 'Query executed.',
        sql: data.sql,
        confidence: data.confidence,
        row_count: data.row_count,
        timings: data.timings,
        suggestions: data.suggestions || [],
        id: `msg-${Date.now()}`,
      }]);

      if (onQueryResult) onQueryResult(data);
    } catch (err) {
      setMessages(prev => [...prev, {
        role: 'bot',
        text: `Error: ${err.message}`,
        isError: true,
        id: `msg-${Date.now()}`,
      }]);
    } finally {
      setLoading(false);
    }
  }, [apiUrl, token, contextTables, onQueryResult]);

  const handleSubmit = (e) => {
    e?.preventDefault();
    sendQuery(input);
  };

  const handleFeedback = useCallback(async (msgId, vote) => {
    setFeedbackGiven(prev => ({ ...prev, [msgId]: vote }));
    try {
      const headers = { 'Content-Type': 'application/json' };
      if (token) headers['Authorization'] = `Bearer ${token}`;
      await fetch(apiUrl.replace('/query', '/feedback'), {
        method: 'POST',
        headers,
        body: JSON.stringify({ query_id: msgId, vote }),
      });
    } catch { /* silent */ }
  }, [apiUrl, token]);

  if (!open) {
    return (
      <button className="raven-widget-fab" onClick={() => setOpen(true)}>
        <MessageCircle size={24} />
      </button>
    );
  }

  return (
    <div className="raven-widget-panel">
      {/* Header */}
      <div className="raven-widget-header">
        <div>
          <h3>{title}</h3>
          <p>{subtitle}</p>
        </div>
        <button className="raven-widget-close" onClick={() => setOpen(false)}>
          <X size={16} />
        </button>
      </div>

      {/* Messages */}
      <div className="raven-widget-messages">
        {messages.length === 0 && (
          <div className="raven-widget-welcome">
            <MessageCircle size={32} style={{ color: '#6366f1' }} />
            <h4>Welcome to {title}</h4>
            <p>Ask a question about your data in plain English and get instant SQL results.</p>
          </div>
        )}

        {messages.map((msg, i) => (
          <div key={i} className={`raven-msg ${msg.role === 'user' ? 'raven-msg-user' : 'raven-msg-bot'}`}>
            {msg.role === 'user' ? (
              msg.text
            ) : (
              <>
                <div className="raven-msg-summary">{msg.text}</div>
                {msg.sql && <div className="raven-msg-sql">{msg.sql}</div>}
                <div className="raven-msg-meta">
                  {msg.confidence && <span>{msg.confidence}</span>}
                  {msg.row_count != null && <span>{msg.row_count} rows</span>}
                  {msg.timings?.total && <span>{msg.timings.total.toFixed(1)}s</span>}
                </div>
                {!msg.isError && msg.id && (
                  <div className="raven-msg-feedback">
                    <button
                      className={feedbackGiven[msg.id] === 'up' ? 'active' : ''}
                      onClick={() => handleFeedback(msg.id, 'up')}
                    >
                      <ThumbsUp size={11} />
                    </button>
                    <button
                      className={feedbackGiven[msg.id] === 'down' ? 'active' : ''}
                      onClick={() => handleFeedback(msg.id, 'down')}
                    >
                      <ThumbsDown size={11} />
                    </button>
                  </div>
                )}
                {msg.suggestions?.length > 0 && (
                  <div className="raven-suggestions" style={{ marginTop: 8, padding: 0 }}>
                    {msg.suggestions.map((s, j) => (
                      <button key={j} className="raven-suggestion-btn" onClick={() => sendQuery(s)}>
                        {s}
                      </button>
                    ))}
                  </div>
                )}
              </>
            )}
          </div>
        ))}

        {loading && (
          <div className="raven-typing">
            <span /><span /><span />
          </div>
        )}
        <div ref={messagesEnd} />
      </div>

      {/* Suggestion chips (only when empty) */}
      {messages.length === 0 && suggestions.length > 0 && (
        <div className="raven-suggestions">
          {suggestions.map((s, i) => (
            <button key={i} className="raven-suggestion-btn" onClick={() => sendQuery(s)}>
              {s}
            </button>
          ))}
        </div>
      )}

      {/* Input */}
      <form className="raven-widget-input" onSubmit={handleSubmit}>
        <input
          ref={inputRef}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="Ask about your data..."
          disabled={loading}
        />
        <button
          type="submit"
          className="raven-widget-send"
          disabled={loading || !input.trim()}
        >
          {loading ? <Loader2 size={16} className="spin" /> : <Send size={16} />}
        </button>
      </form>
    </div>
  );
}
