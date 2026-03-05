import React, { useState, useRef, useEffect, useCallback } from 'react';
import { MessageSquare, Send, PanelLeftClose, PanelLeft, Plus, Sun, Moon,
  Search, Database, Sparkles } from 'lucide-react';
import ResponseCard from './components/ResponseCard';
import Landing from './components/Landing';
import './App.css';

const API_BASE = process.env.REACT_APP_API_URL || '';

const PERSONAS = {
  business: { label: 'Business User', visibleTabs: ['summary', 'chart'] },
  analyst:  { label: 'Analyst',       visibleTabs: ['summary', 'chart', 'data', 'sql'] },
  engineer: { label: 'Engineer',      visibleTabs: ['summary', 'chart', 'data', 'sql', 'debug'] },
};

function App() {
  // ── State ──────────────────────────────────────────────────────
  const [messages, setMessages] = useState([]);         // { role, content, result? }
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [persona, setPersona] = useState(() => localStorage.getItem('raven_persona') || 'analyst');
  const [theme, setTheme] = useState(() => localStorage.getItem('raven_theme') || 'dark');
  const [sessions, setSessions] = useState(() => {
    try { return JSON.parse(localStorage.getItem('raven_sessions') || '[]'); }
    catch { return []; }
  });
  const [activeSession, setActiveSession] = useState(null);
  const [suggestions, setSuggestions] = useState([]);

  const chatEndRef = useRef(null);
  const textareaRef = useRef(null);

  // ── Effects ────────────────────────────────────────────────────
  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem('raven_theme', theme);
  }, [theme]);

  useEffect(() => {
    localStorage.setItem('raven_persona', persona);
  }, [persona]);

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, loading]);

  // Load suggestions on mount
  useEffect(() => {
    fetch(`${API_BASE}/api/suggestions`)
      .then(r => r.json())
      .then(d => setSuggestions(d.suggestions || []))
      .catch(() => {
        // Fallback suggestions when API is not available
        setSuggestions([
          { question: 'How many orders were placed last month?', category: 'revenue' },
          { question: 'What is the daily revenue trend for the past 30 days?', category: 'revenue' },
          { question: 'Top 10 customers by total spend', category: 'user_metrics' },
          { question: 'Monthly conversion rate by platform', category: 'funnel' },
          { question: 'Payment success rate by gateway', category: 'revenue' },
          { question: 'Show me active users by segment', category: 'user_metrics' },
        ]);
      });
  }, []);

  // ── Handlers ───────────────────────────────────────────────────
  const handleSubmit = useCallback(async (text) => {
    const q = (text || input).trim();
    if (!q || loading) return;

    setInput('');
    setMessages(prev => [...prev, { role: 'user', content: q }]);
    setLoading(true);

    try {
      const resp = await fetch(`${API_BASE}/api/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: q }),
      });

      const data = await resp.json();
      if (!resp.ok) throw new Error(data.detail || `HTTP ${resp.status}`);

      setMessages(prev => [...prev, { role: 'assistant', result: data }]);

      // Save to session history
      if (!activeSession) {
        const session = {
          id: Date.now(),
          title: q.length > 50 ? q.slice(0, 50) + '...' : q,
          timestamp: new Date().toISOString(),
        };
        setActiveSession(session.id);
        setSessions(prev => {
          const updated = [session, ...prev].slice(0, 30);
          localStorage.setItem('raven_sessions', JSON.stringify(updated));
          return updated;
        });
      }
    } catch (err) {
      setMessages(prev => [...prev, {
        role: 'assistant',
        result: { status: 'error', error: err.message },
      }]);
    } finally {
      setLoading(false);
    }
  }, [input, loading, activeSession]);

  const handleFeedback = async (queryId, feedback, correction) => {
    try {
      await fetch(`${API_BASE}/api/feedback`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          query_id: queryId,
          feedback,
          correction_sql: correction || null,
        }),
      });
    } catch (err) {
      console.error('Feedback failed:', err);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit();
    }
  };

  const startNewSession = () => {
    setMessages([]);
    setActiveSession(null);
    textareaRef.current?.focus();
  };

  const autoResize = (e) => {
    e.target.style.height = 'auto';
    e.target.style.height = Math.min(e.target.scrollHeight, 120) + 'px';
  };

  const visibleTabs = PERSONAS[persona]?.visibleTabs || PERSONAS.analyst.visibleTabs;
  const isEmpty = messages.length === 0;

  // ── Render ─────────────────────────────────────────────────────
  return (
    <div className="app-layout">
      {/* Sidebar */}
      <aside className={`sidebar ${sidebarOpen ? '' : 'collapsed'}`}>
        <div className="sidebar-header">
          <div className="sidebar-logo">
            <h1>RAVEN</h1>
            <span className="version-tag">v0.3</span>
          </div>
          <button className="sidebar-new-btn" onClick={startNewSession}>
            <Plus size={16} /> New Chat
          </button>
        </div>

        <div className="sidebar-sessions">
          {sessions.length > 0 && (
            <>
              <div className="sidebar-section-label">Recent</div>
              {sessions.map(s => (
                <button
                  key={s.id}
                  className={`sidebar-session-item ${activeSession === s.id ? 'active' : ''}`}
                  onClick={() => { setActiveSession(s.id); }}
                  title={s.title}
                >
                  <MessageSquare size={13} style={{ marginRight: 6, flexShrink: 0 }} />
                  {s.title}
                </button>
              ))}
            </>
          )}
        </div>

        <div className="sidebar-footer">
          <select
            className="persona-select"
            value={persona}
            onChange={(e) => setPersona(e.target.value)}
          >
            {Object.entries(PERSONAS).map(([k, v]) => (
              <option key={k} value={k}>{v.label}</option>
            ))}
          </select>
          <button
            className="theme-toggle"
            onClick={() => setTheme(t => t === 'dark' ? 'light' : 'dark')}
            title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}
          >
            {theme === 'dark' ? <Sun size={14} /> : <Moon size={14} />}
          </button>
        </div>
      </aside>

      {/* Main */}
      <main className={`main-content ${sidebarOpen ? '' : 'sidebar-collapsed'}`}>
        <div className="topbar">
          <button
            className="sidebar-toggle"
            onClick={() => setSidebarOpen(o => !o)}
          >
            {sidebarOpen ? <PanelLeftClose size={18} /> : <PanelLeft size={18} />}
          </button>
          <span className="topbar-title">
            {isEmpty ? '' : 'RAVEN — Text-to-SQL'}
          </span>
        </div>

        <div className="chat-area">
          {isEmpty ? (
            <Landing
              suggestions={suggestions}
              onSelect={(q) => handleSubmit(q)}
            />
          ) : (
            <>
              {messages.map((msg, i) => (
                <div key={i} className={`message message-${msg.role}`}>
                  {msg.role === 'user' ? (
                    <div className="message-bubble">{msg.content}</div>
                  ) : msg.result?.status === 'ambiguous' ? (
                    <div className="ambiguous-card">
                      <p>{msg.result.message}</p>
                      {msg.result.suggestions?.length > 0 && (
                        <div className="ambiguous-suggestions">
                          {msg.result.suggestions.map((s, j) => (
                            <button
                              key={j}
                              className="ambiguous-suggestion-btn"
                              onClick={() => handleSubmit(s)}
                            >
                              {s}
                            </button>
                          ))}
                        </div>
                      )}
                    </div>
                  ) : msg.result?.status === 'error' ? (
                    <div className="error-card">
                      <strong>Error:</strong> {msg.result.error}
                    </div>
                  ) : msg.result?.sql ? (
                    <>
                      <ResponseCard
                        result={msg.result}
                        visibleTabs={visibleTabs}
                        onFeedback={handleFeedback}
                        onRerun={handleSubmit}
                        theme={theme}
                      />
                      {msg.result.suggestions?.length > 0 && (
                        <div className="followup-suggestions">
                          {msg.result.suggestions.map((s, j) => (
                            <button
                              key={j}
                              className="followup-btn"
                              onClick={() => handleSubmit(s)}
                            >
                              {s}
                            </button>
                          ))}
                        </div>
                      )}
                    </>
                  ) : null}
                </div>
              ))}

              {loading && (
                <div className="message message-assistant">
                  <div className="loading-indicator">
                    <div className="loading-dots">
                      <span />
                      <span />
                      <span />
                    </div>
                    <span>Generating SQL...</span>
                  </div>
                </div>
              )}

              <div ref={chatEndRef} />
            </>
          )}
        </div>

        {/* Input Bar */}
        <div className={`input-bar-container ${sidebarOpen ? '' : 'sidebar-collapsed'}`}>
          <div className="input-bar">
            <textarea
              ref={textareaRef}
              value={input}
              onChange={(e) => { setInput(e.target.value); autoResize(e); }}
              onKeyDown={handleKeyDown}
              placeholder="Ask a question about your data..."
              rows={1}
              disabled={loading}
            />
            <button
              className="send-btn"
              onClick={() => handleSubmit()}
              disabled={loading || !input.trim()}
              title="Send"
            >
              <Send size={16} />
            </button>
          </div>
        </div>
      </main>
    </div>
  );
}

export default App;
