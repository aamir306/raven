import React, { useState, useRef, useEffect, useCallback, useMemo, lazy, Suspense } from 'react';
import { MessageSquare, Send, PanelLeftClose, PanelLeft, Plus, Sun, Moon,
  Search, Database, Sparkles, HelpCircle, X, Trash2, FileUp, BookOpen, Settings } from 'lucide-react';
import ResponseCard from './components/ResponseCard';
import Landing from './components/Landing';
import './App.css';

const DocumentUpload = lazy(() => import('./components/pages/DocumentUpload'));
const GlossaryEditor = lazy(() => import('./components/pages/GlossaryEditor'));
const AdminDashboard = lazy(() => import('./components/pages/AdminDashboard'));

const API_BASE = process.env.REACT_APP_API_URL || '';

const PERSONAS = {
  business: { label: 'Business User', visibleTabs: ['summary', 'chart'] },
  analyst:  { label: 'Analyst',       visibleTabs: ['summary', 'chart', 'data', 'sql', 'thinking'] },
  engineer: { label: 'Engineer',      visibleTabs: ['summary', 'chart', 'data', 'sql', 'thinking', 'debug'] },
};

// Pipeline stage names for loading progress
const PIPELINE_STAGES = [
  'Classifying difficulty...',
  'Retrieving context...',
  'Selecting schema...',
  'Running test probes...',
  'Generating SQL...',
  'Validating candidates...',
  'Executing query...',
  'Preparing response...',
];

function App() {
  // ── State ──────────────────────────────────────────────────────
  const [messages, setMessages] = useState([]);         // { role, content, result? }
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [loadingStage, setLoadingStage] = useState(0);
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [persona, setPersona] = useState(() => localStorage.getItem('raven_persona') || 'analyst');
  const [theme, setTheme] = useState(() => localStorage.getItem('raven_theme') || 'dark');
  const [sessions, setSessions] = useState(() => {
    try { return JSON.parse(localStorage.getItem('raven_sessions') || '[]'); }
    catch { return []; }
  });
  const [activeSession, setActiveSession] = useState(null);
  const [conversationId, setConversationId] = useState(null);
  const [suggestions, setSuggestions] = useState([]);
  const [sessionSearch, setSessionSearch] = useState('');
  const [showHelp, setShowHelp] = useState(false);
  const [activeTool, setActiveTool] = useState(null); // 'documents' | 'glossary' | 'admin' | null

  const chatEndRef = useRef(null);
  const textareaRef = useRef(null);
  const loadingInterval = useRef(null);

  // Filter sessions by search query
  const filteredSessions = useMemo(() => {
    if (!sessionSearch.trim()) return sessions;
    const q = sessionSearch.toLowerCase();
    return sessions.filter(s => s.title.toLowerCase().includes(q));
  }, [sessions, sessionSearch]);

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

  // Animate loading stages
  useEffect(() => {
    if (loading) {
      setLoadingStage(0);
      loadingInterval.current = setInterval(() => {
        setLoadingStage(prev => {
          if (prev < PIPELINE_STAGES.length - 1) return prev + 1;
          return prev;
        });
      }, 3000);
    } else {
      clearInterval(loadingInterval.current);
      setLoadingStage(0);
    }
    return () => clearInterval(loadingInterval.current);
  }, [loading]);

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

  // ── Session persistence helpers ────────────────────────────────
  const saveSessionMessages = useCallback((sessionId, msgs) => {
    try {
      const key = `raven_msgs_${sessionId}`;
      // Store last 50 messages per session to avoid storage bloat
      const trimmed = msgs.slice(-50);
      localStorage.setItem(key, JSON.stringify(trimmed));
    } catch (e) {
      // localStorage quota exceeded — silently fail
      console.warn('Session storage full:', e);
    }
  }, []);

  const loadSessionMessages = useCallback((sessionId) => {
    try {
      const key = `raven_msgs_${sessionId}`;
      const stored = localStorage.getItem(key);
      return stored ? JSON.parse(stored) : [];
    } catch {
      return [];
    }
  }, []);

  const deleteSession = useCallback((sessionId, e) => {
    e.stopPropagation();
    localStorage.removeItem(`raven_msgs_${sessionId}`);
    setSessions(prev => {
      const updated = prev.filter(s => s.id !== sessionId);
      localStorage.setItem('raven_sessions', JSON.stringify(updated));
      return updated;
    });
    if (activeSession === sessionId) {
      setActiveSession(null);
      setConversationId(null);
      setMessages([]);
    }
  }, [activeSession]);

  const switchSession = useCallback((sessionId) => {
    // Save current session messages before switching
    if (activeSession && messages.length > 0) {
      saveSessionMessages(activeSession, messages);
    }
    setActiveSession(sessionId);
    setConversationId(`session-${sessionId}`);
    const loaded = loadSessionMessages(sessionId);
    setMessages(loaded);
  }, [activeSession, messages, saveSessionMessages, loadSessionMessages]);

  // ── Handlers ───────────────────────────────────────────────────
  const handleSubmit = useCallback(async (text) => {
    const q = (text || input).trim();
    if (!q || loading) return;

    setInput('');
    const newMessages = [...messages, { role: 'user', content: q }];
    setMessages(newMessages);
    setLoading(true);

    // Create session if needed
    let currentSessionId = activeSession;
    let currentConversationId = conversationId;
    if (!currentSessionId) {
      const session = {
        id: Date.now(),
        title: q.length > 50 ? q.slice(0, 50) + '...' : q,
        timestamp: new Date().toISOString(),
      };
      currentSessionId = session.id;
      currentConversationId = `session-${session.id}`;
      setActiveSession(currentSessionId);
      setConversationId(currentConversationId);
      setSessions(prev => {
        const updated = [session, ...prev].slice(0, 50);
        localStorage.setItem('raven_sessions', JSON.stringify(updated));
        return updated;
      });
    }

    try {
      const resp = await fetch(`${API_BASE}/api/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          question: q,
          conversation_id: currentConversationId,
        }),
      });

      const data = await resp.json();
      if (!resp.ok) throw new Error(data.detail || `HTTP ${resp.status}`);

      const updatedMessages = [...newMessages, { role: 'assistant', result: data }];
      setMessages(updatedMessages);
      saveSessionMessages(currentSessionId, updatedMessages);
    } catch (err) {
      const updatedMessages = [...newMessages, {
        role: 'assistant',
        result: { status: 'error', error: err.message },
      }];
      setMessages(updatedMessages);
      saveSessionMessages(currentSessionId, updatedMessages);
    } finally {
      setLoading(false);
    }
  }, [input, loading, activeSession, conversationId, messages, saveSessionMessages]);

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
    // Save current session messages before starting new
    if (activeSession && messages.length > 0) {
      saveSessionMessages(activeSession, messages);
    }
    setMessages([]);
    setActiveSession(null);
    setConversationId(null);
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
            <span className="version-tag">v0.4</span>
          </div>
          <button className="sidebar-new-btn" onClick={startNewSession}>
            <Plus size={16} /> New Chat
          </button>
        </div>

        {/* Session Search */}
        <div className="sidebar-search">
          <Search size={13} />
          <input
            type="text"
            placeholder="Search sessions..."
            value={sessionSearch}
            onChange={(e) => setSessionSearch(e.target.value)}
          />
          {sessionSearch && (
            <button className="sidebar-search-clear" onClick={() => setSessionSearch('')}>
              <X size={12} />
            </button>
          )}
        </div>

        <div className="sidebar-sessions">
          {filteredSessions.length > 0 && (
            <>
              <div className="sidebar-section-label">Recent</div>
              {filteredSessions.map(s => (
                <div
                  key={s.id}
                  className={`sidebar-session-item ${activeSession === s.id ? 'active' : ''}`}
                  onClick={() => switchSession(s.id)}
                  title={s.title}
                >
                  <MessageSquare size={13} style={{ marginRight: 6, flexShrink: 0 }} />
                  <span className="sidebar-session-title">{s.title}</span>
                  <button
                    className="sidebar-session-delete"
                    onClick={(e) => deleteSession(s.id, e)}
                    title="Delete session"
                  >
                    <Trash2 size={11} />
                  </button>
                </div>
              ))}
            </>
          )}
          {filteredSessions.length === 0 && sessionSearch && (
            <div className="sidebar-empty">No sessions match "{sessionSearch}"</div>
          )}
        </div>

        <div className="sidebar-tools">
          <div className="sidebar-section-label">Tools</div>
          <button className={`sidebar-tool-btn ${activeTool === 'documents' ? 'active' : ''}`}
            onClick={() => setActiveTool(activeTool === 'documents' ? null : 'documents')}>
            <FileUp size={14} /> Documents
          </button>
          <button className={`sidebar-tool-btn ${activeTool === 'glossary' ? 'active' : ''}`}
            onClick={() => setActiveTool(activeTool === 'glossary' ? null : 'glossary')}>
            <BookOpen size={14} /> Glossary
          </button>
          {persona === 'engineer' && (
            <button className={`sidebar-tool-btn ${activeTool === 'admin' ? 'active' : ''}`}
              onClick={() => setActiveTool(activeTool === 'admin' ? null : 'admin')}>
              <Settings size={14} /> Admin
            </button>
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
          <button
            className="theme-toggle"
            onClick={() => setShowHelp(true)}
            title="What can I ask?"
          >
            <HelpCircle size={14} />
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

        {/* Tool panels */}
        {activeTool && (
          <Suspense fallback={<div style={{ padding: 40, textAlign: 'center', color: 'var(--text-muted)' }}>Loading...</div>}>
            {activeTool === 'documents' && <DocumentUpload onClose={() => setActiveTool(null)} />}
            {activeTool === 'glossary' && <GlossaryEditor onClose={() => setActiveTool(null)} />}
            {activeTool === 'admin' && <AdminDashboard onClose={() => setActiveTool(null)} />}
          </Suspense>
        )}

        {/* Chat area — hidden when tool is active */}
        {!activeTool && (
        <>
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
                    <div className="loading-progress">
                      <span className="loading-stage-text">{PIPELINE_STAGES[loadingStage]}</span>
                      <div className="loading-bar">
                        <div
                          className="loading-bar-fill"
                          style={{ width: `${((loadingStage + 1) / PIPELINE_STAGES.length) * 100}%` }}
                        />
                      </div>
                    </div>
                  </div>
                </div>
              )}

              <div ref={chatEndRef} />
            </>
          )}
        </div>

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
        </>
        )}
      </main>

      {/* Help Modal */}
      {showHelp && (
        <div className="help-overlay" onClick={() => setShowHelp(false)}>
          <div className="help-modal" onClick={(e) => e.stopPropagation()}>
            <div className="help-header">
              <h2><HelpCircle size={20} /> What can I ask?</h2>
              <button className="btn-icon" onClick={() => setShowHelp(false)}>
                <X size={18} />
              </button>
            </div>
            <div className="help-body">
              <div className="help-section">
                <h3>Simple Questions</h3>
                <p>Single table lookups, basic counts, and direct filters.</p>
                <ul>
                  <li>"How many active batches are there?"</li>
                  <li>"Total revenue for January 2025"</li>
                  <li>"List all orders from last week"</li>
                </ul>
              </div>
              <div className="help-section">
                <h3>Complex Questions</h3>
                <p>Multi-table JOINs, window functions, cohort analysis.</p>
                <ul>
                  <li>"Top 10 batches by student count with completion rate"</li>
                  <li>"Weekly revenue trend compared to previous quarter"</li>
                  <li>"Faculty with highest utilization this month"</li>
                </ul>
              </div>
              <div className="help-section">
                <h3>Follow-up Questions</h3>
                <p>Refine previous results with natural language.</p>
                <ul>
                  <li>"Break that down by segment"</li>
                  <li>"Now show only premium users"</li>
                  <li>"Compare to last quarter"</li>
                </ul>
              </div>
              <div className="help-section">
                <h3>Tips</h3>
                <ul>
                  <li>Be specific about time ranges (e.g., "last 30 days", "January 2025")</li>
                  <li>Mention table names if you know them for faster results</li>
                  <li>Use the Debug tab to see how RAVEN selected tables and generated SQL</li>
                  <li>Switch to Engineer mode for full pipeline transparency</li>
                </ul>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default App;
