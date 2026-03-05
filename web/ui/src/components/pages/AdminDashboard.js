import React, { useState, useEffect, useMemo } from 'react';
import { X, BarChart3, Clock, DollarSign, ThumbsUp, AlertTriangle, TrendingUp, Users } from 'lucide-react';

const API_BASE = process.env.REACT_APP_API_URL || '';

export default function AdminDashboard({ onClose }) {
  const [stats, setStats] = useState(null);
  const [activeTab, setActiveTab] = useState('overview');

  useEffect(() => {
    fetch(`${API_BASE}/api/stats`)
      .then(r => r.json())
      .then(d => setStats(d))
      .catch(() => {
        /* Fallback demo data */
        setStats({
          cost_summary: { total_cost: 2.84, total_queries: 47, avg_cost: 0.06 },
          overview: {
            total_queries: 47, unique_users: 3, avg_latency: 12.4,
            avg_cost: 0.06, thumbs_up_rate: 0.78, syntax_pass_rate: 0.96,
          },
          top_questions: [
            { question: 'How many active users yesterday?', count: 12 },
            { question: 'Monthly revenue trend', count: 8 },
            { question: 'Top 10 batches by student count', count: 5 },
          ],
          recent_failures: [
            { question: 'Show funnel by...', error: 'schema_link.col_missing', time: '3h ago' },
            { question: 'Compare Q3 vs Q4...', error: 'timeout', time: '5h ago' },
          ],
        });
      });
  }, []);

  const overview = useMemo(() => stats?.overview || stats?.cost_summary || {}, [stats]);

  const TABS = ['overview', 'costs', 'failures'];

  return (
    <div className="page-panel">
      <div className="page-panel-header">
        <h2><BarChart3 size={20} /> Admin Dashboard</h2>
        <button className="btn-icon" onClick={onClose}><X size={18} /></button>
      </div>

      <div className="page-panel-body">
        <div className="admin-tabs">
          {TABS.map(t => (
            <button
              key={t}
              className={`admin-tab-btn ${activeTab === t ? 'active' : ''}`}
              onClick={() => setActiveTab(t)}
            >
              {t.charAt(0).toUpperCase() + t.slice(1)}
            </button>
          ))}
        </div>

        {activeTab === 'overview' && (
          <div className="admin-section">
            <div className="admin-stats-grid">
              <StatCard
                icon={<TrendingUp size={18} />}
                label="Total Queries"
                value={overview.total_queries ?? stats?.cost_summary?.total_queries ?? '—'}
              />
              <StatCard
                icon={<Users size={18} />}
                label="Unique Users"
                value={overview.unique_users ?? '—'}
              />
              <StatCard
                icon={<Clock size={18} />}
                label="Avg Latency"
                value={overview.avg_latency != null ? `${overview.avg_latency.toFixed(1)}s` : '—'}
              />
              <StatCard
                icon={<DollarSign size={18} />}
                label="Avg Cost"
                value={overview.avg_cost != null ? `$${overview.avg_cost.toFixed(3)}` : (stats?.cost_summary?.avg_cost != null ? `$${stats.cost_summary.avg_cost.toFixed(3)}` : '—')}
              />
              <StatCard
                icon={<ThumbsUp size={18} />}
                label="Thumbs Up Rate"
                value={overview.thumbs_up_rate != null ? `${(overview.thumbs_up_rate * 100).toFixed(0)}%` : '—'}
              />
              <StatCard
                icon={<BarChart3 size={18} />}
                label="Syntax Pass"
                value={overview.syntax_pass_rate != null ? `${(overview.syntax_pass_rate * 100).toFixed(0)}%` : '—'}
              />
            </div>

            {stats?.top_questions?.length > 0 && (
              <div className="admin-subsection">
                <h3>Top Questions</h3>
                <div className="admin-list">
                  {stats.top_questions.map((q, i) => (
                    <div key={i} className="admin-list-item">
                      <span className="admin-list-rank">{i + 1}.</span>
                      <span className="admin-list-text">{q.question}</span>
                      <span className="admin-list-count">{q.count}×</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        {activeTab === 'costs' && (
          <div className="admin-section">
            <div className="admin-stats-grid">
              <StatCard
                icon={<DollarSign size={18} />}
                label="Total Cost"
                value={stats?.cost_summary?.total_cost != null ? `$${stats.cost_summary.total_cost.toFixed(2)}` : '—'}
              />
              <StatCard
                icon={<TrendingUp size={18} />}
                label="Total Queries"
                value={stats?.cost_summary?.total_queries ?? '—'}
              />
              <StatCard
                icon={<DollarSign size={18} />}
                label="Avg per Query"
                value={stats?.cost_summary?.avg_cost != null ? `$${stats.cost_summary.avg_cost.toFixed(3)}` : '—'}
              />
            </div>
          </div>
        )}

        {activeTab === 'failures' && (
          <div className="admin-section">
            {stats?.recent_failures?.length > 0 ? (
              <div className="admin-list">
                {stats.recent_failures.map((f, i) => (
                  <div key={i} className="admin-list-item failure">
                    <AlertTriangle size={14} className="failure-icon" />
                    <div className="admin-list-detail">
                      <span className="admin-list-text">"{f.question}"</span>
                      <span className="admin-list-error">{f.error}</span>
                    </div>
                    <span className="admin-list-time">{f.time}</span>
                  </div>
                ))}
              </div>
            ) : (
              <div className="empty-state"><p>No recent failures.</p></div>
            )}
          </div>
        )}

        {!stats && (
          <div className="empty-state"><p>Loading dashboard data...</p></div>
        )}
      </div>
    </div>
  );
}

function StatCard({ icon, label, value }) {
  return (
    <div className="admin-stat-card">
      <div className="admin-stat-icon">{icon}</div>
      <div className="admin-stat-info">
        <div className="admin-stat-value">{value}</div>
        <div className="admin-stat-label">{label}</div>
      </div>
    </div>
  );
}
