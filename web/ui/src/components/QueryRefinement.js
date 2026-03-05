import React, { useState, useCallback } from 'react';
import { DatePicker, Tag } from 'antd';
import { Filter, Calendar, X } from 'lucide-react';

const { RangePicker } = DatePicker;

/* Chip-based filter suggestions — auto-generates from data context */
function generateFilterSuggestions(result, debug) {
  const suggestions = [];

  // Extract common filter dimensions from debug info
  const tables = debug?.selected_tables || [];
  const entityMatches = debug?.entity_matches || [];

  // Common business filters
  const commonFilters = [
    { key: 'premium', label: 'Premium Only', where: "plan_type = 'premium'" },
    { key: 'active', label: 'Active Users', where: "status = 'active'" },
    { key: 'last30', label: 'Last 30 Days', where: "(CURRENT_DATE - INTERVAL '30' DAY)" },
    { key: 'last90', label: 'Last 90 Days', where: "(CURRENT_DATE - INTERVAL '90' DAY)" },
  ];

  // Add entity-based filters
  entityMatches.forEach(entity => {
    if (entity.column && entity.value) {
      suggestions.push({
        key: `entity-${entity.column}-${entity.value}`,
        label: `${entity.value}`,
        where: `${entity.column} = '${entity.value}'`,
        type: 'entity',
      });
    }
  });

  return [...suggestions, ...commonFilters];
}

export default function QueryRefinement({ result, debug, onRefine }) {
  const [dateRange, setDateRange] = useState(null);
  const [activeFilters, setActiveFilters] = useState([]);
  const [expanded, setExpanded] = useState(false);

  const filterSuggestions = generateFilterSuggestions(result, debug);

  const handleDateChange = useCallback((dates, dateStrings) => {
    setDateRange(dates);
    if (dates && onRefine) {
      onRefine({
        type: 'date_range',
        start: dateStrings[0],
        end: dateStrings[1],
      });
    }
  }, [onRefine]);

  const toggleFilter = useCallback((filter) => {
    setActiveFilters(prev => {
      const exists = prev.find(f => f.key === filter.key);
      const next = exists
        ? prev.filter(f => f.key !== filter.key)
        : [...prev, filter];

      if (onRefine) {
        onRefine({
          type: 'filters',
          filters: next,
        });
      }
      return next;
    });
  }, [onRefine]);

  const clearAll = useCallback(() => {
    setDateRange(null);
    setActiveFilters([]);
    if (onRefine) onRefine({ type: 'clear' });
  }, [onRefine]);

  return (
    <div className="query-refinement">
      <button
        className="refinement-toggle"
        onClick={() => setExpanded(!expanded)}
      >
        <Filter size={13} />
        <span>Refine Query</span>
        {activeFilters.length > 0 && (
          <span className="badge badge-info" style={{ fontSize: 10 }}>
            {activeFilters.length}
          </span>
        )}
      </button>

      {expanded && (
        <div className="refinement-panel">
          {/* Date range picker */}
          <div className="refinement-section">
            <div className="refinement-section-header">
              <Calendar size={13} />
              <span>Date Range</span>
            </div>
            <RangePicker
              value={dateRange}
              onChange={handleDateChange}
              size="small"
              style={{ width: '100%' }}
            />
          </div>

          {/* Filter chips */}
          <div className="refinement-section">
            <div className="refinement-section-header">
              <Filter size={13} />
              <span>Quick Filters</span>
            </div>
            <div className="filter-chips">
              {filterSuggestions.map(filter => {
                const active = activeFilters.find(f => f.key === filter.key);
                return (
                  <Tag
                    key={filter.key}
                    className={`filter-chip ${active ? 'filter-chip-active' : ''}`}
                    onClick={() => toggleFilter(filter)}
                    closable={!!active}
                    onClose={(e) => { e.stopPropagation(); toggleFilter(filter); }}
                    color={active ? 'blue' : undefined}
                  >
                    {filter.label}
                  </Tag>
                );
              })}
            </div>
          </div>

          {/* Active filters summary */}
          {(activeFilters.length > 0 || dateRange) && (
            <div className="refinement-active">
              <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>
                Active refinements: {activeFilters.length + (dateRange ? 1 : 0)}
              </span>
              <button className="btn-text" onClick={clearAll}>
                <X size={12} /> Clear all
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
