"use client";

import { useEffect, useState, useCallback } from "react";

interface Props {
  lastUpdated: Date | null;
  refreshing: boolean;
  onRefresh: () => void;
}

export function Header({ lastUpdated, refreshing, onRefresh }: Props) {
  const [now, setNow] = useState(new Date());

  useEffect(() => {
    const t = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(t);
  }, []);

  const age = lastUpdated
    ? Math.round((now.getTime() - lastUpdated.getTime()) / 1000)
    : null;

  return (
    <header
      style={{ background: "var(--surface)", borderBottom: "1px solid var(--border)" }}
      className="sticky top-0 z-50 px-6 py-3 flex items-center justify-between"
    >
      <div className="flex items-center gap-3">
        {/* Logo mark */}
        <div
          style={{ background: "var(--accent)" }}
          className="w-7 h-7 rounded flex items-center justify-center"
        >
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
            <rect x="2" y="3" width="3" height="10" rx="1" fill="white" />
            <rect x="6.5" y="1" width="3" height="14" rx="1" fill="white" opacity="0.7" />
            <rect x="11" y="5" width="3" height="8" rx="1" fill="white" opacity="0.5" />
          </svg>
        </div>
        <span className="font-semibold text-base tracking-tight" style={{ color: "var(--text)" }}>
          mini-kafka
        </span>
        <span
          className="text-xs px-2 py-0.5 rounded"
          style={{ background: "var(--surface2)", color: "var(--text-muted)" }}
        >
          dashboard
        </span>
      </div>

      <div className="flex items-center gap-4">
        {age !== null && (
          <span className="text-xs" style={{ color: "var(--text-muted)" }}>
            Updated {age}s ago
          </span>
        )}
        <button
          onClick={onRefresh}
          disabled={refreshing}
          style={{
            background: "var(--surface2)",
            border: "1px solid var(--border)",
            color: "var(--text-muted)",
          }}
          className="text-xs px-3 py-1.5 rounded hover:opacity-80 transition-opacity disabled:opacity-40 flex items-center gap-1.5"
        >
          <svg
            width="12"
            height="12"
            viewBox="0 0 12 12"
            fill="none"
            style={{
              animation: refreshing ? "spin 0.8s linear infinite" : "none",
            }}
          >
            <path
              d="M10 6A4 4 0 1 1 6 2"
              stroke="currentColor"
              strokeWidth="1.5"
              strokeLinecap="round"
            />
            <path d="M6 0l2 2-2 2" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
          Refresh
        </button>

        <div className="flex items-center gap-1.5">
          <div style={{ background: "var(--green)" }} className="w-2 h-2 rounded-full animate-pulse" />
          <span className="text-xs" style={{ color: "var(--text-muted)" }}>
            Live
          </span>
        </div>
      </div>

      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
    </header>
  );
}
