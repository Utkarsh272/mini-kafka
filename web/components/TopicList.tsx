"use client";

import { useState } from "react";
import { TopicInfo } from "@/lib/api";

interface Props {
  topics: TopicInfo[];
  loading: boolean;
}

function Badge({
  children,
  color,
}: {
  children: React.ReactNode;
  color?: string;
}) {
  return (
    <span
      style={{
        background: color ? `${color}20` : "var(--surface2)",
        color: color ?? "var(--text-muted)",
        border: `1px solid ${color ? `${color}40` : "var(--border)"}`,
      }}
      className="text-xs px-2 py-0.5 rounded-full font-mono"
    >
      {children}
    </span>
  );
}

function PartitionRow({
  topic,
  p,
}: {
  topic: string;
  p: TopicInfo["partitions"][0];
}) {
  const lag = p.log_end_offset - p.high_watermark;
  const isUnderReplicated = p.isr.length < p.replicas.length;

  return (
    <div
      style={{
        background: "var(--surface2)",
        border: `1px solid ${isUnderReplicated ? "var(--yellow)" : "var(--border)"}`,
      }}
      className="rounded p-3 flex items-center justify-between gap-4 text-sm"
    >
      <div className="flex items-center gap-3 min-w-0">
        <span
          style={{
            background: "var(--surface)",
            color: "var(--text-muted)",
            border: "1px solid var(--border)",
          }}
          className="text-xs font-mono px-2 py-0.5 rounded shrink-0"
        >
          P{p.id}
        </span>
        <span className="text-xs truncate" style={{ color: "var(--text-muted)" }}>
          Leader: <span style={{ color: "var(--text)" }}>{p.leader_host}</span>
        </span>
      </div>

      <div className="flex items-center gap-2 shrink-0">
        <Badge>LEO: {p.log_end_offset.toLocaleString()}</Badge>
        <Badge>HWM: {p.high_watermark.toLocaleString()}</Badge>
        {lag > 0 && <Badge color="var(--yellow)">lag: {lag}</Badge>}
        {isUnderReplicated && (
          <Badge color="var(--yellow)">
            ISR: {p.isr.length}/{p.replicas.length}
          </Badge>
        )}
        {!isUnderReplicated && (
          <span style={{ color: "var(--green)" }} className="text-xs">
            ✓ in-sync
          </span>
        )}
      </div>
    </div>
  );
}

function TopicRow({ topic }: { topic: TopicInfo }) {
  const [expanded, setExpanded] = useState(false);
  const totalMessages = topic.partitions.reduce(
    (sum, p) => sum + p.log_end_offset,
    0
  );
  const underReplicated = topic.partitions.filter(
    (p) => p.isr.length < p.replicas.length
  ).length;

  return (
    <div
      style={{ border: "1px solid var(--border)" }}
      className="rounded-lg overflow-hidden"
    >
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center justify-between px-4 py-3 hover:opacity-90 transition-opacity"
        style={{ background: "var(--surface)" }}
      >
        <div className="flex items-center gap-3">
          <svg
            width="12"
            height="12"
            viewBox="0 0 12 12"
            style={{
              color: "var(--text-muted)",
              transform: expanded ? "rotate(90deg)" : "rotate(0deg)",
              transition: "transform 0.15s",
            }}
          >
            <path d="M4 2l4 4-4 4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" fill="none" />
          </svg>
          <span className="font-mono text-sm font-medium" style={{ color: "var(--text)" }}>
            {topic.name}
          </span>
        </div>
        <div className="flex items-center gap-3">
          <Badge>{topic.partitions.length} partition{topic.partitions.length !== 1 ? "s" : ""}</Badge>
          <Badge color="var(--accent)">{totalMessages.toLocaleString()} msgs</Badge>
          {underReplicated > 0 && (
            <Badge color="var(--yellow)">{underReplicated} under-replicated</Badge>
          )}
        </div>
      </button>

      {expanded && (
        <div
          style={{ background: "var(--surface)", borderTop: "1px solid var(--border)" }}
          className="p-3 flex flex-col gap-2"
        >
          {topic.partitions.map((p) => (
            <PartitionRow key={p.id} topic={topic.name} p={p} />
          ))}
        </div>
      )}
    </div>
  );
}

export function TopicList({ topics, loading }: Props) {
  if (loading && topics.length === 0) {
    return (
      <div className="flex flex-col gap-3">
        {[0, 1, 2].map((i) => (
          <div
            key={i}
            style={{ background: "var(--surface)", border: "1px solid var(--border)" }}
            className="rounded-lg p-4 animate-pulse"
          >
            <div style={{ background: "var(--border)" }} className="h-4 w-40 rounded" />
          </div>
        ))}
      </div>
    );
  }

  if (!loading && topics.length === 0) {
    return (
      <div
        style={{ background: "var(--surface)", border: "1px solid var(--border)" }}
        className="rounded-lg p-8 text-center"
      >
        <p style={{ color: "var(--text-muted)" }} className="text-sm">
          No topics yet. Create one:
        </p>
        <code
          style={{ color: "var(--accent)", background: "var(--surface2)" }}
          className="text-xs mt-2 inline-block px-3 py-1.5 rounded"
        >
          ./bin/mk topics create --partitions 3 my-topic
        </code>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-2">
      {topics.map((t) => (
        <TopicRow key={t.name} topic={t} />
      ))}
    </div>
  );
}
