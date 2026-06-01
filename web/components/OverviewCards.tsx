"use client";

import { Overview } from "@/lib/api";

interface Props {
  overview: Overview | null;
  error: string | null;
}

function StatCard({
  label,
  value,
  sub,
  color,
}: {
  label: string;
  value: string | number;
  sub?: string;
  color?: string;
}) {
  return (
    <div
      style={{
        background: "var(--surface)",
        border: "1px solid var(--border)",
        borderTop: `3px solid ${color ?? "var(--accent)"}`,
      }}
      className="rounded-lg p-5"
    >
      <p className="text-xs font-medium uppercase tracking-wider mb-2" style={{ color: "var(--text-muted)" }}>
        {label}
      </p>
      <p className="text-3xl font-bold tabular-nums" style={{ color: "var(--text)" }}>
        {value}
      </p>
      {sub && (
        <p className="text-xs mt-1" style={{ color: "var(--text-muted)" }}>
          {sub}
        </p>
      )}
    </div>
  );
}

export function OverviewCards({ overview, error }: Props) {
  if (error) {
    return (
      <div
        style={{ background: "var(--surface)", border: "1px solid var(--red)" }}
        className="rounded-lg p-5 col-span-4"
      >
        <p className="text-sm" style={{ color: "var(--red)" }}>
          ⚠ Cannot reach admin API — is the broker running?
        </p>
        <p className="text-xs mt-1" style={{ color: "var(--text-muted)" }}>
          {error}
        </p>
        <p className="text-xs mt-2" style={{ color: "var(--text-muted)" }}>
          Start with: <code className="font-mono">./bin/broker --addr=:9092 --data-dir=/tmp/mk --node-id=1 --host=localhost --port=9092</code>
          &nbsp;and&nbsp;
          <code className="font-mono">./bin/admin --broker=localhost:9092 --addr=:8080</code>
        </p>
      </div>
    );
  }

  if (!overview) {
    return (
      <>
        {[0, 1, 2, 3].map((i) => (
          <div
            key={i}
            style={{ background: "var(--surface)", border: "1px solid var(--border)" }}
            className="rounded-lg p-5 animate-pulse"
          >
            <div style={{ background: "var(--border)" }} className="h-3 w-16 rounded mb-3" />
            <div style={{ background: "var(--border)" }} className="h-8 w-12 rounded" />
          </div>
        ))}
      </>
    );
  }

  const brokerList = overview.brokers
    .map((b) => `${b.host}:${b.port}`)
    .join(", ");

  return (
    <>
      <StatCard
        label="Brokers"
        value={overview.broker_count}
        sub={brokerList}
        color="var(--green)"
      />
      <StatCard
        label="Topics"
        value={overview.topic_count}
        color="var(--accent)"
      />
      <StatCard
        label="Partitions"
        value={overview.partition_count}
        color="var(--yellow)"
      />
      <StatCard
        label="Status"
        value="Healthy"
        sub={`Last polled: ${new Date(overview.collected_at).toLocaleTimeString()}`}
        color="var(--green)"
      />
    </>
  );
}
