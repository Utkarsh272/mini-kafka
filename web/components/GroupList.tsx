"use client";

import { GroupInfo } from "@/lib/api";

interface Props {
  groups: GroupInfo[];
  loading: boolean;
}

function StateChip({ state }: { state: string }) {
  const color =
    state === "Stable"
      ? "var(--green)"
      : state === "PreRebalance" || state === "AwaitingSync"
      ? "var(--yellow)"
      : "var(--text-muted)";

  return (
    <span
      style={{
        color,
        background: `${color}20`,
        border: `1px solid ${color}40`,
      }}
      className="text-xs px-2 py-0.5 rounded-full"
    >
      {state}
    </span>
  );
}

export function GroupList({ groups, loading }: Props) {
  if (loading && groups.length === 0) {
    return (
      <div
        style={{ background: "var(--surface)", border: "1px solid var(--border)" }}
        className="rounded-lg p-4 animate-pulse"
      >
        <div style={{ background: "var(--border)" }} className="h-4 w-32 rounded" />
      </div>
    );
  }

  if (!loading && groups.length === 0) {
    return (
      <div
        style={{ background: "var(--surface)", border: "1px solid var(--border)" }}
        className="rounded-lg p-8 text-center"
      >
        <p style={{ color: "var(--text-muted)" }} className="text-sm">
          No active consumer groups.
        </p>
        <code
          style={{ color: "var(--accent)", background: "var(--surface2)" }}
          className="text-xs mt-2 inline-block px-3 py-1.5 rounded"
        >
          ./bin/mk consume --from-beginning --group my-app my-topic
        </code>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-2">
      {groups.map((g) => (
        <div
          key={g.group_id}
          style={{ background: "var(--surface)", border: "1px solid var(--border)" }}
          className="rounded-lg p-4"
        >
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-3">
              <span className="font-mono text-sm font-medium" style={{ color: "var(--text)" }}>
                {g.group_id}
              </span>
              <StateChip state={g.state} />
            </div>
            <div className="flex items-center gap-3 text-xs" style={{ color: "var(--text-muted)" }}>
              <span>gen: {g.generation_id}</span>
              <span>{g.members.length} member{g.members.length !== 1 ? "s" : ""}</span>
            </div>
          </div>

          {g.members.length > 0 && (
            <div className="flex flex-col gap-1.5">
              {g.members.map((m) => (
                <div
                  key={m.member_id}
                  style={{ background: "var(--surface2)", border: "1px solid var(--border)" }}
                  className="rounded p-2.5 flex items-start justify-between gap-4"
                >
                  <div className="min-w-0">
                    <p className="text-xs font-mono truncate" style={{ color: "var(--text-muted)" }}>
                      {m.member_id.length > 40
                        ? m.member_id.slice(0, 37) + "..."
                        : m.member_id}
                    </p>
                    <p className="text-xs mt-0.5" style={{ color: "var(--text-muted)" }}>
                      client: <span style={{ color: "var(--text)" }}>{m.client_id}</span>
                    </p>
                  </div>
                  <div className="flex flex-wrap gap-1.5 justify-end shrink-0">
                    {m.assignment.map((a) => (
                      <span
                        key={a.topic}
                        style={{
                          background: "var(--surface)",
                          border: "1px solid var(--border)",
                          color: "var(--accent)",
                        }}
                        className="text-xs font-mono px-2 py-0.5 rounded"
                      >
                        {a.topic}:[{a.partitions.join(",")}]
                      </span>
                    ))}
                    {m.assignment.length === 0 && (
                      <span className="text-xs" style={{ color: "var(--text-muted)" }}>
                        (no assignment)
                      </span>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}
