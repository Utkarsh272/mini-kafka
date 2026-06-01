"use client";

import { useEffect, useState, useCallback, useRef } from "react";
import { api, Overview, TopicInfo, GroupInfo, ThroughputPoint } from "@/lib/api";
import { Header } from "@/components/Header";
import { OverviewCards } from "@/components/OverviewCards";
import { TopicList } from "@/components/TopicList";
import { GroupList } from "@/components/GroupList";
import { ThroughputChart } from "@/components/ThroughputChart";

const POLL_INTERVAL = 2000;
const MAX_CHART_POINTS = 30;

function SectionTitle({ children }: { children: React.ReactNode }) {
  return (
    <h2 className="text-xs font-semibold uppercase tracking-widest mb-3" style={{ color: "var(--text-muted)" }}>
      {children}
    </h2>
  );
}

function Card({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <div
      style={{ background: "var(--surface)", border: "1px solid var(--border)" }}
      className={`rounded-xl p-5 ${className ?? ""}`}
    >
      {children}
    </div>
  );
}

export default function Dashboard() {
  const [overview, setOverview] = useState<Overview | null>(null);
  const [topics, setTopics] = useState<TopicInfo[]>([]);
  const [groups, setGroups] = useState<GroupInfo[]>([]);
  const [chartData, setChartData] = useState<ThroughputPoint[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const prevTotalRef = useRef<number>(0);

  const fetchAll = useCallback(async () => {
    setRefreshing(true);
    try {
      const [ov, tps, grps] = await Promise.all([
        api.overview(),
        api.topics(),
        api.groups(),
      ]);
      setOverview(ov);
      setTopics(tps);
      setGroups(grps);
      setError(null);
      setLastUpdated(new Date());

      // Compute total messages across all partitions for the chart.
      const totalNow = tps.reduce(
        (sum, t) => sum + t.partitions.reduce((s, p) => s + p.log_end_offset, 0),
        0
      );
      const totalLag = tps.reduce(
        (sum, t) =>
          sum + t.partitions.reduce((s, p) => s + (p.log_end_offset - p.high_watermark), 0),
        0
      );
      const delta = Math.max(0, totalNow - prevTotalRef.current);
      prevTotalRef.current = totalNow;

      setChartData((prev) => {
        const point: ThroughputPoint = {
          time: new Date().toLocaleTimeString("en", {
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
          }),
          messages: delta,
          lag: totalLag,
        };
        const next = [...prev, point];
        return next.length > MAX_CHART_POINTS ? next.slice(-MAX_CHART_POINTS) : next;
      });
    } catch (e: any) {
      setError(e.message ?? "Unknown error");
    } finally {
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    fetchAll();
    const t = setInterval(fetchAll, POLL_INTERVAL);
    return () => clearInterval(t);
  }, [fetchAll]);

  const totalMessages = topics.reduce(
    (sum, t) => sum + t.partitions.reduce((s, p) => s + p.log_end_offset, 0),
    0
  );

  return (
    <div className="min-h-screen flex flex-col" style={{ background: "var(--bg)" }}>
      <Header
        lastUpdated={lastUpdated}
        refreshing={refreshing}
        onRefresh={fetchAll}
      />

      <main className="flex-1 p-6 max-w-7xl mx-auto w-full">
        {/* Stats row */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          <OverviewCards overview={overview} error={error} />
        </div>

        {/* Main content: 2/3 left + 1/3 right */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">

          {/* Left column */}
          <div className="lg:col-span-2 flex flex-col gap-6">

            {/* Throughput chart */}
            <Card>
              <SectionTitle>Message throughput & replication lag</SectionTitle>
              <ThroughputChart data={chartData} />
              <div className="mt-3 flex gap-4">
                <div>
                  <p className="text-xs" style={{ color: "var(--text-muted)" }}>Total messages</p>
                  <p className="text-xl font-bold font-mono tabular-nums" style={{ color: "var(--text)" }}>
                    {totalMessages.toLocaleString()}
                  </p>
                </div>
                <div>
                  <p className="text-xs" style={{ color: "var(--text-muted)" }}>Poll interval</p>
                  <p className="text-xl font-bold font-mono" style={{ color: "var(--text)" }}>
                    2s
                  </p>
                </div>
              </div>
            </Card>

            {/* Topics */}
            <Card>
              <div className="flex items-center justify-between mb-3">
                <SectionTitle>Topics</SectionTitle>
                {topics.length > 0 && (
                  <span className="text-xs" style={{ color: "var(--text-muted)" }}>
                    {topics.length} topic{topics.length !== 1 ? "s" : ""}
                  </span>
                )}
              </div>
              <TopicList topics={topics} loading={refreshing && topics.length === 0} />
            </Card>
          </div>

          {/* Right column */}
          <div className="flex flex-col gap-6">

            {/* Broker health */}
            <Card>
              <SectionTitle>Brokers</SectionTitle>
              {!overview ? (
                <div style={{ background: "var(--surface2)" }} className="rounded p-3 animate-pulse h-10" />
              ) : (
                <div className="flex flex-col gap-2">
                  {overview.brokers.map((b) => (
                    <div
                      key={b.node_id}
                      style={{ background: "var(--surface2)", border: "1px solid var(--border)" }}
                      className="rounded p-3 flex items-center justify-between"
                    >
                      <div className="flex items-center gap-2">
                        <div style={{ background: "var(--green)" }} className="w-2 h-2 rounded-full" />
                        <span className="text-sm font-mono" style={{ color: "var(--text)" }}>
                          {b.host}:{b.port}
                        </span>
                      </div>
                      <span
                        style={{ color: "var(--text-muted)", background: "var(--surface)", border: "1px solid var(--border)" }}
                        className="text-xs px-2 py-0.5 rounded font-mono"
                      >
                        node-{b.node_id}
                      </span>
                    </div>
                  ))}
                </div>
              )}
            </Card>

            {/* Consumer groups */}
            <Card>
              <div className="flex items-center justify-between mb-3">
                <SectionTitle>Consumer groups</SectionTitle>
                {groups.length > 0 && (
                  <span className="text-xs" style={{ color: "var(--text-muted)" }}>
                    {groups.length} group{groups.length !== 1 ? "s" : ""}
                  </span>
                )}
              </div>
              <GroupList groups={groups} loading={refreshing && groups.length === 0} />
            </Card>

            {/* Quick reference */}
            <Card>
              <SectionTitle>Quick start</SectionTitle>
              <div className="flex flex-col gap-2">
                {[
                  {
                    label: "Create topic",
                    cmd: "./bin/mk topics create --partitions 3 orders",
                  },
                  {
                    label: "Produce",
                    cmd: "./bin/mk produce --value hello orders",
                  },
                  {
                    label: "Consume",
                    cmd: "./bin/mk consume --from-beginning orders",
                  },
                ].map(({ label, cmd }) => (
                  <div key={label}>
                    <p className="text-xs mb-1" style={{ color: "var(--text-muted)" }}>
                      {label}
                    </p>
                    <code
                      style={{
                        background: "var(--surface2)",
                        border: "1px solid var(--border)",
                        color: "var(--text)",
                      }}
                      className="text-xs block px-3 py-2 rounded font-mono break-all"
                    >
                      {cmd}
                    </code>
                  </div>
                ))}
              </div>
            </Card>
          </div>
        </div>
      </main>
    </div>
  );
}
