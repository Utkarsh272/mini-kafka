export interface BrokerInfo {
  node_id: number;
  host: string;
  port: number;
}

export interface PartitionInfo {
  id: number;
  leader_id: number;
  leader_host: string;
  replicas: number[];
  isr: number[];
  log_end_offset: number;
  high_watermark: number;
}

export interface TopicInfo {
  name: string;
  partitions: PartitionInfo[];
}

export interface MemberAssignment {
  topic: string;
  partitions: number[];
}

export interface MemberInfo {
  member_id: string;
  client_id: string;
  assignment: MemberAssignment[];
}

export interface GroupInfo {
  group_id: string;
  state: string;
  generation_id: number;
  leader_id: string;
  members: MemberInfo[];
}

export interface Overview {
  collected_at: string;
  broker_count: number;
  topic_count: number;
  partition_count: number;
  brokers: BrokerInfo[];
}

// Throughput data point for charts
export interface ThroughputPoint {
  time: string;
  messages: number;
  lag: number;
}

const BASE = process.env.NEXT_PUBLIC_ADMIN_URL ?? "http://localhost:8080";

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`${path}: ${res.status}`);
  return res.json();
}

export const api = {
  overview: () => get<Overview>("/api/overview"),
  topics: () => get<TopicInfo[]>("/api/topics"),
  topic: (name: string) => get<TopicInfo>(`/api/topics/${name}`),
  groups: () => get<GroupInfo[]>("/api/groups"),
  group: (id: string) => get<GroupInfo>(`/api/groups/${id}`),
};
