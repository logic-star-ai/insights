import { AgentType } from "./agents";

export type Release = {
  doi: string,
  url: string
  date: string
};

export interface AgentOverview {
  total_prs: number;
  closed_prs: number;
  merged_prs: number;
  additions: number;
  deletions: number;
  files_changed: number;
  first_seen: Date;
}

export type Overview = Record<AgentType, AgentOverview>;

export type InsightKey = string
export type InsightValue = { value: number, lower?: number, upper?: number }
export type InsightFilter = string

export type InsightItem = Partial<Record<AgentType, InsightValue>> // Agent -> Value
export type FilteredInsight = Array<[InsightKey, InsightItem]> // Key, Item
export type BinnedInsight = Record<InsightFilter, FilteredInsight> // Filter -> Insight (Filter -> Key -> Agent -> Value)