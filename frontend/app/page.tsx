import { getBinnedInsight, getLastRelease, getOverview } from "@/lib/db";
import { Metadata } from "next";
import Dashboard, { DashboardInsights } from "@/components/dashboard";

export const metadata: Metadata = {
  metadataBase: new URL('https://insights.logicstar.ai/'),
  title: "Agents in the Wild",
  description:
    "We track and analyze the performance of autonomous code agents in the wild (on GitHub).",
  openGraph: {
    type: "website",
    url: "https://insights.logicstar.ai/",
    title: "Agents in the Wild",
    description: "We track and analyze the performance of autonomous code agents in the wild (on GitHub).",
    images: [{ url: "https://insights.logicstar.ai/social-media.png" }]
  }
};

// Prevent prerendering
export const dynamic = "force-dynamic";

export default async function Home() {
  const overview = await getOverview();
  const lastRelease = await getLastRelease();
  const insights: DashboardInsights = {
    dailyInsightPrs: await getBinnedInsight("insight_daily", "bin", "total_prs", "bin::date"),
    dailyInsightMergeRate: await getBinnedInsight("insight_daily", "bin", "merge_rate", "bin::date", true),
    
    repoPolularityShare: await getBinnedInsight("insight_repo_popularity", "bin", "total_pr_share", "min"),
    repoPolularityMergeRate: await getBinnedInsight("insight_repo_popularity", "bin", "merge_rate", "min", true),

    changeComplexityShare: await getBinnedInsight("insight_change_complexity", "bin", "total_pr_share", "min"),
    changeComplexityMergeRate: await getBinnedInsight("insight_change_complexity", "bin", "merge_rate", "min", true),

    changedFilesShare: await getBinnedInsight("insight_changed_files", "bin", "total_pr_share", "min"),
    changedFilesMergeRate: await getBinnedInsight("insight_changed_files", "bin", "merge_rate", "min", true),

    adRatioShare: await getBinnedInsight("insight_ad_ratio", "bin", "total_pr_share", "min"),
    adRatioMergeRate: await getBinnedInsight("insight_ad_ratio", "bin", "merge_rate", "min", true),

    repoLanguageShare: await getBinnedInsight("insight_language", "bin", "total_pr_share", "min"),
    repoLanguageMergeRate: await getBinnedInsight("insight_language", "bin", "merge_rate", "min", true),

    changeLanguageShare: await getBinnedInsight("insight_change_language", "bin", "total_pr_share", "min"),
    changeLanguageMergeRate: await getBinnedInsight("insight_change_language", "bin", "merge_rate", "min", true),
  }

  return <Dashboard overview={overview} lastRelease={lastRelease} insights={insights}  />
}