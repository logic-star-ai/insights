import { agentsInfo, AgentType } from "@/lib/agents";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./ui/table";
import Image from "next/image";
import { Checkbox } from "./ui/checkbox";
import { AgentOverview, Overview } from "@/lib/types";

export function OverviewTable({
  overview,
  selectedAgents,
  setSelectedAgents
}: {
  overview: Overview,
  selectedAgents: Record<AgentType, boolean>
  setSelectedAgents: any
}) {
  const rows: [AgentType, AgentOverview][] = Object.entries(overview)
    .map(([k, v]) => [k as AgentType, v])
  rows.sort(([, v1], [, v2]) => v2.total_prs - v1.total_prs);

  return (
    <div className="rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="w-[40px]"/>
            <TableHead>Agent</TableHead>
            <TableHead>PRs Opened</TableHead>
            <TableHead>PRs Closed</TableHead>
            <TableHead>PRs Merged</TableHead>
            <TableHead>Merge Rate</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map(([agent, overview]) => {
            return (
              <TableRow key={agentsInfo[agent].name}>
                <TableCell className="flex justify-center">
                  <Checkbox className="cursor-pointer" checked={selectedAgents[agent]} onClick={() => setSelectedAgents(
                    {...selectedAgents, [agent]: !selectedAgents[agent]}
                  )} />
                </TableCell>
                <TableCell>
                  <div className="flex gap-[10px] font-medium items-center">
                    {!!agentsInfo[agent]?.logo ? (
                      <Image
                        width={20}
                        height={20}
                        src={agentsInfo[agent].logo}
                        alt={agentsInfo[agent].name}
                        className={`w-[18px] ${agentsInfo[agent].logo_invert ? 'dark:invert' : ''}`}
                      />
                    ) : null}
                    {agentsInfo[agent].name}
                  </div>
                </TableCell>
                <TableCell>{overview.total_prs || 0}</TableCell>
                <TableCell>{overview.closed_prs || 0}</TableCell>
                <TableCell>{overview.merged_prs || 0}</TableCell>
                <TableCell>
                  {(overview.closed_prs || 0) == 0 ? '-' : ((overview.merged_prs || 0) / overview.closed_prs * 100).toFixed(2)} %
                </TableCell>
              </TableRow>
            )
          })}
        </TableBody>
      </Table>
    </div>
  );
}
