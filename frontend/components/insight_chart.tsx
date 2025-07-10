"use client"

import { Area, Bar, CartesianGrid, ComposedChart, ErrorBar, Line, XAxis, YAxis } from "recharts"

import {
    Card,
    CardAction,
    CardContent,
    CardDescription,
    CardHeader,
    CardTitle,
} from "@/components/ui/card"
import {
    ChartConfig,
    ChartContainer,
    ChartTooltip,
    ChartTooltipContent
} from "@/components/ui/chart"
import { useState } from "react"
import { agentsInfo, agentsList, AgentType } from "@/lib/agents"
import { Button } from "./ui/button"
import { useMediaPredicate } from "react-media-hook";
import { ValueType } from "recharts/types/component/DefaultTooltipContent"
import { DropdownMenu, DropdownMenuCheckboxItem, DropdownMenuContent, DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger } from "./ui/dropdown-menu"
import { BinnedInsight } from "@/lib/types"

const chartConfig = Object.fromEntries(
    Object.entries(agentsInfo).map(([key, agent], i) => [
        key,
        { label: agent.name, color: `var(--chart-${i + 1})` },
    ])
) satisfies ChartConfig

export function AgentInsightChart({ title, subtitle, insight, showAgents, percentage = false, bounds = false, chartType = "line", xLabel = "" }:
    { title: string, subtitle: string, insight: BinnedInsight, showAgents: Record<AgentType, boolean>, percentage?: boolean, bounds?: boolean, chartType?: "bar" | "line", xLabel?: string }) {
    const isLargeScreen = useMediaPredicate("(min-width: 640px)");

    const [showBounds, setShowBounds] = useState(false);
    const [showPopular, setShowPopular] = useState(false);

    const filter = showPopular ? 'popular' : 'all';

    const chartData = insight[filter].map(([key, item]) =>
    ({
        key: key,
        ...(Object.fromEntries(Object.entries(item).map(([key, i]) => ([key, i.value || 'NaN'])))),
        ...(Object.fromEntries(Object.entries(item).map(([key, i]) => ([key + '_lb', i.lower])))),
        ...(Object.fromEntries(Object.entries(item).map(([key, i]) => ([key + '_range', (i.upper || 0) - (i.lower || 0)]))))
    }))

    return (
        <Card>
            <CardHeader>
                <CardTitle>{title}</CardTitle>
                <CardDescription>{subtitle}</CardDescription>
                <CardAction>
                    <DropdownMenu>
                        <DropdownMenuTrigger asChild>
                            <Button variant="outline" className="cursor-pointer">⚙️</Button>
                        </DropdownMenuTrigger>
                        <DropdownMenuContent className="w-70" side="bottom" align="end">
                            <DropdownMenuLabel>Plot Settings</DropdownMenuLabel>
                            <DropdownMenuSeparator />
                            {bounds && <DropdownMenuCheckboxItem
                                checked={showBounds}
                                onCheckedChange={setShowBounds}
                                className="cursor-pointer"
                            >
                                Show Uncertainty Band (95%)
                            </DropdownMenuCheckboxItem>}

                            <DropdownMenuCheckboxItem
                                checked={showPopular}
                                onCheckedChange={setShowPopular}
                                className="cursor-pointer"
                            >
                                &ge; 10 star repos only
                            </DropdownMenuCheckboxItem>
                        </DropdownMenuContent>
                    </DropdownMenu>
                </CardAction>
            </CardHeader>


            <CardContent>
                <ChartContainer config={chartConfig} className="w-full aspect-[4/3] lg:aspect-[2/1]">
                    <ComposedChart
                        accessibilityLayer
                        data={chartData}
                        margin={{ bottom: 25 }}
                    >
                        <CartesianGrid vertical={false} />
                        <XAxis
                            dataKey="key"
                            tickLine={false}
                            axisLine={false}
                            tickMargin={8}
                            tickFormatter={(value) => value}
                            label={{ value: xLabel, position: 'insideBottom', offset: -20 }}
                        />

                        {isLargeScreen ? <YAxis
                            axisLine={false}
                            tickLine={false}
                            tickMargin={8}
                        >
                        </YAxis> : <></>}

                        <ChartTooltip cursor={false} content={<ChartTooltipContent valueFormatter={(e: ValueType) => {
                            if (percentage) {
                                return `${(e as number).toFixed(1)} %`
                            } else {
                                return e.toLocaleString();
                            }
                        }} />} />

                        {chartType == 'line' && bounds && showBounds && agentsList.map((k => {
                            if (showAgents[k]) {
                                return <Area
                                    key={k + "_base"}
                                    type="monotone"
                                    dataKey={k + '_lb'}
                                    stroke="none"
                                    fill="none"
                                    stackId={k + "confidence"}
                                    legendType="none"
                                    activeDot={false}
                                    name="hidden"
                                    isAnimationActive={false}
                                />
                            }
                        }))}

                        {chartType == 'line' && bounds && showBounds && agentsList.map((k => {
                            if (showAgents[k]) {
                                return <Area
                                    key={k + "_base2"}
                                    dataKey={k + '_range'}
                                    type="monotone"
                                    stroke="none"
                                    fill={chartConfig[k].color}
                                    opacity={0.2}
                                    stackId={k + "confidence"}
                                    activeDot={false}
                                    name="hidden"
                                    isAnimationActive={false}
                                />
                            }
                        }))}


                        {agentsList.map((k => {
                            if (showAgents[k]) {
                                if (chartType == "line") {
                                    return <Line
                                        key={k}
                                        dataKey={k}
                                        type="monotone"
                                        stroke={chartConfig[k].color}
                                        strokeWidth={2}
                                        dot={false}
                                        isAnimationActive={false}
                                    />
                                } else if (chartType == "bar") {
                                    return <Bar
                                        key={k}
                                        dataKey={k}
                                        type="monotone"
                                        fill={chartConfig[k].color}
                                        stroke="black"
                                        strokeWidth={0}
                                        isAnimationActive={false}
                                    >
                                        {showBounds && <ErrorBar
                                            dataKey={k + '_range'}
                                            width={8}
                                            strokeWidth={3}
                                            stroke={chartConfig[k].color}
                                            direction="y" />}
                                    </Bar>
                                }
                            }
                        }))}
                    </ComposedChart>
                </ChartContainer>
            </CardContent>
        </Card>
    )
}
