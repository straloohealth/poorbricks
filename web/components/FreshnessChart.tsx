"use client";

import { useMemo, useState } from "react";
import {
  CartesianGrid,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
  ResponsiveContainer,
} from "recharts";

export interface FreshPoint {
  table: string;
  ageHours: number;
  status: string;
}

// Bucket points whose age rounds to the same hour so overlapping dots become a
// single stacked column; clicking any dot reveals the bucket's table list.
interface Dot {
  x: number; // bucket centre (hours)
  y: number; // stack position within the bucket
  tables: string[]; // every table in the bucket (same for all dots in it)
  table: string; // this dot's own table
}

function bucketize(points: FreshPoint[]): { dots: Dot[]; buckets: Map<number, string[]> } {
  const buckets = new Map<number, string[]>();
  const sorted = [...points].sort((a, b) => a.ageHours - b.ageHours);
  for (const p of sorted) {
    const key = Math.round(p.ageHours);
    const arr = buckets.get(key) ?? [];
    arr.push(p.table);
    buckets.set(key, arr);
  }
  const dots: Dot[] = [];
  for (const [age, tables] of buckets) {
    tables.forEach((t, i) => dots.push({ x: age, y: i + 1, tables, table: t }));
  }
  return { dots, buckets };
}

function DotTooltip({ active, payload }: { active?: boolean; payload?: { payload: Dot }[] }) {
  if (!active || !payload?.length) return null;
  const d = payload[0].payload;
  return (
    <div className="tooltip" data-cy="freshness-tooltip">
      <strong>~{d.x}h old</strong>
      <div>{d.tables.join(", ")}</div>
    </div>
  );
}

export function FreshnessChart({ points }: { points: FreshPoint[] }) {
  const { dots, buckets } = useMemo(() => bucketize(points), [points]);
  const [picked, setPicked] = useState<number | null>(null);

  if (points.length === 0)
    return <div className="empty" data-cy="freshness-empty">No freshness data.</div>;

  const pickedTables = picked != null ? buckets.get(picked) ?? [] : [];

  return (
    <div data-cy="freshness-chart">
      <div style={{ width: "100%", height: 260 }}>
        <ResponsiveContainer>
          <ScatterChart margin={{ top: 16, right: 24, bottom: 24, left: 8 }}>
            <CartesianGrid stroke="#243049" />
            <XAxis
              type="number"
              dataKey="x"
              name="age"
              unit="h"
              stroke="#7d8aa3"
              label={{ value: "hours since last run", position: "bottom", fill: "#7d8aa3" }}
            />
            <YAxis type="number" dataKey="y" hide domain={[0, "dataMax + 1"]} />
            <ZAxis range={[120, 120]} />
            <Tooltip content={<DotTooltip />} cursor={{ strokeDasharray: "3 3" }} />
            <Scatter
              data={dots}
              fill="#3b82f6"
              // recharts exposes the original datum under `.payload`; the
              // top-level `.x` is the pixel coordinate, not the bucket hour.
              onClick={(d: { payload?: Dot }) => setPicked(d?.payload?.x ?? null)}
              cursor="pointer"
            />
          </ScatterChart>
        </ResponsiveContainer>
      </div>
      {picked != null && (
        <div className="panel-2" data-cy="freshness-bucket">
          <strong>~{picked}h old — {pickedTables.length} table(s)</strong>
          <ul>
            {pickedTables.map((t) => (
              <li key={t} data-cy="freshness-bucket-item">{t}</li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}
