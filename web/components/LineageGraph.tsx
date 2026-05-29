"use client";

import { useMemo } from "react";
import ReactFlow, {
  Background,
  Controls,
  type Edge,
  type Node,
} from "reactflow";
import "reactflow/dist/style.css";
import type { LineageGraph as Graph } from "@/lib/api";

const C_SELECTED = "#ef4444";
const C_SOURCE = "#3b82f6";
const C_DEST = "#22c55e";
const C_IDLE = "#334155";
const C_EDGE_HOT = "#e2e8f0";
const C_EDGE_IDLE = "#334155";

function adjacency(edges: Graph["edges"]) {
  const parents = new Map<string, string[]>();
  const children = new Map<string, string[]>();
  for (const e of edges) {
    (children.get(e.source) ?? children.set(e.source, []).get(e.source)!).push(e.target);
    (parents.get(e.target) ?? parents.set(e.target, []).get(e.target)!).push(e.source);
  }
  return { parents, children };
}

function walk(start: string, adj: Map<string, string[]>): Set<string> {
  const seen = new Set<string>();
  const stack = [...(adj.get(start) ?? [])];
  while (stack.length) {
    const cur = stack.pop()!;
    if (seen.has(cur)) continue;
    seen.add(cur);
    stack.push(...(adj.get(cur) ?? []));
  }
  return seen;
}

// Longest-path layering for a left→right DAG layout.
function layers(graph: Graph): Map<string, number> {
  const { parents } = adjacency(graph.edges);
  const level = new Map<string, number>();
  const visiting = new Set<string>();
  const depth = (id: string): number => {
    if (level.has(id)) return level.get(id)!;
    if (visiting.has(id)) return 0; // cycle guard
    visiting.add(id);
    const ps = parents.get(id) ?? [];
    const d = ps.length ? Math.max(...ps.map(depth)) + 1 : 0;
    visiting.delete(id);
    level.set(id, d);
    return d;
  };
  for (const n of graph.nodes) depth(n.id);
  return level;
}

export function LineageGraph({
  graph,
  selected,
  onSelect,
}: {
  graph: Graph;
  selected: string | null;
  onSelect: (id: string) => void;
}) {
  const { nodes, edges } = useMemo(() => {
    const { parents, children } = adjacency(graph.edges);
    const sources = selected ? walk(selected, parents) : new Set<string>();
    const dests = selected ? walk(selected, children) : new Set<string>();
    const level = layers(graph);
    const perLevel = new Map<number, number>();

    const colorOf = (id: string): string => {
      if (!selected) return C_IDLE;
      if (id === selected) return C_SELECTED;
      if (sources.has(id)) return C_SOURCE;
      if (dests.has(id)) return C_DEST;
      return C_IDLE;
    };

    const nodes: Node[] = graph.nodes.map((n) => {
      const lv = level.get(n.id) ?? 0;
      const idx = perLevel.get(lv) ?? 0;
      perLevel.set(lv, idx + 1);
      const color = colorOf(n.id);
      const dim = selected && color === C_IDLE;
      return {
        id: n.id,
        position: { x: lv * 240, y: idx * 72 },
        data: { label: n.label },
        style: {
          background: "var(--panel-2)",
          color: "var(--text)",
          border: `2px solid ${color}`,
          borderRadius: 8,
          fontSize: 11,
          padding: 6,
          opacity: dim ? 0.4 : 1,
          width: 180,
        },
      };
    });

    const hotUp = new Set<string>([...sources, ...(selected ? [selected] : [])]);
    const hotDown = new Set<string>([...dests, ...(selected ? [selected] : [])]);
    const edges: Edge[] = graph.edges.map((e, i) => {
      const hot =
        !!selected &&
        ((hotUp.has(e.source) && hotUp.has(e.target)) ||
          (hotDown.has(e.source) && hotDown.has(e.target)));
      return {
        id: `e${i}`,
        source: e.source,
        target: e.target,
        animated: hot,
        style: { stroke: hot ? C_EDGE_HOT : C_EDGE_IDLE, strokeWidth: hot ? 2 : 1 },
      };
    });
    return { nodes, edges };
  }, [graph, selected]);

  return (
    <div className="lineage-wrap" data-cy="lineage-graph">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        fitView
        onNodeClick={(_e, node) => onSelect(node.id)}
        proOptions={{ hideAttribution: true }}
      >
        <Background color="#243049" gap={20} />
        <Controls />
      </ReactFlow>
    </div>
  );
}
