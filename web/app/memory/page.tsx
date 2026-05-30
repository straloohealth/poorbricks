"use client";

import { useEffect, useState } from "react";
import { cosmo, type MemoryItem } from "@/lib/api";
import { fmtDateTime } from "@/lib/datetime";
import { usePagination } from "@/lib/usePagination";
import { PaginationControls } from "@/components/PaginationControls";

export default function MemoryPage() {
  const [items, setItems] = useState<MemoryItem[]>([]);
  const [loaded, setLoaded] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [editing, setEditing] = useState<string | null>(null);
  const [draft, setDraft] = useState("");
  const [note, setNote] = useState<string | null>(null);
  const pg = usePagination(items, 10);

  function reload() {
    cosmo
      .memory()
      .then(setItems)
      .catch(() => setError("Could not reach Cosmo's memory API."))
      .finally(() => setLoaded(true));
  }
  useEffect(reload, []);

  async function saveLesson(id: string) {
    try {
      const updated = await cosmo.editMemory(id, draft);
      setItems((m) => m.map((x) => (x.id === id ? updated : x)));
      setEditing(null);
    } catch {
      setError("Failed to update the lesson.");
    }
  }

  async function remove(id: string) {
    await cosmo.deleteMemory(id).catch(() => {});
    setItems((m) => m.filter((x) => x.id !== id));
  }

  async function reindex() {
    setNote("Reindexing…");
    try {
      const r = await cosmo.reindex();
      setNote(`Reindexed ${r.docs} doc chunks, ${r.experiences} memories, ${r.glossary} terms.`);
    } catch {
      setNote("Reindex failed.");
    }
  }

  return (
    <div data-cy="memory-page">
      <section className="panel">
        <div className="rowflex">
          <h2 style={{ margin: 0 }}>Cosmo memory</h2>
          <button className="btn" data-cy="memory-reindex" onClick={reindex}>Rebuild index</button>
          {note && <span className="muted" data-cy="memory-note">{note}</span>}
        </div>
        <p className="muted">
          What Cosmo has learned from interactions — reusable questions and the SQL recipes / lessons
          to answer them (e.g. &quot;Próximos agendamentos&quot;). Curate freely: edit a lesson or
          delete a wrong memory. (No patient data is stored here — only queries and notes.)
        </p>
        {error && <div className="alert error" data-cy="memory-error">{error}</div>}
        {!loaded ? (
          <div className="empty">Loading…</div>
        ) : items.length === 0 ? (
          <div className="empty" data-cy="no-memory">Cosmo hasn&apos;t learned anything yet.</div>
        ) : (
          <>
            {pg.pageItems.map((m) => (
              <div className="panel-2" data-cy="memory-item" key={m.id}>
                <div className="rowflex">
                  <b>{m.question}</b>
                  <span className={`badge ${m.status === "corrected" ? "overdue" : "ok"}`}>{m.status}</span>
                  <span className="muted" style={{ marginLeft: "auto" }}>{fmtDateTime(m.updated_at)}</span>
                  <button className="btn btn-xs" data-cy="memory-delete" onClick={() => remove(m.id)}>✕</button>
                </div>
                <pre className="stacktrace" data-cy="memory-answer">{m.answer}</pre>
                {editing === m.id ? (
                  <div>
                    <textarea
                      data-cy="memory-lesson-edit"
                      rows={2}
                      value={draft}
                      onChange={(e) => setDraft(e.target.value)}
                      style={{ width: "100%", background: "var(--panel)", color: "var(--text)", border: "1px solid var(--border)", borderRadius: 6, padding: "0.4rem 0.55rem" }}
                    />
                    <div className="rowflex" style={{ gap: "0.4rem", marginTop: "0.3rem" }}>
                      <button className="btn btn-xs active" data-cy="memory-lesson-save" onClick={() => saveLesson(m.id)}>save</button>
                      <button className="btn btn-xs" onClick={() => setEditing(null)}>cancel</button>
                    </div>
                  </div>
                ) : (
                  <div className="rowflex">
                    <span className="muted" data-cy="memory-lesson">
                      {m.lesson ? `Lesson: ${m.lesson}` : "no lesson"}
                    </span>
                    <button
                      className="btn btn-xs"
                      data-cy="memory-lesson-edit-btn"
                      onClick={() => {
                        setEditing(m.id);
                        setDraft(m.lesson);
                      }}
                    >
                      edit lesson
                    </button>
                  </div>
                )}
              </div>
            ))}
            <PaginationControls p={pg} cyPrefix="memory" />
          </>
        )}
      </section>
    </div>
  );
}
