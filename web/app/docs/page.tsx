"use client";

import { useEffect, useRef, useState } from "react";
import { cosmo, type CosmoDoc } from "@/lib/api";
import { fmtDateTime } from "@/lib/datetime";
import { usePagination } from "@/lib/usePagination";
import { PaginationControls } from "@/components/PaginationControls";

export default function DocsPage() {
  const [docs, setDocs] = useState<CosmoDoc[]>([]);
  const [loaded, setLoaded] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [title, setTitle] = useState("");
  const [text, setText] = useState("");
  const [busy, setBusy] = useState(false);
  const fileRef = useRef<HTMLInputElement>(null);
  const pg = usePagination(docs, 20);

  function reload() {
    cosmo
      .docs()
      .then(setDocs)
      .catch(() => setError("Could not reach Cosmo's docs API."))
      .finally(() => setLoaded(true));
  }
  useEffect(reload, []);

  async function saveMarkdown() {
    if (!title.trim() || !text.trim() || busy) return;
    setBusy(true);
    setError(null);
    try {
      await cosmo.createDoc(title.trim(), text);
      setTitle("");
      setText("");
      reload();
    } catch {
      setError("Failed to save the document.");
    } finally {
      setBusy(false);
    }
  }

  async function uploadPdf(file: File) {
    if (busy) return;
    setBusy(true);
    setError(null);
    try {
      await cosmo.uploadDoc(title.trim() || file.name, file);
      setTitle("");
      if (fileRef.current) fileRef.current.value = "";
      reload();
    } catch {
      setError("Failed to upload the PDF.");
    } finally {
      setBusy(false);
    }
  }

  async function remove(id: string) {
    await cosmo.deleteDoc(id).catch(() => {});
    setDocs((ds) => ds.filter((x) => x.id !== id));
  }

  const inputStyle = {
    background: "var(--panel-2)",
    color: "var(--text)",
    border: "1px solid var(--border)",
    borderRadius: 6,
    padding: "0.4rem 0.55rem",
  } as const;

  return (
    <div data-cy="docs-page">
      <section className="panel">
        <h2>Docs</h2>
        <p className="muted">
          Domain documentation Cosmo retrieves to answer concept questions (how AON/Straloo status is
          computed, process definitions, …). Write Markdown or upload a PDF — it is chunked and indexed
          into the knowledge base. Non-PHI content only.
        </p>
        {error && <div className="alert error" data-cy="docs-error">{error}</div>}
        <div className="rowflex" style={{ alignItems: "flex-start" }}>
          <input
            data-cy="doc-title"
            placeholder="Title"
            value={title}
            onChange={(e) => setTitle(e.target.value)}
            style={{ ...inputStyle, width: 240 }}
          />
          <input
            ref={fileRef}
            data-cy="doc-pdf"
            type="file"
            accept="application/pdf,.pdf"
            onChange={(e) => {
              const f = e.target.files?.[0];
              if (f) uploadPdf(f);
            }}
            style={{ ...inputStyle }}
            disabled={busy}
          />
        </div>
        <textarea
          data-cy="doc-text"
          placeholder="Write the document in Markdown…"
          value={text}
          onChange={(e) => setText(e.target.value)}
          rows={8}
          style={{ ...inputStyle, width: "100%", marginTop: "0.6rem", fontFamily: "var(--mono, monospace)" }}
        />
        <div className="rowflex" style={{ marginTop: "0.5rem" }}>
          <button className="btn active" data-cy="doc-save" onClick={saveMarkdown} disabled={busy}>
            Save document
          </button>
          <span className="muted">PDF upload starts as soon as you pick a file.</span>
        </div>
      </section>

      <section className="panel">
        {!loaded ? (
          <div className="empty">Loading…</div>
        ) : docs.length === 0 ? (
          <div className="empty" data-cy="no-docs">
            No documents yet. Write one above or upload a PDF.
          </div>
        ) : (
          <>
            <table className="grid" data-cy="docs-table">
              <thead>
                <tr>
                  <th>title</th>
                  <th>type</th>
                  <th>chunks</th>
                  <th>ingested</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {pg.pageItems.map((d) => (
                  <tr key={d.id} data-cy="doc-row">
                    <td><b>{d.title}</b></td>
                    <td><span className="badge">{d.doc_type}</span></td>
                    <td className="muted">{d.chunks}</td>
                    <td className="muted">{fmtDateTime(d.ingested_at)}</td>
                    <td>
                      <button className="btn btn-xs" data-cy="doc-delete" onClick={() => remove(d.id)}>
                        ✕
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            <PaginationControls p={pg} cyPrefix="docs" />
          </>
        )}
      </section>
    </div>
  );
}
