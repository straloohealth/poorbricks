"use client";

import { useEffect, useState } from "react";
import { cosmo, type GlossaryTerm } from "@/lib/api";
import { fmtDateTime } from "@/lib/datetime";
import { usePagination } from "@/lib/usePagination";
import { PaginationControls } from "@/components/PaginationControls";

export default function GlossaryPage() {
  const [terms, setTerms] = useState<GlossaryTerm[]>([]);
  const [loaded, setLoaded] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [term, setTerm] = useState("");
  const [definition, setDefinition] = useState("");
  const [aliases, setAliases] = useState("");
  const [busy, setBusy] = useState(false);
  const pg = usePagination(terms, 20);

  function reload() {
    cosmo
      .glossary()
      .then(setTerms)
      .catch(() => setError("Could not reach Cosmo's glossary API."))
      .finally(() => setLoaded(true));
  }
  useEffect(reload, []);

  async function save() {
    if (!term.trim() || !definition.trim() || busy) return;
    setBusy(true);
    setError(null);
    try {
      const al = aliases.split(",").map((a) => a.trim()).filter(Boolean);
      await cosmo.saveTerm(term.trim(), definition.trim(), al);
      setTerm("");
      setDefinition("");
      setAliases("");
      reload();
    } catch {
      setError("Failed to save the term.");
    } finally {
      setBusy(false);
    }
  }

  function edit(t: GlossaryTerm) {
    setTerm(t.term);
    setDefinition(t.definition);
    setAliases(t.aliases.join(", "));
  }

  async function remove(id: string) {
    await cosmo.deleteTerm(id).catch(() => {});
    setTerms((ts) => ts.filter((x) => x.id !== id));
  }

  const inputStyle = {
    background: "var(--panel-2)",
    color: "var(--text)",
    border: "1px solid var(--border)",
    borderRadius: 6,
    padding: "0.4rem 0.55rem",
  } as const;

  return (
    <div data-cy="glossary-page">
      <section className="panel">
        <h2>Glossary</h2>
        <p className="muted">
          Curated domain terms (VPP = Validação de Procedimento Preliminar, GHE, …). The whole
          glossary is injected into Cosmo&apos;s prompt so it always resolves these terms.
        </p>
        {error && <div className="alert error" data-cy="glossary-error">{error}</div>}
        <div className="rowflex" style={{ alignItems: "flex-start" }}>
          <input data-cy="term-name" placeholder="Term (e.g. VPP)" value={term} onChange={(e) => setTerm(e.target.value)} style={{ ...inputStyle, width: 140 }} />
          <input data-cy="term-def" placeholder="Definition" value={definition} onChange={(e) => setDefinition(e.target.value)} style={{ ...inputStyle, flex: 1, minWidth: 240 }} />
          <input data-cy="term-aliases" placeholder="aliases, comma-separated" value={aliases} onChange={(e) => setAliases(e.target.value)} style={{ ...inputStyle, width: 200 }} />
          <button className="btn active" data-cy="term-save" onClick={save} disabled={busy}>
            Save term
          </button>
        </div>
      </section>

      <section className="panel">
        {!loaded ? (
          <div className="empty">Loading…</div>
        ) : terms.length === 0 ? (
          <div className="empty" data-cy="no-terms">No terms yet. Add VPP, GHE, …</div>
        ) : (
          <>
            <table className="grid" data-cy="glossary-table">
              <thead>
                <tr>
                  <th>term</th>
                  <th>definition</th>
                  <th>aliases</th>
                  <th>updated</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {pg.pageItems.map((t) => (
                  <tr key={t.id} data-cy="term-row">
                    <td><b>{t.term}</b></td>
                    <td>{t.definition}</td>
                    <td className="muted">{t.aliases.join(", ")}</td>
                    <td className="muted">{fmtDateTime(t.updated_at)}</td>
                    <td className="rowflex" style={{ gap: "0.3rem" }}>
                      <button className="btn btn-xs" data-cy="term-edit" onClick={() => edit(t)}>edit</button>
                      <button className="btn btn-xs" data-cy="term-delete" onClick={() => remove(t.id)}>✕</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            <PaginationControls p={pg} cyPrefix="glossary" />
          </>
        )}
      </section>
    </div>
  );
}
