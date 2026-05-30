"use client";

import { useEffect, useRef, useState } from "react";
import { cosmo } from "@/lib/api";

interface Msg {
  role: "user" | "cosmo";
  text: string;
  intent?: string;
}

const INTENT_CLASS: Record<string, string> = {
  data: "ok",
  concept: "overdue",
  clarify: "missing",
};

export default function AskPage() {
  const [messages, setMessages] = useState<Msg[]>([]);
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const threadId = useRef<string>("");
  const endRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    // A stable thread id lets Cosmo learn from corrections within this chat.
    threadId.current = `web-${Math.random().toString(36).slice(2)}-${Date.now()}`;
  }, []);

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  async function send() {
    const q = input.trim();
    if (!q || busy) return;
    setInput("");
    setError(null);
    setMessages((m) => [...m, { role: "user", text: q }]);
    setBusy(true);
    try {
      const res = await cosmo.ask(q, threadId.current);
      setMessages((m) => [...m, { role: "cosmo", text: res.answer, intent: res.intent }]);
    } catch {
      setError("Could not reach Cosmo. Is the knowledge API running?");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div data-cy="ask-page">
      <section className="panel">
        <h2>Ask Cosmo</h2>
        <p className="muted">
          Ask about the data (it queries the warehouse) or about concepts (it answers from docs,
          the glossary, and what it has learned). If it doesn&apos;t know a term, it will ask —
          explain it and Cosmo remembers.
        </p>
        {error && (
          <div className="alert error" data-cy="ask-error">
            {error}
          </div>
        )}
        <div className="chat" data-cy="chat-log">
          {messages.length === 0 && (
            <div className="empty">Ask something like &quot;What is VPP?&quot; or &quot;Próximos agendamentos&quot;.</div>
          )}
          {messages.map((m, i) => (
            <div key={i} className={`chat-msg ${m.role}`} data-cy={`msg-${m.role}`}>
              <div className="chat-who">
                {m.role === "cosmo" ? "◆ Cosmo" : "you"}
                {m.intent && (
                  <span className={`badge ${INTENT_CLASS[m.intent] ?? "ok"}`} data-cy="msg-intent">
                    {m.intent}
                  </span>
                )}
              </div>
              <div className="chat-body">{m.text}</div>
            </div>
          ))}
          {busy && <div className="empty" data-cy="ask-thinking">Cosmo is thinking…</div>}
          <div ref={endRef} />
        </div>
        <div className="rowflex" style={{ marginTop: "0.6rem" }}>
          <input
            data-cy="ask-input"
            style={{ flex: 1, background: "var(--panel-2)", color: "var(--text)", border: "1px solid var(--border)", borderRadius: 6, padding: "0.45rem 0.6rem" }}
            placeholder="Ask Cosmo…"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && send()}
          />
          <button className="btn active" data-cy="ask-send" onClick={send} disabled={busy}>
            Send
          </button>
        </div>
      </section>
    </div>
  );
}
