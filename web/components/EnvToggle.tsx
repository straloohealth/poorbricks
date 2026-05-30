"use client";

export function EnvToggle({
  env,
  onChange,
}: {
  env: string;
  onChange: (e: string) => void;
}) {
  return (
    <div className="rowflex" data-cy="env-toggle">
      {["prod", "dev"].map((e) => (
        <button
          key={e}
          className={`btn ${env === e ? "active" : ""}`}
          data-cy={`env-${e}`}
          onClick={() => onChange(e)}
        >
          {e}
        </button>
      ))}
    </div>
  );
}
