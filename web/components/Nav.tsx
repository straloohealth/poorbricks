"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const LINKS = [
  { href: "/", label: "Main" },
  { href: "/live", label: "Live Now" },
];

export function Nav() {
  const path = usePathname();
  return (
    <nav className="nav">
      <span className="brand">◆ Poorbricks</span>
      {LINKS.map((l) => (
        <Link
          key={l.href}
          href={l.href}
          className={path === l.href ? "active" : ""}
          data-cy={`nav-${l.label.toLowerCase().replace(" ", "-")}`}
        >
          {l.label}
        </Link>
      ))}
    </nav>
  );
}
