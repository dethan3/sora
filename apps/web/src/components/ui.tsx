import type { PropsWithChildren } from 'react';
import { Link } from 'react-router';

export function PageHeader({
  eyebrow,
  title,
  description,
}: {
  eyebrow: string;
  title: string;
  description: string;
}) {
  return (
    <header className="page-header">
      <p className="eyebrow">{eyebrow}</p>
      <h1>{title}</h1>
      <p className="muted">{description}</p>
    </header>
  );
}

export function Panel({
  title,
  children,
}: PropsWithChildren<{ title: string }>) {
  return (
    <section className="panel">
      <h2>{title}</h2>
      {children}
    </section>
  );
}

export function ActionLink({
  to,
  children,
}: PropsWithChildren<{ to: string }>) {
  return (
    <Link className="action" to={to}>
      {children}
      <span aria-hidden="true">↗</span>
    </Link>
  );
}

export function EmptyState({
  title,
  children,
}: PropsWithChildren<{ title: string }>) {
  return (
    <div className="empty">
      <h2>{title}</h2>
      <p className="muted">{children}</p>
    </div>
  );
}

export function Change({ value }: { value: number }) {
  return (
    <span className={value >= 0 ? 'positive' : 'negative'}>
      {value > 0 ? '+' : ''}
      {value.toFixed(2)}%
    </span>
  );
}
