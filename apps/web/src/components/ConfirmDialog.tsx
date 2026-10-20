import { useEffect, useId, useRef, type PropsWithChildren } from 'react';

export function ConfirmDialog({
  title,
  onCancel,
  onConfirm,
  confirmLabel = '确认',
  children,
}: PropsWithChildren<{
  title: string;
  onCancel: () => void;
  onConfirm: () => void;
  confirmLabel?: string;
}>) {
  const dialog = useRef<HTMLDialogElement>(null);
  const titleId = useId();
  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null;
    dialog.current?.showModal();
    return () => {
      (previous?.isConnected
        ? previous
        : document.getElementById('main-content')
      )?.focus({ preventScroll: true });
    };
  }, []);
  return (
    <dialog
      ref={dialog}
      className="confirm-dialog"
      aria-labelledby={titleId}
      onCancel={(event) => {
        event.preventDefault();
        onCancel();
      }}
    >
      <h2 id={titleId}>{title}</h2>
      <div className="muted">{children}</div>
      <div className="button-row">
        <button className="button-secondary" onClick={onCancel} autoFocus>
          取消
        </button>
        <button className="action" onClick={onConfirm}>
          {confirmLabel}
        </button>
      </div>
    </dialog>
  );
}
