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
  const cancelButton = useRef<HTMLButtonElement>(null);
  const titleId = useId();
  const descriptionId = useId();
  const restoreTarget = useRef(document.activeElement as HTMLElement | null);
  useEffect(() => {
    const previous = restoreTarget.current;
    const element = dialog.current;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    if (!element?.open) element?.showModal();
    cancelButton.current?.focus();
    return () => {
      document.body.style.overflow = previousOverflow;
      element?.close();
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
      aria-describedby={descriptionId}
      onKeyDown={(event) => {
        if (event.key !== 'Tab') return;
        const controls = Array.from(
          event.currentTarget.querySelectorAll<HTMLElement>(
            'button:not([disabled]), a[href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])',
          ),
        );
        const first = controls[0];
        const last = controls.at(-1);
        if (event.shiftKey && document.activeElement === first) {
          event.preventDefault();
          last?.focus();
        } else if (!event.shiftKey && document.activeElement === last) {
          event.preventDefault();
          first?.focus();
        }
      }}
      onCancel={(event) => {
        event.preventDefault();
        onCancel();
      }}
    >
      <h2 id={titleId}>{title}</h2>
      <div className="muted" id={descriptionId}>
        {children}
      </div>
      <div className="button-row">
        <button
          ref={cancelButton}
          className="button-secondary"
          onClick={onCancel}
          autoFocus
        >
          取消
        </button>
        <button className="action" onClick={onConfirm}>
          {confirmLabel}
        </button>
      </div>
    </dialog>
  );
}
