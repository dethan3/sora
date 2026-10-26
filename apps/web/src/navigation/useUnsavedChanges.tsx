import { useCallback, useEffect, useRef, useState } from 'react';
import { useBeforeUnload, useBlocker } from 'react-router';
import { ConfirmDialog } from '../components/ConfirmDialog';

export function useUnsavedChanges(dirty: boolean) {
  const [pending, setPending] = useState<{ run: () => void }>();
  const lastField = useRef<HTMLElement | null>(null);
  const resume = useRef(false);
  const approved = useRef(false);
  useEffect(() => {
    approved.current = false;
  }, [dirty]);
  const blocker = useBlocker(
    ({ currentLocation, nextLocation }) =>
      dirty &&
      !approved.current &&
      (currentLocation.pathname !== nextLocation.pathname ||
        currentLocation.search !== nextLocation.search),
  );
  useEffect(() => {
    const remember = () => {
      const element = document.activeElement as HTMLElement | null;
      if (
        element?.matches('input, select, textarea') &&
        element.closest('.position-editor')
      )
        lastField.current = element;
    };
    remember();
    document.addEventListener('focusin', remember);
    return () => document.removeEventListener('focusin', remember);
  }, [dirty]);
  useEffect(() => {
    if (resume.current && !pending && blocker.state !== 'blocked') {
      resume.current = false;
      const target = lastField.current?.isConnected
        ? lastField.current
        : document.querySelector<HTMLElement>('[data-position-autofocus]');
      target?.focus();
      target?.scrollIntoView({ block: 'nearest' });
    }
  }, [pending, blocker.state]);
  useBeforeUnload(
    useCallback(
      (event) => {
        if (!dirty) return;
        event.preventDefault();
        event.returnValue = '';
      },
      [dirty],
    ),
  );
  return {
    approved(run: () => void) {
      approved.current = true;
      run();
    },
    request(run: () => void) {
      if (dirty) setPending({ run });
      else run();
    },
    dialog: (pending || blocker.state === 'blocked') && (
      <ConfirmDialog
        title="放弃未保存的修改？"
        cancelLabel="继续编辑"
        confirmLabel="放弃修改"
        onCancel={() => {
          resume.current = true;
          setPending(undefined);
          if (blocker.state === 'blocked') blocker.reset();
        }}
        onConfirm={() => {
          const action = pending;
          setPending(undefined);
          if (blocker.state === 'blocked') blocker.proceed();
          else {
            approved.current = true;
            action?.run();
          }
        }}
      >
        <p>
          这笔持仓还没有保存。继续编辑可保留当前输入；放弃后不会更改已保存的持仓。
        </p>
      </ConfirmDialog>
    ),
  };
}
