import '@testing-library/jest-dom/vitest';
import { cleanup } from '@testing-library/react';
import { afterEach } from 'vitest';
afterEach(() => {
  cleanup();
  sessionStorage.clear();
});
Object.defineProperty(HTMLElement.prototype, 'scrollIntoView', {
  value() {},
  configurable: true,
});
Object.defineProperty(HTMLDialogElement.prototype, 'showModal', {
  value() {
    this.setAttribute('open', '');
  },
  configurable: true,
});
Object.defineProperty(HTMLDialogElement.prototype, 'close', {
  value() {
    this.removeAttribute('open');
  },
  configurable: true,
});
