import { act, fireEvent, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { SiteHeader } from './SiteHeader';

afterEach(() => vi.unstubAllGlobals());

function header(path = '/funds') {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <SiteHeader />
      <main id="main-content" tabIndex={-1}>
        <button>页面操作</button>
      </main>
    </MemoryRouter>,
  );
}

describe('collapsible navigation', () => {
  it('starts closed, supports keyboard opening and restores focus on Escape', async () => {
    header();
    const toggle = screen.getByRole('button', { name: '打开导航菜单' });
    const nav = screen.getByRole('navigation', { name: '主导航' });
    expect(toggle).toHaveAttribute('aria-expanded', 'false');
    expect(toggle).toHaveAttribute('aria-controls', nav.id);
    expect(nav).toHaveAttribute('data-open', 'false');
    toggle.focus();
    await userEvent.keyboard('{Enter}');
    expect(toggle).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getByRole('link', { name: '首页' })).toHaveFocus();
    await userEvent.tab();
    expect(screen.getByRole('link', { name: '发现' })).toHaveFocus();
    await userEvent.keyboard('{Escape}');
    expect(toggle).toHaveFocus();
    expect(toggle).toHaveAttribute('aria-expanded', 'false');
    expect(nav).toHaveAttribute('data-open', 'false');
  });

  it('closes on navigation and returns to content when selecting the current page', async () => {
    header();
    await userEvent.click(screen.getByRole('button', { name: '打开导航菜单' }));
    await userEvent.click(screen.getByRole('link', { name: '基金' }));
    expect(screen.getByRole('main')).toHaveFocus();
    expect(
      screen.getByRole('button', { name: '打开导航菜单' }),
    ).toHaveAttribute('aria-expanded', 'false');
    await userEvent.click(screen.getByRole('button', { name: '打开导航菜单' }));
    await userEvent.click(screen.getByRole('link', { name: '我的组合' }));
    expect(screen.getByRole('link', { name: '我的组合' })).toHaveAttribute(
      'aria-current',
      'page',
    );
    expect(screen.getByRole('navigation')).toHaveAttribute(
      'data-open',
      'false',
    );
  });

  it('closes on outside interaction or when keyboard focus leaves the header', async () => {
    header();
    await userEvent.click(screen.getByRole('button', { name: '打开导航菜单' }));
    await userEvent.click(screen.getByRole('button', { name: '页面操作' }));
    expect(screen.getByRole('button', { name: '页面操作' })).toHaveFocus();
    expect(screen.getByRole('navigation')).toHaveAttribute(
      'data-open',
      'false',
    );
    await userEvent.click(screen.getByRole('button', { name: '打开导航菜单' }));
    for (let i = 0; i < 8; i++) await userEvent.tab();
    expect(screen.getByRole('button', { name: '页面操作' })).toHaveFocus();
    expect(screen.getByRole('navigation')).toHaveAttribute(
      'data-open',
      'false',
    );
  });

  it('clears mobile expansion when returning to desktop and removes the listener', async () => {
    const media = new EventTarget() as EventTarget & { matches: boolean };
    media.matches = false;
    const remove = vi.spyOn(media, 'removeEventListener');
    vi.stubGlobal(
      'matchMedia',
      vi.fn(() => media),
    );
    const view = header();
    await userEvent.click(screen.getByRole('button', { name: '打开导航菜单' }));
    media.matches = true;
    act(() => media.dispatchEvent(new Event('change')));
    expect(screen.getByRole('navigation')).toHaveAttribute(
      'data-open',
      'false',
    );
    view.unmount();
    expect(remove).toHaveBeenCalledWith('change', expect.any(Function));
  });

  it('includes an exact home link and keeps the fund section active on detail pages', () => {
    const view = header('/');
    expect(screen.getByRole('link', { name: '首页' })).toHaveAttribute(
      'aria-current',
      'page',
    );
    expect(screen.queryByText('示例预览')).not.toBeInTheDocument();
    view.unmount();
    header('/funds/DEMO-CN');
    expect(screen.getByRole('link', { name: '首页' })).not.toHaveAttribute(
      'aria-current',
    );
    expect(screen.getByRole('link', { name: '基金' })).toHaveAttribute(
      'aria-current',
      'page',
    );
  });

  it.each(['登录', '注册'])(
    'explains the %s entry without leaving the current page or creating an account',
    async (label) => {
      header();
      await userEvent.click(
        screen.getByRole('button', { name: '打开导航菜单' }),
      );
      const trigger = screen.getByRole('button', { name: label });
      await userEvent.click(trigger);
      const dialog = screen.getByRole('dialog', { name: `${label}暂未开放` });
      expect(dialog).toHaveAccessibleDescription(/记录保存在当前浏览器/);
      expect(within(dialog).getAllByRole('button')).toHaveLength(1);
      const close = within(dialog).getByRole('button', { name: '知道了' });
      expect(close).toHaveFocus();
      expect(screen.getByRole('navigation')).toHaveAttribute(
        'data-open',
        'false',
      );
      await userEvent.tab();
      expect(close).toHaveFocus();
      await userEvent.tab({ shift: true });
      expect(close).toHaveFocus();
      fireEvent(dialog, new Event('cancel', { cancelable: true }));
      expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
      expect(trigger).toHaveFocus();
      expect(screen.getByRole('link', { name: '基金' })).toHaveAttribute(
        'aria-current',
        'page',
      );
      await userEvent.click(trigger);
      await userEvent.click(screen.getByRole('button', { name: '知道了' }));
      expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
      expect(trigger).toHaveFocus();
    },
  );
});
