import { StrictMode } from 'react';
import { fireEvent, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import fixture from '../../public/data/demo.json';
import { App } from '../App';

beforeEach(() => {
  localStorage.clear();
  vi.spyOn(globalThis, 'fetch').mockImplementation(
    async () => new Response(JSON.stringify(fixture)),
  );
});
function open(path: string) {
  return render(
    <StrictMode>
      <MemoryRouter initialEntries={[path]}>
        <App />
      </MemoryRouter>
    </StrictMode>,
  );
}

describe('M2 keyboard and accessible flows', () => {
  it('focuses invalid fields, associates errors and returns focus after saving', async () => {
    open('/portfolio?add=DEMO-CN');
    const units = await screen.findByRole('textbox', { name: '持有份额' });
    const cost = screen.getByRole('textbox', { name: '总投入成本（CNY）' });
    expect(units).toHaveFocus();
    await userEvent.click(screen.getByRole('button', { name: '保存持仓' }));
    expect(units).toHaveFocus();
    expect(units).toHaveAttribute('aria-invalid', 'true');
    expect(cost).toHaveAttribute('aria-invalid', 'true');
    const error = screen.getByRole('alert');
    expect(units.getAttribute('aria-describedby')).toContain(error.id);
    await userEvent.type(units, '10.25');
    expect(units).toHaveAttribute('aria-invalid', 'false');
    await userEvent.click(screen.getByRole('button', { name: '保存持仓' }));
    expect(cost).toHaveFocus();
    await userEvent.type(cost, '10.29');
    await userEvent.keyboard('{Enter}');
    expect(
      screen.queryByRole('region', { name: '录入持仓' }),
    ).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: '+ 录入持仓' })).toHaveFocus();
  });
  it('describes confirmation, locks background scroll and restores focus on cancel', async () => {
    open('/portfolio');
    const trigger = await screen.findByRole('button', { name: '清空数据' });
    const previousOverflow = document.body.style.overflow;
    await userEvent.click(trigger);
    const dialog = screen.getByRole('dialog', { name: '清空本地关注与持仓？' });
    expect(dialog).toHaveAccessibleDescription(/替换后将有/);
    expect(within(dialog).getByRole('button', { name: '取消' })).toHaveFocus();
    expect(document.body.style.overflow).toBe('hidden');
    await userEvent.tab({ shift: true });
    expect(
      within(dialog).getByRole('button', { name: '确认替换' }),
    ).toHaveFocus();
    await userEvent.tab();
    expect(within(dialog).getByRole('button', { name: '取消' })).toHaveFocus();
    fireEvent(dialog, new Event('cancel', { cancelable: true }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(document.body.style.overflow).toBe(previousOverflow);
    expect(trigger).toHaveFocus();
  });
  it('keeps one semantic table and one action per fund for all layouts', async () => {
    open('/funds');
    const table = await screen.findByRole('table', { name: '基金示例列表' });
    expect(within(table).getAllByRole('columnheader')).toHaveLength(5);
    expect(within(table).getAllByRole('rowheader')).toHaveLength(4);
    expect(within(table).getAllByRole('button')).toHaveLength(4);
    expect(screen.getAllByRole('navigation', { name: '主导航' })).toHaveLength(
      1,
    );
    expect(
      within(table).getByRole('button', { name: '关注稳健债券示例' }),
    ).toHaveAccessibleName('关注稳健债券示例');
  });
  it('announces filtered counts while preserving keyboard input focus', async () => {
    open('/funds');
    const search = await screen.findByRole('searchbox', {
      name: '搜索示例基金',
    });
    await userEvent.type(search, 'DEMO-KR');
    expect(search).toHaveFocus();
    expect(screen.getByRole('status')).toHaveTextContent('1 只基金');
    await userEvent.clear(search);
    await userEvent.type(search, '没有结果');
    expect(screen.getByRole('status')).toHaveTextContent('0 只基金');
    expect(
      screen.getByRole('heading', { name: '暂无基金' }),
    ).toBeInTheDocument();
  });
});
