import {
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createMemoryRouter, RouterProvider } from 'react-router';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import fixture from '../test/fixtures/demo.json';
import { App } from '../App';
import { emptyPersonal, STORAGE_KEY, parsePersonal } from './local/state';

beforeEach(() => {
  localStorage.clear();
  vi.spyOn(globalThis, 'fetch').mockImplementation(
    async () => new Response(JSON.stringify(fixture)),
  );
});
function open(path: string) {
  return render(
    <RouterProvider
      router={createMemoryRouter([{ path: '*', element: <App /> }], {
        initialEntries: [path],
      })}
    />,
  );
}
function saved() {
  return JSON.parse(localStorage.getItem(STORAGE_KEY) ?? '{}');
}
async function fillPosition(units = '1000', cost = '1000.29') {
  await userEvent.type(
    screen.getByRole('textbox', { name: '持有份额' }),
    units,
  );
  await userEvent.type(
    screen.getByRole('textbox', { name: '持仓成本总额（CNY）' }),
    cost,
  );
  await userEvent.click(screen.getByRole('button', { name: '保存持仓' }));
}
describe('M1 desktop flows', () => {
  it('exports a backup that can be read back without losing money or shares', async () => {
    const state = {
      ...emptyPersonal(),
      watchlist: ['DEMO-CN'],
      positions: [
        {
          code: 'DEMO-CN',
          units: '1000.2500',
          costMinor: 100029,
          currency: 'CNY',
        },
      ],
    };
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
    let exported: Blob | undefined;
    const originalCreate = Object.getOwnPropertyDescriptor(
      URL,
      'createObjectURL',
    );
    const originalRevoke = Object.getOwnPropertyDescriptor(
      URL,
      'revokeObjectURL',
    );
    Object.defineProperty(URL, 'createObjectURL', {
      configurable: true,
      value: (blob: Blob) => {
        exported = blob;
        return 'blob:sora-test';
      },
    });
    Object.defineProperty(URL, 'revokeObjectURL', {
      configurable: true,
      value: () => {},
    });
    vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {});
    try {
      open('/portfolio');
      await userEvent.click(await screen.findByText('备份与数据管理'));
      await userEvent.click(
        await screen.findByRole('button', { name: '导出备份' }),
      );
      expect(exported).toBeDefined();
      const text =
        typeof exported!.text === 'function'
          ? await exported!.text()
          : await new Promise<string>((resolve, reject) => {
              const reader = new FileReader();
              reader.onload = () => resolve(String(reader.result));
              reader.onerror = reject;
              reader.readAsText(exported!);
            });
      expect(parsePersonal(text)).toEqual(state);
    } finally {
      if (originalCreate)
        Object.defineProperty(URL, 'createObjectURL', originalCreate);
      else Reflect.deleteProperty(URL, 'createObjectURL');
      if (originalRevoke)
        Object.defineProperty(URL, 'revokeObjectURL', originalRevoke);
      else Reflect.deleteProperty(URL, 'revokeObjectURL');
    }
  });
  it('starts with personal empty states and persists watch/unwatch across remounts', async () => {
    const first = open('/watchlist');
    expect(
      await screen.findByRole('heading', { name: '还没有关注的基金' }),
    ).toBeInTheDocument();
    await userEvent.click(screen.getByRole('link', { name: '基金' }));
    await userEvent.click(
      await screen.findByRole('button', { name: '关注宽基观察示例' }),
    );
    expect(saved().watchlist).toEqual(['DEMO-CN']);
    first.unmount();
    open('/watchlist');
    expect(
      await screen.findByRole('link', { name: '宽基观察示例' }),
    ).toBeInTheDocument();
    await userEvent.click(
      screen.getByRole('button', { name: '取消关注宽基观察示例' }),
    );
    expect(
      screen.getByRole('heading', { name: '还没有关注的基金' }),
    ).toBeInTheDocument();
    expect(saved().watchlist).toEqual([]);
  });
  it('opens filtered modules and fund details with actual sample history', async () => {
    open('/funds?module=technology');
    expect(
      await screen.findByRole('link', { name: '全球科技示例' }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole('link', { name: '宽基观察示例' }),
    ).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole('link', { name: '全球科技示例' }));
    expect(
      await screen.findByRole('heading', { level: 1, name: '全球科技示例' }),
    ).toBeInTheDocument();
    expect(screen.getByRole('img', { name: /净值趋势/ })).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: '近 7 天' }));
    expect(
      screen.getByRole('img', { name: /2026-10-13至2026-10-19/ }),
    ).toBeInTheDocument();
    expect(screen.getByRole('link', { name: '基金' })).toHaveAttribute(
      'aria-current',
      'page',
    );
  });
  it('handles direct missing-fund and missing-history URLs', async () => {
    const first = open('/funds/missing');
    expect(
      await screen.findByRole('heading', { name: '未找到这只基金' }),
    ).toBeInTheDocument();
    first.unmount();
    open('/funds/DEMO-BOND');
    expect(
      await screen.findByRole('heading', { name: '暂无走势数据' }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole('img', { name: /净值趋势/ }),
    ).not.toBeInTheDocument();
  });
  it('adds, edits and deletes positions with durable state and confirmation', async () => {
    const first = open('/portfolio?add=DEMO-CN');
    await screen.findByRole('textbox', { name: '持有份额' });
    await fillPosition();
    expect(saved().positions[0]).toMatchObject({
      code: 'DEMO-CN',
      units: '1000',
      costMinor: 100029,
      currency: 'CNY',
    });
    expect(
      within(screen.getByRole('region', { name: 'CNY 组合汇总' })).getByText(
        '¥1,042.00',
      ),
    ).toBeInTheDocument();
    first.unmount();
    open('/portfolio');
    await userEvent.click(
      await screen.findByRole('button', { name: '编辑宽基观察示例' }),
    );
    const units = screen.getByRole('textbox', { name: '持有份额' });
    await userEvent.clear(units);
    await userEvent.type(units, '2000');
    await userEvent.click(screen.getByRole('button', { name: '保存持仓' }));
    expect(saved().positions[0].units).toBe('2000');
    await userEvent.click(
      screen.getByRole('button', { name: '删除宽基观察示例' }),
    );
    await userEvent.click(screen.getByRole('button', { name: '取消' }));
    expect(saved().positions).toHaveLength(1);
    await userEvent.click(
      screen.getByRole('button', { name: '删除宽基观察示例' }),
    );
    await userEvent.click(screen.getByRole('button', { name: '确认删除' }));
    expect(saved().positions).toEqual([]);
    expect(
      screen.getByRole('heading', { name: '从第一笔持仓开始' }),
    ).toBeInTheDocument();
  });
  it('rejects invalid input and opens existing holdings for editing without duplication', async () => {
    open('/portfolio?add=DEMO-CN');
    await screen.findByRole('textbox', { name: '持有份额' });
    await fillPosition('-2', '10');
    expect(await screen.findByRole('alert')).toHaveTextContent('非负数字');
    expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
    await userEvent.clear(screen.getByRole('textbox', { name: '持有份额' }));
    await userEvent.type(
      screen.getByRole('textbox', { name: '持有份额' }),
      '1',
    );
    await userEvent.click(screen.getByRole('button', { name: '保存持仓' }));
    await userEvent.click(screen.getByRole('button', { name: '+ 录入持仓' }));
    expect(screen.getByRole('combobox', { name: '基金' })).toHaveValue('');
    await userEvent.selectOptions(
      screen.getByRole('combobox', { name: '基金' }),
      'DEMO-CN',
    );
    expect(
      screen.getByRole('region', { name: '编辑持仓' }),
    ).toBeInTheDocument();
    expect(screen.getByRole('textbox', { name: '持有份额' })).toHaveValue('1');
    expect(saved().positions).toHaveLength(1);
  });
  it('keeps incomplete valuations unknown and separates currencies', async () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        ...emptyPersonal(),
        positions: [
          { code: 'DEMO-CN', units: '1', costMinor: 0, currency: 'CNY' },
          { code: 'DEMO-BOND', units: '1', costMinor: 100, currency: 'CNY' },
          { code: 'DEMO-US', units: '1', costMinor: 100, currency: 'USD' },
        ],
      }),
    );
    open('/portfolio');
    const cny = await screen.findByRole('region', { name: 'CNY 组合汇总' });
    expect(within(cny).getByText('待完整估值')).toBeInTheDocument();
    expect(screen.getByText('待估值')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: /美元 USD/ }));
    expect(
      screen.getByRole('region', { name: 'USD 组合汇总' }),
    ).toBeInTheDocument();
  });
  it('preserves corrupted storage until the user explicitly recovers it', async () => {
    localStorage.setItem(STORAGE_KEY, 'corrupted');
    open('/portfolio');
    expect(await screen.findByRole('alert')).toHaveTextContent(
      '原始内容已保留',
    );
    expect(localStorage.getItem(STORAGE_KEY)).toBe('corrupted');
    expect(screen.getByRole('button', { name: '+ 录入持仓' })).toBeDisabled();
    await userEvent.click(screen.getByRole('button', { name: '清空数据' }));
    await userEvent.click(screen.getByRole('button', { name: '确认替换' }));
    expect(saved()).toEqual(emptyPersonal());
    expect(screen.getByRole('button', { name: '+ 录入持仓' })).toBeEnabled();
  });
  it('shows a write failure without pretending the change was saved', async () => {
    open('/funds');
    await screen.findByRole('button', { name: '关注宽基观察示例' });
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new Error('quota');
    });
    await userEvent.click(
      screen.getByRole('button', { name: '关注宽基观察示例' }),
    );
    expect(await screen.findByRole('alert')).toHaveTextContent('修改未生效');
    expect(
      screen.getByRole('button', { name: '关注宽基观察示例' }),
    ).toHaveAttribute('aria-pressed', 'false');
  });
  it('syncs other tabs and blocks undelivered conflicting writes', async () => {
    open('/funds');
    await screen.findByRole('button', { name: '关注宽基观察示例' });
    const raw = JSON.stringify({ ...emptyPersonal(), watchlist: ['DEMO-US'] });
    localStorage.setItem(STORAGE_KEY, raw);
    await userEvent.click(
      screen.getByRole('button', { name: '关注宽基观察示例' }),
    );
    expect(await screen.findByRole('alert')).toHaveTextContent('另一个页面');
    fireEvent(
      window,
      new StorageEvent('storage', { key: STORAGE_KEY, newValue: raw }),
    );
    expect(
      screen.getByRole('button', { name: '取消关注全球科技示例' }),
    ).toHaveAttribute('aria-pressed', 'true');
  });
  it('refuses to save an edit when another tab has changed that holding', async () => {
    const position = {
      code: 'DEMO-CN',
      units: '1',
      costMinor: 100,
      currency: 'CNY',
    };
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ ...emptyPersonal(), positions: [position] }),
    );
    open('/portfolio?add=DEMO-CN');
    await screen.findByRole('region', { name: '编辑持仓' });
    const updated = {
      ...emptyPersonal(),
      positions: [{ ...position, units: '2' }],
    };
    const raw = JSON.stringify(updated);
    localStorage.setItem(STORAGE_KEY, raw);
    fireEvent(
      window,
      new StorageEvent('storage', { key: STORAGE_KEY, newValue: raw }),
    );
    await userEvent.click(screen.getByRole('button', { name: '保存持仓' }));
    expect(await screen.findByRole('alert')).toHaveTextContent(
      '已在其他页面更改',
    );
    expect(saved()).toEqual(updated);
  });
  it('validates import, previews replacement and preserves data on cancellation', async () => {
    open('/portfolio');
    await screen.findByRole('heading', { name: '从第一笔持仓开始' });
    await userEvent.click(screen.getByText('备份与数据管理'));
    const file = (text: string) => {
      const value = new File([text], 'backup.json', {
        type: 'application/json',
      });
      Object.defineProperty(value, 'text', { value: async () => text });
      return value;
    };
    await userEvent.upload(screen.getByLabelText('导入备份'), file('{bad'));
    expect(await screen.findByRole('alert')).toHaveTextContent(
      '格式或版本不支持',
    );
    const raw = JSON.stringify({ ...emptyPersonal(), watchlist: ['DEMO-CN'] });
    await userEvent.upload(screen.getByLabelText('导入备份'), file(raw));
    expect(await screen.findByRole('dialog')).toHaveTextContent('1 项关注');
    await userEvent.click(screen.getByRole('button', { name: '取消' }));
    expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
    await userEvent.upload(screen.getByLabelText('导入备份'), file(raw));
    await userEvent.click(screen.getByRole('button', { name: '确认替换' }));
    expect(saved().watchlist).toEqual(['DEMO-CN']);
    await waitFor(() =>
      expect(screen.queryByRole('dialog')).not.toBeInTheDocument(),
    );
  });
});
