import {
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createMemoryRouter, RouterProvider, useNavigate } from 'react-router';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import fixture from '../../public/data/demo.json';
import { App } from '../App';
import { emptyPersonal, STORAGE_KEY } from './local/state';
import { detailSource, listSource } from '../navigation/flow';

beforeEach(() => {
  localStorage.clear();
  sessionStorage.clear();
  vi.spyOn(globalThis, 'fetch').mockImplementation(
    async () => new Response(JSON.stringify(fixture)),
  );
});
function HistoryBack() {
  const navigate = useNavigate();
  return <button onClick={() => navigate(-1)}>返回上一页（测试）</button>;
}
function open(entries: string[] = ['/'], withBack = false) {
  const router = createMemoryRouter(
    [
      {
        path: '*',
        element: (
          <>
            <App />
            {withBack && <HistoryBack />}
          </>
        ),
      },
    ],
    {
      initialEntries: entries,
    },
  );
  return { ...render(<RouterProvider router={router} />), router };
}
const saved = () => JSON.parse(localStorage.getItem(STORAGE_KEY) ?? '{}');
const click = (name: string | RegExp) =>
  userEvent.click(screen.getByRole('button', { name }));
const units = () => screen.getByRole('textbox', { name: '持有份额' });
const cost = (currency = 'CNY') =>
  screen.getByRole('textbox', { name: `持仓成本总额（${currency}）` });
const position = {
  code: 'DEMO-CN',
  units: '100',
  costMinor: 10000,
  currency: 'CNY',
};

describe('continuous product journeys', () => {
  it('connects search, watch, holding entry and return to the original filtered result', async () => {
    open(['/funds?module=technology&q=DEMO-US']);
    await userEvent.click(
      await screen.findByRole('link', { name: '全球科技示例' }),
    );
    await click('关注全球科技示例');
    await userEvent.click(screen.getByRole('link', { name: '录入持仓' }));
    await screen.findByRole('region', { name: '录入持仓' });
    expect(screen.getByRole('combobox', { name: '基金' })).toHaveValue(
      'DEMO-US',
    );
    await userEvent.type(units(), '10.25');
    await userEvent.type(cost('USD'), '10.29');
    await click('保存持仓');
    expect(saved().positions).toEqual([
      { code: 'DEMO-US', units: '10.25', costMinor: 1029, currency: 'USD' },
    ]);
    expect(
      screen.getByRole('button', { name: '编辑全球科技示例' }),
    ).toHaveFocus();
    const unloadAfterSave = new Event('beforeunload', { cancelable: true });
    fireEvent(window, unloadAfterSave);
    expect(unloadAfterSave.defaultPrevented).toBe(false);
    await userEvent.click(screen.getByRole('link', { name: '← 返回基金详情' }));
    expect(screen.getByRole('link', { name: '编辑持仓' })).toBeInTheDocument();
    await userEvent.click(screen.getByRole('link', { name: '← 基金目录' }));
    expect(screen.getByRole('searchbox')).toHaveValue('DEMO-US');
    expect(screen.getByRole('combobox')).toHaveValue('technology');
    expect(screen.getByRole('link', { name: '全球科技示例' })).toHaveFocus();
    await userEvent.click(screen.getByRole('link', { name: '关注' }));
    expect(
      screen.getByRole('link', { name: '全球科技示例' }),
    ).toBeInTheDocument();
    expect(saved().watchlist).toEqual(['DEMO-US']);
  });

  it('retains the return destination and focus after reloading a detail page', async () => {
    const first = open(['/funds?q=DEMO-KR']);
    await userEvent.click(
      await screen.findByRole('link', { name: '韩国市场示例' }),
    );
    const detail =
      first.router.state.location.pathname + first.router.state.location.search;
    first.unmount();
    open([detail]);
    await userEvent.click(
      await screen.findByRole('link', { name: '← 基金目录' }),
    );
    expect(screen.getByRole('searchbox')).toHaveValue('DEMO-KR');
    expect(screen.getByRole('link', { name: '韩国市场示例' })).toHaveFocus();
  });

  it('returns to watchlist rather than sending a watchlist user to the library', async () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ ...emptyPersonal(), watchlist: ['DEMO-CN'] }),
    );
    open(['/watchlist']);
    await userEvent.click(
      await screen.findByRole('link', { name: '宽基观察示例' }),
    );
    await userEvent.click(screen.getByRole('link', { name: '← 我的关注' }));
    expect(
      screen.getByRole('heading', { level: 1, name: '我的关注' }),
    ).toBeInTheDocument();
    expect(screen.getByRole('link', { name: '宽基观察示例' })).toHaveFocus();
  });

  it('protects a browser Back navigation, resumes the field and only leaves on explicit discard', async () => {
    const { router } = open(['/funds', '/portfolio?add=DEMO-CN'], true);
    await screen.findByRole('textbox', { name: '持有份额' });
    await userEvent.type(units(), '123.45');
    await click('返回上一页（测试）');
    expect(
      screen.getByRole('dialog', { name: '放弃未保存的修改？' }),
    ).toBeInTheDocument();
    await click('继续编辑');
    expect(units()).toHaveValue('123.45');
    expect(units()).toHaveFocus();
    await click('返回上一页（测试）');
    await click('放弃修改');
    await waitFor(() => expect(router.state.location.pathname).toBe('/funds'));
    expect(
      await screen.findByRole('heading', { level: 1, name: '基金' }),
    ).toBeInTheDocument();
    expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
  });

  it('protects cancel, replacing a filled form and native unload without silently saving', async () => {
    open(['/portfolio?add=DEMO-CN']);
    await screen.findByRole('textbox', { name: '持有份额' });
    await userEvent.type(units(), '12.25');
    const unload = new Event('beforeunload', { cancelable: true });
    fireEvent(window, unload);
    expect(unload.defaultPrevented).toBe(true);
    await click('取消编辑');
    await click('继续编辑');
    expect(units()).toHaveValue('12.25');
    await userEvent.selectOptions(
      screen.getByRole('combobox', { name: '基金' }),
      'DEMO-US',
    );
    await click('继续编辑');
    expect(screen.getByRole('combobox')).toHaveValue('DEMO-CN');
    expect(units()).toHaveValue('12.25');
    await userEvent.selectOptions(screen.getByRole('combobox'), 'DEMO-US');
    await click('放弃修改');
    expect(screen.getByRole('combobox')).toHaveValue('DEMO-US');
    expect(units()).toHaveValue('');
    expect(cost('USD')).toHaveValue('');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
    await userEvent.click(screen.getByRole('link', { name: '基金' }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('starts from an explicit fund choice and opens existing holdings without duplication', async () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ ...emptyPersonal(), positions: [position] }),
    );
    open(['/']);
    await userEvent.click(screen.getByRole('link', { name: '回到我的组合' }));
    await click('+ 录入持仓');
    expect(screen.getByRole('combobox')).toHaveValue('');
    expect(screen.getByRole('combobox')).toHaveFocus();
    expect(units()).toBeDisabled();
    await userEvent.selectOptions(screen.getByRole('combobox'), 'DEMO-CN');
    expect(
      screen.getByRole('region', { name: '编辑持仓' }),
    ).toBeInTheDocument();
    expect(units()).toHaveValue('100');
    expect(cost()).toHaveValue('100.00');
    expect(saved().positions).toHaveLength(1);
  });

  it('resets a restarted editor only after explicit discard and enables protection again for new input', async () => {
    open(['/portfolio']);
    await userEvent.click(
      await screen.findByRole('button', { name: '+ 录入持仓' }),
    );
    await userEvent.selectOptions(screen.getByRole('combobox'), 'DEMO-CN');
    await userEvent.type(units(), '88');
    await click('+ 录入持仓');
    await click('继续编辑');
    expect(units()).toHaveValue('88');
    await click('+ 录入持仓');
    await click('放弃修改');
    expect(screen.getByRole('combobox')).toHaveValue('');
    expect(units()).toHaveValue('');
    await userEvent.selectOptions(screen.getByRole('combobox'), 'DEMO-US');
    await userEvent.type(units(), '99');
    await userEvent.click(screen.getByRole('link', { name: '基金' }));
    expect(screen.getByRole('dialog')).toBeInTheDocument();
    await click('继续编辑');
    expect(units()).toHaveValue('99');
    expect(units()).toHaveFocus();
  });

  it('shows one currency at a time, preserves the selected currency on return and restores edit focus', async () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        ...emptyPersonal(),
        positions: [
          position,
          { ...position, code: 'DEMO-US', currency: 'USD' },
        ],
      }),
    );
    const { router } = open(['/portfolio?currency=USD']);
    await screen.findByRole('region', { name: 'USD 组合汇总' });
    expect(
      screen.queryByRole('region', { name: 'CNY 组合汇总' }),
    ).not.toBeInTheDocument();
    expect(
      within(screen.getByRole('table')).getAllByRole('rowheader'),
    ).toHaveLength(1);
    await userEvent.click(screen.getByRole('link', { name: '全球科技示例' }));
    await userEvent.click(screen.getByRole('link', { name: '← 我的组合' }));
    expect(
      screen.getByRole('region', { name: 'USD 组合汇总' }),
    ).toBeInTheDocument();
    await click('编辑全球科技示例');
    await userEvent.clear(units());
    await userEvent.type(units(), '101.5');
    await click('保存持仓');
    expect(
      screen.getByRole('button', { name: '编辑全球科技示例' }),
    ).toHaveFocus();
    expect(
      saved().positions.find(
        (item: { code: string }) => item.code === 'DEMO-US',
      ).units,
    ).toBe('101.5');
    await click('删除全球科技示例');
    await click('确认删除');
    expect(
      screen.getByRole('region', { name: 'CNY 组合汇总' }),
    ).toBeInTheDocument();
    expect(
      new URLSearchParams(router.state.location.search).get('currency'),
    ).toBe('CNY');
    expect(saved().positions).toHaveLength(1);
  });

  it('protects an edit before destructive data management and does not overwrite on cancel', async () => {
    const original = { ...emptyPersonal(), positions: [position] };
    localStorage.setItem(STORAGE_KEY, JSON.stringify(original));
    open(['/portfolio?add=DEMO-CN']);
    await screen.findByRole('region', { name: '编辑持仓' });
    await userEvent.clear(units());
    await userEvent.type(units(), '200');
    await userEvent.click(screen.getByText('备份与数据管理'));
    await click('清空数据');
    await click('继续编辑');
    expect(units()).toHaveValue('200');
    expect(saved()).toEqual(original);
    await click('清空数据');
    await click('放弃修改');
    expect(
      screen.getByRole('dialog', { name: '清空本地关注与持仓？' }),
    ).toBeInTheDocument();
    await click('取消');
    expect(saved()).toEqual(original);
    expect(document.body.style.overflow).not.toBe('hidden');
    expect(
      screen.queryByRole('region', { name: '编辑持仓' }),
    ).not.toBeInTheDocument();
  });

  it('keeps input and stored data unchanged on a failed save', async () => {
    open(['/portfolio?add=DEMO-CN']);
    await screen.findByRole('textbox', { name: '持有份额' });
    await userEvent.type(units(), '10.25');
    await userEvent.type(cost(), '10.29');
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new Error('quota');
    });
    await click('保存持仓');
    expect(screen.getByRole('alert')).toHaveTextContent('修改未生效');
    expect(screen.getByRole('alert')).toHaveFocus();
    expect(units()).toHaveValue('10.25');
    expect(cost()).toHaveValue('10.29');
    expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
    await userEvent.click(screen.getByRole('link', { name: '基金' }));
    await click('放弃修改');
    expect(
      screen.getByRole('heading', { level: 1, name: '基金' }),
    ).toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('provides a real first-use action and a recoverable empty search', async () => {
    open();
    await userEvent.click(screen.getByRole('link', { name: '建立我的组合' }));
    await click('录入第一笔持仓');
    expect(screen.getByRole('combobox')).toHaveValue('');
    await userEvent.click(screen.getByRole('link', { name: '基金' }));
    await userEvent.type(screen.getByRole('searchbox'), 'no-such-fund');
    await click('清除筛选');
    expect(screen.getByRole('searchbox')).toHaveValue('');
    expect(screen.getByRole('searchbox')).toHaveFocus();
    expect(
      within(screen.getByRole('table')).getAllByRole('rowheader'),
    ).toHaveLength(4);
  });

  it('restricts persisted return links to supported internal destinations', () => {
    for (const value of [
      'https://other.test/funds',
      '//other.test/funds',
      '/\\other.test/funds',
      '/not-a-route',
    ]) {
      expect(listSource(value)).toBeNull();
      expect(detailSource(value)).toBeNull();
    }
    expect(listSource('/funds?q=科技&module=technology&ignored=1')).toBe(
      '/funds?q=%E7%A7%91%E6%8A%80&module=technology',
    );
    expect(detailSource('/funds/DEMO-CN?from=https://other.test')).toBe(
      '/funds/DEMO-CN',
    );
  });
});
