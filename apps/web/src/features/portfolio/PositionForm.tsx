import { useEffect, useId, useRef, useState, type FormEvent } from 'react';
import type { Fund } from '../../data/demo';
import {
  costInput,
  parseCost,
  parseUnits,
  valuePosition,
  money,
  type Position,
} from './model';

type Fields = 'code' | 'units' | 'cost';
export function PositionForm({
  funds,
  position,
  defaultCode,
  onSave,
  onCancel,
  submitError = '',
}: {
  funds: Fund[];
  position?: Position;
  defaultCode?: string;
  onSave: (position: Position) => boolean;
  onCancel: () => void;
  submitError?: string;
}) {
  const id = useId();
  const [code, setCode] = useState(
    position?.code ?? defaultCode ?? funds[0]?.code ?? '',
  );
  const [units, setUnits] = useState(position?.units ?? '');
  const [cost, setCost] = useState(position ? costInput(position) : '');
  const [errors, setErrors] = useState<Partial<Record<Fields, string>>>({});
  const codeRef = useRef<HTMLSelectElement>(null);
  const unitsRef = useRef<HTMLInputElement>(null);
  const costRef = useRef<HTMLInputElement>(null);
  const errorRef = useRef<HTMLParagraphElement>(null);
  const fund = funds.find((item) => item.code === code);
  const currency = position?.currency ?? fund?.currency ?? 'CNY';
  useEffect(() => {
    if (submitError) errorRef.current?.focus();
  }, [submitError]);
  function clearError(field: Fields) {
    setErrors((current) => ({ ...current, [field]: undefined }));
  }
  function submit(event: FormEvent) {
    event.preventDefault();
    const invalid: Partial<Record<Fields, string>> = {};
    let parsedUnits = '';
    let costMinor = 0;
    if (!fund && !position) invalid.code = '请选择目录中的基金';
    try {
      parsedUnits = parseUnits(units);
    } catch (cause) {
      invalid.units =
        '份额：' + (cause instanceof Error ? cause.message : '请检查输入');
    }
    try {
      costMinor = parseCost(cost, currency);
    } catch (cause) {
      invalid.cost =
        '成本：' + (cause instanceof Error ? cause.message : '请检查输入');
    }
    setErrors(invalid);
    if (Object.keys(invalid).length) {
      const field = invalid.code
        ? codeRef.current
        : invalid.units
          ? unitsRef.current
          : costRef.current;
      field?.focus();
      return;
    }
    onSave({ code, units: parsedUnits, costMinor, currency });
  }
  let estimate: number | null = null;
  try {
    estimate = valuePosition(
      { code, units: parseUnits(units), costMinor: 0, currency },
      fund,
    );
  } catch {
    /* incomplete input */
  }
  const error = Object.values(errors).filter(Boolean).join('；') || submitError;
  const description = (field: Fields) =>
    `${id}-help${errors[field] ? ' ' + id + '-error' : ''}`;
  return (
    <section
      className="panel position-editor"
      aria-label={position ? '编辑持仓' : '录入持仓'}
    >
      <div className="section-heading">
        <h2>{position ? '编辑持仓' : '录入持仓'}</h2>
        <button className="button-quiet" onClick={onCancel}>
          取消编辑
        </button>
      </div>
      <form onSubmit={submit} noValidate>
        <div className="form-grid">
          <div>
            <label htmlFor={`${id}-code`}>基金</label>
            <select
              id={`${id}-code`}
              ref={codeRef}
              value={code}
              disabled={!!position}
              aria-invalid={!!errors.code}
              aria-describedby={description('code')}
              onChange={(event) => {
                setCode(event.target.value);
                setCost('');
                clearError('code');
                clearError('cost');
              }}
            >
              <option value="" disabled>
                请选择基金
              </option>
              {position && !fund && (
                <option value={position.code}>
                  {position.code}（已离开目录）
                </option>
              )}
              {funds.map((item) => (
                <option key={item.code} value={item.code}>
                  {item.name} · {item.currency}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label htmlFor={`${id}-units`}>持有份额</label>
            <input
              id={`${id}-units`}
              ref={unitsRef}
              data-position-autofocus=""
              inputMode="decimal"
              autoComplete="off"
              value={units}
              onChange={(event) => {
                setUnits(event.target.value);
                clearError('units');
              }}
              placeholder="例如 1000.0000"
              aria-invalid={!!errors.units}
              aria-describedby={description('units')}
              autoFocus
            />
          </div>
          <div>
            <label htmlFor={`${id}-cost`}>总投入成本（{currency}）</label>
            <input
              id={`${id}-cost`}
              ref={costRef}
              inputMode={currency === 'KRW' ? 'numeric' : 'decimal'}
              autoComplete="off"
              value={cost}
              onChange={(event) => {
                setCost(event.target.value);
                clearError('cost');
              }}
              placeholder="包含你希望计入的费用"
              aria-invalid={!!errors.cost}
              aria-describedby={description('cost')}
            />
          </div>
        </div>
        <p id={`${id}-help`} className="field-help">
          份额最多 4 位小数；成本可为 0。零成本时收益率显示“—”。
          {estimate === null
            ? '缺少有效净值时暂不估值。'
            : `按当前样本净值估算：${money(estimate, currency)}。`}
        </p>
        {error && (
          <p
            id={`${id}-error`}
            ref={errorRef}
            tabIndex={-1}
            role="alert"
            className="form-error"
          >
            {error}
          </p>
        )}
        <div className="button-row form-actions">
          <button className="action" type="submit">
            保存持仓
          </button>
          <span className="field-help">
            只保存到此浏览器，使用虚构样本估值。
          </span>
        </div>
      </form>
    </section>
  );
}
