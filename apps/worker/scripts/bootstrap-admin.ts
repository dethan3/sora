import { execFile } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { createInterface } from 'node:readline/promises';
import { Writable } from 'node:stream';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { bootstrapStatements } from '../src/bootstrap.ts';
import { hashPassword, normalizeEmail } from '../src/security.ts';
import { renderSql } from '../src/seed.ts';

// Credentials are read from a masked prompt or JSON stdin, never argv/env/logs.
try {
  const args = process.argv.slice(2);
  const local = args.includes('--local');
  const remote = args.includes('--remote');
  if (local === remote) throw new Error('必须明确指定 --local 或 --remote');
  const persistIndex = args.indexOf('--persist-to');
  const persist = persistIndex >= 0 ? args[persistIndex + 1] : undefined;
  if (persistIndex >= 0 && (!local || !persist))
    throw new Error('--persist-to 仅用于本地数据库');
  const allowed = new Set([
    '--local',
    '--remote',
    '--persist-to',
    ...(persist ? [persist] : []),
  ]);
  if (args.some((arg) => !allowed.has(arg)))
    throw new Error('不支持的参数；邮箱和密码请通过提示或标准输入提供');
  let email: unknown;
  let password: unknown;
  if (process.stdin.isTTY) {
    let muted = false;
    const output = new Writable({
      write(chunk, _encoding, callback) {
        if (!muted) process.stdout.write(chunk);
        callback();
      },
    });
    const prompt = createInterface({
      input: process.stdin,
      output,
      terminal: true,
    });
    try {
      email = await prompt.question('首个管理员邮箱：');
      process.stdout.write('密码（12～128 字符，不回显）：');
      muted = true;
      password = await prompt.question('');
      process.stdout.write('\n');
    } finally {
      prompt.close();
    }
  } else {
    process.stdin.setEncoding('utf8');
    let input = '';
    for await (const chunk of process.stdin) {
      input += chunk.toString();
      if (input.length > 4096) throw new Error('输入过大');
    }
    ({ email, password } = JSON.parse(input));
  }
  const normalized = normalizeEmail(email);
  if (!normalized || typeof password !== 'string')
    throw new Error('请提供有效邮箱和密码');
  const statements = bootstrapStatements(
    normalized,
    await hashPassword(password),
    new Date().toISOString(),
  );
  const scratch = await mkdtemp(join(tmpdir(), 'sora-bootstrap-'));
  try {
    const sql = join(scratch, 'bootstrap.sql');
    await writeFile(sql, renderSql(statements), { mode: 0o600 });
    const require = createRequire(import.meta.url);
    const wrangler = resolve(
      dirname(require.resolve('wrangler')),
      '../bin/wrangler.js',
    );
    let result: string;
    try {
      ({ stdout: result } = await promisify(execFile)(
        process.execPath,
        [
          wrangler,
          'd1',
          'execute',
          'sora-db',
          local ? '--local' : '--remote',
          '--file',
          sql,
          '--json',
          ...(persist ? ['--persist-to', persist] : []),
        ],
        {
          cwd: fileURLToPath(new URL('../', import.meta.url)),
          timeout: 60_000,
        },
      ));
    } catch {
      throw new Error('管理员初始化失败，请检查 D1 迁移和 Wrangler 权限');
    }
    const rows = JSON.parse(result) as { results?: { id?: string }[] }[];
    // Local Wrangler returns duration-only metadata. Verify this invocation's
    // generated ID explicitly, rather than relying on optional write counts.
    if (
      !rows.some((row) =>
        row.results?.some((user) => user.id === statements[0].params[0]),
      )
    )
      throw new Error('已有管理员或邮箱已存在，初始化未执行');
    console.log('首个管理员已创建。请从登录页登录，再生成邀请码。');
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
} catch (error) {
  console.error(error instanceof Error ? error.message : '管理员初始化失败');
  process.exitCode = 1;
}
