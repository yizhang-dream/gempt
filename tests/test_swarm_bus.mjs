import { createGifEncoder } from '../mcp_app/swarm-gif.js';
import assert from 'node:assert/strict';
import { execFileSync, spawn, spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { createServer, get as httpGet } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import test from 'node:test';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { resolvePath } from '../core/state.mjs';
import { openDatabase } from '../core/sqlite.mjs';
import {
  archiveChannel, computeMentionEdges, createChannel, detectSystemAttention, listChannels, markRead, openSwarmDb,
  parkWorker, resetRateLimiter, subscribeChannels,
  ensureSwarm, getBoardHead, getSwarmOverview, getWorker, killWorkerRecord, listRoster, listSwarms,
  listUndeliveredForWorker, markDelivered, parseMentions, parseScreenshots, postBoardMessage,
  readBoardMessages, resolveSwarmDbPath, resolveSwarmId, setSwarmPlan, setSwarmTitle, updateWorker,
  findCacheKeyByNativeSession, upsertRunLineage, upsertWorker, validateWorkerName,
} from '../core/swarm-bus.mjs';
import {
  activeAt, channelMentions, channelRegistry, channelsAt, countsUnread, currentChannels, effectiveChannels,
  homeChannelOf, memberView, mentions, mentionsIn, mentionsOf, seesIn, validateChannelName, validateTopic, visibleUnread,
} from '../core/swarm-channels.mjs';
import { BOARD_TOOLS, handleBoardMcpTool } from '../core/board-mcp.mjs';
import { buildWorkerInstructions, handleBoardAction, handleSwarmAction, onWorkerTurnFinished } from '../core/swarm-engine.mjs';
import { ensureBranchWorkspace, ensureShareWorktree } from '../core/worktrees.mjs';
import { narratorDirectory, publishNarratorView, readNarratorView } from '../core/swarm-narrator.mjs';
import { detectMonitorStall, MONITOR_INTERVAL_MS, runMonitorTick, sweepSwarmMonitors } from '../core/swarm-monitor.mjs';
import { runSwarmOperation } from '../core/swarm-service.mjs';
import { reconcileRunState, saveRun, tryDelegateDispatch } from '../core/runs.mjs';
import { handleTool, TOOLS } from '../core/mcp-server.mjs';
import { runWait, waitForBoard } from '../core/wait.mjs';
import { dispatchPageApi } from '../magpie_plugin/lib/page-api.mjs';
import { createGemptTools } from '../magpie_plugin/lib/tools.mjs';
import { startServer } from '../magpie_plugin/lib/server.mjs';
import { runAppServer } from '../magpie_plugin/lib/app-server-run.mjs';
import { compactSwarmView, groupWorkersByThreads, tokenizeBoardText } from '../mcp_app/swarm-ui.js';
import { serveAppServerSocket } from './app-server-fixture.mjs';

test('compact Swarm follows visible workers, board activity and pending questions', () => {
  const overview = { swarm: { id: 'board' }, roster: [{ state: 'waiting' }, { state: 'killed' }] };
  assert.equal(compactSwarmView(overview), true);
  assert.equal(compactSwarmView({ ...overview, roster: [] }), false);
  assert.equal(compactSwarmView({ ...overview, roster: [{ state: 'waiting_reply' }] }), false);
  assert.equal(compactSwarmView({ ...overview, roster: [{ state: 'working' }, { state: 'waiting' }] }), false);
  assert.equal(compactSwarmView(overview, [{ kind: 'system' }]), true);
  assert.equal(compactSwarmView(overview, [{ from_agent: 'alice' }]), false);
  assert.equal(compactSwarmView(overview, [{ from_agent: 'alice' }, { from_agent: 'bob' }]), false);
  assert.equal(compactSwarmView(overview, [{ from_agent: 'alice', mentions: ['bob'] }]), false);
});

const REPO_ROOT = fileURLToPath(new URL('../', import.meta.url));

async function startMockAppServer(dir, { onThreadStart, onTurnStart } = {}) {
  const token = 'testtoken123';
  const gatewayUrl = 'http://127.0.0.1:3425';
  const statePath = join(dir, 'app-server.json');
  const pendingCalls = new Map();
  let reqId = 100;
  let threadSeq = 0;

  const server = createServer();
  const sockets = new Set();
  server.on('upgrade', (req, socket) => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
    serveAppServerSocket(req, socket, (pkt, send) => {
      if (pkt.id != null && !pkt.method) {
        const resolver = pendingCalls.get(pkt.id);
        if (resolver) {
          pendingCalls.delete(pkt.id);
          resolver(pkt.result);
        }
        return;
      }
      if (pkt.method === 'initialize') {
        send({ id: pkt.id, result: { serverInfo: { name: 'mock-codex', version: '1.0' } } });
      } else if (pkt.method === 'thread/start') {
        onThreadStart?.(pkt.params);
        const tid = `thr_${++threadSeq}`;
        const sandboxType = pkt.params.sandbox === 'read-only' ? 'readOnly' : 'workspaceWrite';
        send({
          id: pkt.id,
          result: {
            thread: { id: tid },
            cwd: pkt.params.cwd,
            sandbox: { type: sandboxType },
            modelProvider: pkt.params.modelProvider,
          },
        });
      } else if (pkt.method === 'turn/start') {
        const turnId = `turn_${threadSeq}`;
        const threadId = pkt.params.threadId;
        send({ id: pkt.id, result: { turn: { id: turnId } } });
        send({ method: 'turn/started', params: { threadId, turnId } });
        const callTool = (tool, args, callId = `call_${++reqId}`) => new Promise((resolveCall) => {
          const id = ++reqId;
          pendingCalls.set(id, resolveCall);
          send({ id, method: 'item/tool/call', params: { threadId, turnId, callId, tool, arguments: args } });
        });
        Promise.resolve(onTurnStart ? onTurnStart(pkt.params, { callTool, threadId, turnId }) : { text: 'ok' })
          .then((out) => {
            const text = out?.text || 'ok';
            send({ method: 'item/completed', params: { threadId, turnId, item: { id: 'msg1', type: 'agentMessage', text } } });
            send({ method: 'turn/completed', params: { threadId, turn: { id: turnId, status: 'completed' } } });
          })
          .catch((err) => {
            send({ method: 'turn/completed', params: { threadId, turn: { id: turnId, status: 'failed', error: { message: String(err?.stack || err) } } } });
          });
      }
    });
  });

  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  const { port } = server.address();
  writeFileSync(statePath, JSON.stringify({
    status: 'running',
    ws_url: `ws://127.0.0.1:${port}/`,
    auth_header: 'Authorization',
    auth_token: token,
    gateway_url: gatewayUrl,
  }));

  return {
    statePath,
    gatewayUrl,
    close: () => new Promise((r) => {
      for (const s of sockets) s.destroy();
      server.closeAllConnections?.();
      server.close(r);
    }),
  };
}

async function getPageKey(base) {
  const html = await new Promise((resolveHtml, rejectHtml) => {
    httpGet(base + '/', (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => resolveHtml(Buffer.concat(chunks).toString('utf8')));
      res.on('error', rejectHtml);
    }).on('error', rejectHtml);
  });
  return html.match(/name="gempt-key" content="([^"]+)"/)?.[1];
}

function makeTmp(t, prefix = 'gempt-swarm-') {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  t.after(() => {
    try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
  });
  return dir;
}

test('sqlite wrapper supports CRUD, WAL pragmas, and nested transactions', async (t) => {
  const dir = makeTmp(t);
  const dbPath = join(dir, 'sub', 'test.sqlite');
  const db = await openDatabase(dbPath);
  t.after(() => db.close());

  assert.ok(db.runtime === 'node' || db.runtime === 'bun');
  const mode = db.get('PRAGMA journal_mode');
  assert.equal(String(mode?.journal_mode || '').toLowerCase(), 'wal');

  db.exec('CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)');
  const r1 = db.run('INSERT INTO items (name) VALUES (?)', 'alpha');
  assert.equal(r1.changes, 1);
  assert.equal(r1.lastInsertRowid, 1);

  db.transaction(() => {
    db.run('INSERT INTO items (name) VALUES (?)', 'beta');
    assert.throws(() => {
      db.transaction(() => {
        db.run('INSERT INTO items (name) VALUES (?)', 'rolled-back');
        throw new Error('boom');
      });
    }, /boom/);
  });

  const rows = db.all('SELECT id, name FROM items ORDER BY id ASC');
  assert.deepEqual(rows, [{ id: 1, name: 'alpha' }, { id: 2, name: 'beta' }]);
  assert.equal(db.get('SELECT * FROM items WHERE id = ?', 999), null);
});

test('external linked worktree Swarm scopes the board and verifies swarm and worker screenshot roots', async (t) => {
  const dir = resolvePath(makeTmp(t));
  const sessionsDir = join(dir, 'sessions');
  const repo = join(dir, 'repo');
  const tree = join(dir, 'swarm-tree');
  const workerTree = join(dir, 'worker-tree');
  const other = join(dir, 'other');
  mkdirSync(repo);
  mkdirSync(other);
  const git = (...args) => execFileSync('git', ['-C', repo, ...args], { windowsHide: true, stdio: 'pipe' });
  git('init', '-q');
  git('-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', '-c', 'commit.gpgsign=false',
    'commit', '--allow-empty', '-qm', 'fixture');
  git('worktree', 'add', '--detach', tree);
  git('worktree', 'add', '--detach', workerTree);

  const opts = { sessionsDir, configPath: join(dir, 'config.json'), workspaceRoots: [repo] };
  const unrelated = { ...opts, workspaceRoots: [other] };
  const swarm_id = 'sw-linked';
  await ensureSwarm({ swarmId: swarm_id, cwd: tree, sessionsDir });
  await upsertWorker(swarm_id, { name: 'builder', worktree_path: workerTree, source_cwd: repo }, opts);
  await upsertWorker(swarm_id, { name: 'old-worker', worktree_path: other, source_cwd: repo }, opts);
  const paths = [join(tree, 'board.png'), join(workerTree, 'worker.png'), join(other, 'other.png')];
  for (const path of paths) writeFileSync(path, 'fixture');
  const message = await postBoardMessage(swarm_id, { text: 'Screenshots', screenshots:
    ['board.png', ...paths.slice(1)].map((path) => ({ path, caption: path })) }, opts);
  assert.deepEqual((await runSwarmOperation('list', { scope: 'all' }, opts)).swarms.map((s) => s.id), [swarm_id]);
  const board = await runSwarmOperation('board', { swarm_id }, opts);
  assert.deepEqual(board.messages.map((m) => m.seq), [message.seq]);
  const { screenshots } = await runSwarmOperation('overview', { swarm_id }, opts);
  assert.deepEqual(screenshots.map((s) => s.file_uri), [pathToFileURL(paths[0]).href, pathToFileURL(paths[1]).href, undefined]);
  assert.equal(screenshots[2].restricted_reason, 'outside_workspace');
  assert.deepEqual((await runSwarmOperation('list', { scope: 'all' }, unrelated)).swarms, []);
  for (const op of ['overview', 'board']) {
    await assert.rejects(runSwarmOperation(op, { swarm_id }, unrelated), { status: 403 });
  }
});

test('swarm-bus validates worker names, parses mentions & screenshots, and manages board & deliveries', async (t) => {
  const dir = makeTmp(t);
  const sessionsDir = join(dir, 'sessions');

  assert.equal(validateWorkerName('builder-1'), 'builder-1');
  assert.equal(validateWorkerName('a_b2'), 'a_b2');
  for (const bad of ['', 'Builder', '-bad', 'all', 'coordinator', 'coordinator-1', 'human', 'a'.repeat(25)]) {
    assert.throws(() => validateWorkerName(bad));
  }

  assert.deepEqual(
    parseMentions('Hey @Builder-1 and @ALL, email foo@bar.com ignored, @builder-1 again, @qa_2!'),
    ['builder-1', 'all', 'qa_2'],
  );
  assert.deepEqual(
    parseScreenshots('Line\nScreenshot: /tmp/a.png | Main view\nscreenshot: C:\\x\\b.jpg | After fix\n'),
    [
      { path: '/tmp/a.png', caption: 'Main view' },
      { path: 'C:\\x\\b.jpg', caption: 'After fix' },
    ],
  );

  const swarmId = await resolveSwarmId({ hostConversationId: 'conv-42', cwd: dir, sessionsDir });
  assert.match(swarmId, /^conversation-[0-9a-f]{64}$/);
  await ensureSwarm({ swarmId: 'explicit', hostConversationId: 'conv-42', sessionsDir });
  assert.notEqual(await resolveSwarmId({ hostConversationId: 'other', cwd: dir, sessionsDir }), swarmId);
  await assert.rejects(ensureSwarm({ swarmId, hostConversationId: 'other', sessionsDir }), /Reserved/);

  const sw = await ensureSwarm({ swarmId, hostConversationId: 'conv-42', cwd: dir, title: 'Alpha Swarm', sessionsDir });
  assert.equal(sw.title, 'Alpha Swarm');
  assert.equal(await resolveSwarmId({ hostConversationId: 'conv-42', sessionsDir }), swarmId);

  const unbound = await resolveSwarmId({ cwd: dir, sessionsDir });
  assert.notEqual(unbound, swarmId);
  const renamed = await setSwarmTitle(swarmId, 'Beta Swarm', { sessionsDir });
  assert.equal(renamed.title, 'Beta Swarm');

  const w1 = await upsertWorker(swarmId, {
    name: 'worker-1', cli: 'codex', model: 'codex/gpt-5', tier: 'sonnet',
    access: 'change', workspace: 'share', source_cwd: dir, state: 'working', brief: 'Build API',
  }, { sessionsDir });
  assert.equal(w1.name, 'worker-1');
  assert.equal(w1.state, 'working');

  const planned = await setSwarmPlan(swarmId, {
    goal: { title: 'Ship Swarm', line: 'Phase 1 bus' },
    tasks: { 'worker-1': 'Own SQLite bus' },
    threads: [{ id: 't1', title: 'Core', goal: 'Bus', tasks: ['sqlite', 'bus'] }],
  }, { sessionsDir });
  assert.equal(planned.swarm.goal_title, 'Ship Swarm');
  assert.equal(planned.swarm.threads.length, 1);
  assert.equal((await getWorker(swarmId, 'worker-1', { sessionsDir })).task_summary, 'Own SQLite bus');

  const m1 = await postBoardMessage(swarmId, {
    from: 'coordinator', kind: 'brief', text: '@worker please implement feature\nScreenshot: /a.png | mockup',
  }, { sessionsDir });
  const m2 = await postBoardMessage(swarmId, {
    from: 'worker-1', kind: 'message', text: '@worker-1 self note ignored @coordinator progress',
  }, { sessionsDir });
  const m3 = await postBoardMessage(swarmId, {
    from: 'coordinator', kind: 'message', text: '@all sync up',
  }, { sessionsDir });

  assert.equal(await getBoardHead(swarmId, { sessionsDir }), m3.seq);
  assert.deepEqual(m1.screenshots, [{ path: '/a.png', caption: 'mockup' }]);
  assert.deepEqual((await readBoardMessages(swarmId, { since: m1.seq, sessionsDir })).map((m) => m.seq), [m2.seq, m3.seq]);
  assert.deepEqual((await readBoardMessages(swarmId, { seqs: [m3.seq, m1.seq], sessionsDir })).map((m) => m.seq), [m1.seq, m3.seq]);

  // Group prefix @worker and @all both match worker-1; self-post m2 excluded.
  const undelivered = await listUndeliveredForWorker(swarmId, 'worker-1', { sessionsDir });
  assert.deepEqual(undelivered.map((m) => m.seq), [m1.seq, m3.seq]);

  assert.equal(await markDelivered(swarmId, [m1.seq], 'worker-1', { sessionsDir }), 1);
  assert.equal(await markDelivered(swarmId, [m1.seq], 'worker-1', { sessionsDir }), 0);
  assert.deepEqual((await listUndeliveredForWorker(swarmId, 'worker-1', { sessionsDir })).map((m) => m.seq), [m3.seq]);
  assert.equal((await getWorker(swarmId, 'worker-1', { sessionsDir })).last_seq, m1.seq);

  await updateWorker(swarmId, 'worker-1', { state: 'waiting' }, { sessionsDir });
  await killWorkerRecord(swarmId, 'worker-1', { sessionsDir });
  assert.equal((await listRoster(swarmId, { includeKilled: false, sessionsDir })).length, 0);
  assert.equal((await listRoster(swarmId, { includeKilled: true, sessionsDir })).length, 1);
  assert.deepEqual(new Set((await listSwarms({ hostConversationId: 'conv-42', sessionsDir })).map((s) => s.id)), new Set([swarmId, 'explicit']));
  const overview = await getSwarmOverview(swarmId, { sessionsDir });
  assert.equal(overview.head, m3.seq);
  assert.equal(overview.recent_messages.length, 3);
});

test('reconcileRunState marks connecting-phase dead PID as failed instead of outcome_unknown', () => {
  const deadPid = 2_147_483_000;
  const connectingDead = {
    source: 'magpie_exec',
    run_id: 'a'.repeat(32),
    state: 'running',
    pid: deadPid,
    deadline: Date.now() / 1000 + 600,
    progress: { phase: 'connecting', thread_id: null, turn_id: null },
  };
  reconcileRunState(connectingDead);
  assert.equal(connectingDead.state, 'failed');
  assert.match(connectingDead.error, /exited before connecting/i);
  assert.equal(connectingDead.remote_turn_unconfirmed, undefined);

  const activeTurnDead = {
    source: 'magpie_exec',
    run_id: 'b'.repeat(32),
    state: 'running',
    pid: deadPid,
    turn_id: 'turn-99',
    codex_thread_id: 'thr-99',
    deadline: Date.now() / 1000 + 600,
    progress: { phase: 'running', thread_id: 'thr-99', turn_id: 'turn-99' },
  };
  reconcileRunState(activeTurnDead);
  assert.equal(activeTurnDead.state, 'outcome_unknown');
  assert.equal(activeTurnDead.remote_turn_unconfirmed, true);
});

test('resident server publishes 0o600 IPC token, protects /api/internal/dispatch, and delegates runs', async (t) => {
  const dir = makeTmp(t, 'gempt-ipc-');
  const configDir = join(dir, 'cfg');
  const configPath = join(configDir, 'config.json');
  const sessionsDir = join(dir, 'sessions');
  const workDir = join(dir, 'work');
  const magpieDir = join(dir, 'magpie');
  mkdirSync(configDir, { recursive: true });
  mkdirSync(workDir, { recursive: true });
  const appFixture = await startMockAppServer(configDir);
  t.after(() => appFixture.close());

  const env = {
    ...process.env,
    GEMPT_CONFIG: configPath,
    GEMPT_SESSIONS_DIR: sessionsDir,
    GEMPT_APP_SERVER_STATE: appFixture.statePath,
    MAGPIE_CONFIG_DIR: magpieDir,
  };
  const srv = await startServer({
    port: 0,
    configPath,
    sessionsDir,
    magpieConfigDir: magpieDir,
    manageAppServer: false,
    appServerOptions: { env },
  });
  t.after(() => srv.close());

  const ipcPath = join(configDir, `page-server-${srv.port}.ipc`);
  assert.ok(existsSync(ipcPath), 'IPC token file should be published beside identity file');
  const ipc = JSON.parse(readFileSync(ipcPath, 'utf8'));
  assert.equal(ipc.port, srv.port);
  assert.ok(typeof ipc.token === 'string' && ipc.token.length >= 16);

  // /api/ping remains public and must NOT expose the IPC token.
  const ping = await (await fetch(`http://127.0.0.1:${srv.port}/api/ping`)).json();
  assert.equal(ping.ok, true);
  assert.equal(ping.token, undefined);

  // Rejects missing / wrong IPC token.
  const unauth = await fetch(`http://127.0.0.1:${srv.port}/api/internal/dispatch`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-gempt-ipc-token': 'wrong' },
    body: JSON.stringify({ args: { goal: 'x' } }),
  });
  assert.equal(unauth.status, 401);

  // Returns 400 validation error when goal/cwd invalid via tryDelegateDispatch (without swallowing error).
  await assert.rejects(
    tryDelegateDispatch({ goal: 'test', cwd: workDir, access: 'inspect', workspace: 'share', background: true }, { configPath, sessionsDir, env }),
    /workspace='share' is not allowed/,
  );

  // Delegates background dispatch through the resident server.
  const delegated = await tryDelegateDispatch(
    { goal: 'Resident delegation smoke test', cwd: workDir, access: 'inspect', background: true },
    { configPath, sessionsDir, env },
  );
  assert.ok(delegated?.run_id);
  assert.equal(delegated.state, 'queued');

  await srv.close();
  assert.equal(existsSync(ipcPath), false, 'IPC token file should be cleaned up on server close');
});

test('swarm-engine orchestrates add/sharding/steer/warm-wake/reply/model/kill and turn finish hooks', async (t) => {
  const dir = makeTmp(t);
  const sessionsDir = join(dir, 'sessions');
  const configPath = join(dir, 'config.json');

  const dispatched = [];
  const steered = [];
  const interrupted = [];
  const replied = [];
  let seqId = 0;

  const mocks = {
    sessionsDir,
    configPath,
    dispatchRunImpl: async (args) => {
      seqId += 1;
      const run = {
        run_id: String(seqId).padStart(32, '0'),
        session_id: args.session_id || `exec-swarm-${seqId}`,
        state: 'queued',
        cwd: args.cwd,
        worktree_path: args.workspace === 'share' ? join(dir, `wt-${seqId}`) : null,
        source_cwd: args.cwd,
        model: args.model || 'codex/gpt-5',
      };
      dispatched.push({ args, run });
      return run;
    },
    steerRunImpl: async (runId, msg) => {
      steered.push({ runId, msg });
      return { ok: true, run_id: runId };
    },
    interruptRunImpl: async (runId) => {
      interrupted.push(runId);
      return { ok: true, run_id: runId };
    },
    replyToolCallImpl: async (runId, callId, answer) => {
      replied.push({ runId, callId, answer });
      return { ok: true, run_id: runId, call_id: callId };
    },
    readRunImpl: (runId) => ({
      run_id: runId,
      state: 'running',
    }),
  };

  const titled = await handleSwarmAction({ action: 'title', swarm_id: 'sw-demo', cwd: dir, title: 'Phase 1 Swarm' }, mocks);
  assert.equal(titled.title, 'Phase 1 Swarm');

  // Sharded add creates builder-1 and builder-2.
  const added = await handleSwarmAction({
    action: 'add',
    swarm_id: 'sw-demo',
    cwd: dir,
    name: 'builder',
    brief: 'Implement module',
    tasks: ['core/a.mjs', 'core/b.mjs'],
    workspace: 'share',
    model: 'codex/gpt-5',
  }, mocks);
  assert.equal(added.workers.length, 2);
  assert.deepEqual(added.workers.map((w) => w.name), ['builder-1', 'builder-2']);
  assert.match(added.wait_command, /--board sw-demo/);
  assert.equal(dispatched.length, 2);
  assert.equal(dispatched[0].args.swarm_agent, 'builder-1');
  const prefixes = dispatched.map(({ args }) => args.goal.split('Worker identity:')[0]);
  assert.equal(prefixes[0], prefixes[1]);
  assert.match(prefixes[0], /## Assignment\nImplement module/);
  assert.ok(!prefixes[0].includes('Assigned slice'));
  for (const worker of added.workers) {
    assert.equal(worker.last_seq, added.head);
    assert.equal(worker.last_read_seq, added.head);
  }

  // Posting @builder-1 while working steers its active run.
  const steerRes = await handleBoardAction({
    action: 'post',
    swarm_id: 'sw-demo',
    text: '@builder-1 focus on edge cases',
  }, mocks);
  assert.equal(steerRes.deliveries.length, 1);
  assert.equal(steerRes.deliveries[0].mode, 'steer');
  assert.equal(steered.length, 1);
  assert.match(steered[0].msg, /focus on edge cases/);

  // Turn finish auto-posts @coordinator [done] summary and transitions to waiting.
  const finishRes = await onWorkerTurnFinished({
    swarmId: 'sw-demo',
    swarmAgent: 'builder-1',
    runId: added.workers[0].run_id,
    sessionId: added.workers[0].session_id,
    startedAt: new Date().toISOString(),
    outcome: { state: 'finished' },
    result: { text: '[mode=exec | tier=paired]\nImplemented core/a.mjs and passed tests.', session_id: added.workers[0].session_id },
    ...mocks,
  });
  assert.equal(finishRes.state, 'waiting');
  assert.ok(finishRes.auto_post);
  assert.match(finishRes.auto_post.text, /@coordinator \[done\] Implemented core\/a\.mjs/);

  // Posting @builder-1 while waiting warm-wakes reusing session_id and worktree_path!
  const wakeRes = await handleBoardAction({
    action: 'post',
    swarm_id: 'sw-demo',
    text: '@builder-1 add one more test',
  }, mocks);
  assert.equal(wakeRes.deliveries[0].mode, 'wake');
  const wakeCall = dispatched[dispatched.length - 1];
  assert.equal(wakeCall.args.session_id, added.workers[0].session_id);
  assert.equal(wakeCall.args.cwd, added.workers[0].worktree_path);

  // Worker waiting_reply answered via coordinator board post -> replyToolCallImpl.
  await updateWorker('sw-demo', 'builder-2', { state: 'waiting_reply', parked_on: 'call_42' }, { sessionsDir });
  const replyRes = await handleBoardAction({
    action: 'post',
    swarm_id: 'sw-demo',
    from: 'coordinator',
    text: '@builder-2 use WAL mode',
  }, mocks);
  assert.equal(replyRes.deliveries[0].mode, 'reply');
  assert.deepEqual(replied[0], { runId: added.workers[1].run_id, callId: 'call_42', answer: '@builder-2 use WAL mode' });
  assert.equal((await getWorker('sw-demo', 'builder-2', { sessionsDir })).state, 'working');

  // Model switch on waiting worker resumes session with new model.
  await updateWorker('sw-demo', 'builder-1', { state: 'waiting' }, { sessionsDir });
  const switched = await handleSwarmAction({
    action: 'model',
    swarm_id: 'sw-demo',
    name: 'builder-1',
    model: 'codex/gpt-5.2',
    reason: 'quota',
  }, mocks);
  assert.equal(switched.worker.model, 'codex/gpt-5.2');
  assert.equal(switched.worker.state, 'working');
  assert.equal(dispatched[dispatched.length - 1].args.model, 'codex/gpt-5.2');

  // Kill interrupts active worker and marks killed.
  const killed = await handleSwarmAction({ action: 'kill', swarm_id: 'sw-demo', name: 'builder-2' }, mocks);
  assert.equal(killed.worker.state, 'killed');
  assert.equal(killed.interrupted, true);
  assert.ok(interrupted.includes(added.workers[1].run_id));

  // Wait action returns valid gempt-wait --board command.
  const waitInfo = await handleBoardAction({ action: 'wait', swarm_id: 'sw-demo', since: 2, mentions_only: false }, mocks);
  assert.match(waitInfo.wait_command, /--board sw-demo --since 2 --all-messages/);
});

test('board wait repairs a dead CLI worker and clears its parked call in SQLite', async (t) => {
  const sessionsDir = makeTmp(t), opts = { sessionsDir }, swarmId = 'dead-cli';
  await ensureSwarm({ swarmId, cwd: sessionsDir, sessionsDir });
  for (const [index, cli] of ['claude', 'gemini', 'opencode', 'custom-cli'].entries()) {
    const run_id = String(index).repeat(32);
    const file = join(sessionsDir, 'runs', run_id, 'state.json');
    saveRun(file, { run_id, state: 'running', cli, source: 'magpie_exec', pid: 2147483000, turn_id: 'synthetic' });
    await upsertWorker(swarmId, { name: cli, run_id, state: 'working', parked_on: 'call' }, opts);
    const { roster } = await waitForBoard(swarmId, { ...opts, timeoutMs: 0 });
    const worker = roster.find((worker) => worker.name === cli);
    assert.equal(worker.state, 'stopped');
    assert.equal((await getWorker(swarmId, cli, opts)).parked_on, null);
    const run = JSON.parse(readFileSync(file));
    assert.equal(run.state, 'failed');
    assert.match(run.error, new RegExp(cli));
    assert.doesNotMatch(run.error, /remote codex/i);
    assert.equal(run.remote_turn_unconfirmed, undefined);
  }
});

test('waitForBoard and gempt-wait --board wake on coordinator mentions and worker questions', async (t) => {
  const dir = makeTmp(t);
  const sessionsDir = join(dir, 'sessions');
  await ensureSwarm({ swarmId: 'sw-wait', cwd: dir, sessionsDir });
  await upsertWorker('sw-wait', { name: 'alpha', state: 'working' }, { sessionsDir });

  // Peer chatter does not wake coordinator wait, then @coordinator does.
  const waiter = waitForBoard('sw-wait', { since: 0, sessionsDir, pollIntervalMs: 30, timeoutMs: 3000 });
  setTimeout(async () => {
    await postBoardMessage('sw-wait', { from: 'alpha', text: '@beta internal sync' }, { sessionsDir });
  }, 40);
  setTimeout(async () => {
    await postBoardMessage('sw-wait', { from: 'alpha', text: '@coordinator [done] finished' }, { sessionsDir });
  }, 100);
  const res = await waiter;
  assert.equal(res.swarm_id, 'sw-wait');
  assert.ok(res.messages.some((m) => m.text.includes('[done]')));

  // CLI runWait --board exits 0 with JSON.
  const prevLog = console.log;
  let captured = '';
  console.log = (line) => { captured += `${line}\n`; };
  let code;
  try {
    code = await runWait(['--board', 'sw-wait', '--since', '0'], { sessionsDir });
  } finally {
    console.log = prevLog;
  }
  assert.equal(code, 0);
  const parsed = JSON.parse(captured.trim());
  assert.equal(parsed.swarm_id, 'sw-wait');
  assert.ok(parsed.head >= 2);
  const text = 'x'.repeat(1999) + '😀' + 'y'.repeat(3000) + ' @coordinator';
  const post = await postBoardMessage('sw-wait', { from: 'alpha', text }, { sessionsDir });
  const snapshot = await waitForBoard('sw-wait', { sessionsDir, since: parsed.head, timeoutMs: 1000 });
  assert.equal(snapshot.messages.length, 1);
  const [preview] = snapshot.messages;
  assert.equal(preview.text, text);
  assert.equal(preview.seq, post.seq);
  const read = async (args) => JSON.parse((await handleTool('gpt_board',
    { swarm_id: 'sw-wait', action: 'read', ...args }, {}, { sessionsDir })).content[0].text).messages;
  assert.deepEqual((await read({ since: parsed.head }))[0], preview);
  assert.equal((await read({ seqs: [post.seq] }))[0].text, text);
});

test('board-mcp stdio server exposes board_post, board_read, board_ask, and board_roster', async (t) => {
  const dir = makeTmp(t);
  const sessionsDir = join(dir, 'sessions');
  await ensureSwarm({ swarmId: 'sw-mcp', cwd: dir, sessionsDir });
  await upsertWorker('sw-mcp', { name: 'helper', state: 'working' }, { sessionsDir });

  const env = {
    ...process.env,
    GEMPT_SWARM_ID: 'sw-mcp',
    GEMPT_AGENT_NAME: 'helper',
    GEMPT_SESSIONS_DIR: sessionsDir,
  };
  const child = spawn(process.execPath, [join(REPO_ROOT, 'core', 'board-mcp.mjs')], {
    env,
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  t.after(() => child.kill());

  const lines = createInterface({ input: child.stdout })[Symbol.asyncIterator]();
  let id = 0;
  const req = async (method, params) => {
    child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', id: ++id, method, params })}\n`);
    const next = await lines.next();
    return JSON.parse(next.value);
  };

  const init = await req('initialize', { protocolVersion: '2024-11-05' });
  assert.equal(init.result.serverInfo.name, 'gempt-board');
  const list = await req('tools/list');
  assert.deepEqual(list.result.tools.map((x) => x.name), ['board_post', 'board_read', 'board_ask', 'board_roster', 'board_wait', 'board_publish_view', 'board_swarm']);

  const posted = await req('tools/call', { name: 'board_post', arguments: { text: '@coordinator hello from helper' } });
  const postData = JSON.parse(posted.result.content[0].text);
  assert.equal(postData.ok, true);
  assert.equal(postData.message.from_agent, 'helper');

  const invalid = await req('tools/call', { name: 'board_ask', arguments: '{' });
  assert.deepEqual(invalid.error, { code: -32603, message: 'Tool arguments must be a JSON object.' });
  const asked = await req('tools/call', {
    name: 'board_ask',
    arguments: JSON.stringify({ question: 'Approve layout?', reason: 'visual_check', media_paths: ['/tmp/ui.png'] }),
  });
  const askData = JSON.parse(asked.result.content[0].text);
  assert.equal(askData.message.kind, 'question');
  assert.deepEqual(askData.message.screenshots, [{ path: '/tmp/ui.png', caption: 'attachment' }]);
  assert.equal((await getWorker('sw-mcp', 'helper', { sessionsDir })).state, 'waiting_reply');

  const read = await req('tools/call', { name: 'board_read', arguments: { since: 0 } });
  assert.equal(JSON.parse(read.result.content[0].text).messages.length, 2);

  const roster = await req('tools/call', { name: 'board_roster', arguments: {} });
  assert.deepEqual(JSON.parse(roster.result.content[0].text).roster.map(({ name }) => name), ['coordinator', 'helper']);

  child.stdin.end();
});

test('MCP tools, Magpie plugin tools, and CLI subcommands expose gpt_swarm / gpt_board cleanly', async (t) => {
  const dir = makeTmp(t);
  const sessionsDir = join(dir, 'sessions');
  const configPath = join(dir, 'config.json');
  const env = { ...process.env, GEMPT_SESSIONS_DIR: sessionsDir, GEMPT_CONFIG: configPath };

  assert.deepEqual(TOOLS.map((x) => x.name), ['gpt_run', 'gpt_ask', 'gpt_ctl', 'gpt_swarm', 'gpt_board']);
  const magpieTools = createGemptTools({ configPath, sessionsDir, env, options: { exec: false } });
  assert.deepEqual(Object.keys(magpieTools).sort(), ['ask', 'board', 'ctl', 'run', 'swarm']);

  // Read-only Magpie tools reject writable swarm add when exec: false.
  await assert.rejects(
    magpieTools.swarm.execute({ action: 'add', name: 'w1', brief: 'edit', cwd: dir }),
    /disabled/,
  );

  const swTitle = JSON.parse((await handleTool('gpt_swarm', {
    action: 'title', swarm_id: 'sw-cli', cwd: dir, title: 'CLI Swarm',
  }, env)).content[0].text);
  assert.equal(swTitle.title, 'CLI Swarm');

  const cliPost = JSON.parse(execFileSync(process.execPath, [
    join(REPO_ROOT, 'bin', 'gempt.mjs'), 'board', 'post', '--swarm', 'sw-cli', '@coordinator from cli',
  ], { env, encoding: 'utf8' }));
  assert.equal(cliPost.ok, true);
  assert.equal(cliPost.head, 1);

  const cliStatus = JSON.parse(execFileSync(process.execPath, [
    join(REPO_ROOT, 'bin', 'gempt.mjs'), 'swarm', 'status', '--swarm', 'sw-cli',
  ], { env, encoding: 'utf8' }));
  assert.equal(cliStatus.title, 'CLI Swarm');
  assert.equal(cliStatus.head, 1);
});

function boardCli(t) {
  const sessionsDir = makeTmp(t);
  const configPath = join(sessionsDir, 'config.json');
  const run = (args, { agent, input, error } = {}) => {
    const env = { ...process.env, GEMPT_CONFIG: configPath, GEMPT_SESSIONS_DIR: sessionsDir,
      GEMPT_SWARM_ID: agent ? 'sw-cli' : '', GEMPT_AGENT_NAME: agent || '', GEMPT_RUN_ID: agent ? 'cli-run' : '' };
    const result = spawnSync(process.execPath, [join(REPO_ROOT, 'bin/gempt.mjs'), 'board', ...args], {
      cwd: sessionsDir, env, input, encoding: 'utf8', timeout: 10000,
    });
    assert.equal(result.status, error ? 1 : 0, result.error?.message || result.stderr);
    if (error) {
      assert.equal(result.stdout, '');
      assert.match(result.stderr, error);
    } else {
      return args.includes('--help') ? result.stdout : JSON.parse(result.stdout);
    }
  };
  return { sessionsDir, run };
}

test('board CLI exposes seven tools and preserves manual sender, host resolution and stdin', async (t) => {
  const { sessionsDir, run } = boardCli(t);
  const help = run(['--help']);
  assert.deepEqual([...help.matchAll(/gempt board ([\w-]+)/g)].map((m) => m[1]).sort(),
    BOARD_TOOLS.map(({ name }) => name.slice(6).replace('_', '-')).sort());
  writeFileSync(join(sessionsDir, 'host-context.json'), JSON.stringify({ conversation_id: 'host-cli' }));
  const swarmId = await resolveSwarmId({ hostConversationId: 'host-cli', sessionsDir });
  const post = ['post', '--from', 'human', '--request-id', 'submit-1'];
  const first = run(post, { input: 'From stdin' });
  assert.equal(first.swarm_id, swarmId);
  assert.equal(first.message.from_agent, 'human');
  assert.equal(first.message.request_id, 'submit-1');
  assert.deepEqual(run([...post, '--text', '-'], { input: 'Retry' }).message, first.message);
  const reply = run(['post', '-', '--swarm', swarmId, '--reply-to', String(first.head)], { input: 'Reply' });
  assert.equal(reply.message.reply_to_seq, first.head);
  assert.equal(reply.message.from_agent, 'coordinator');
  assert.deepEqual(run(['read', '--since', '0', '--limit', '1']).messages, [first.message]);
  assert.deepEqual(run(['read', '--seqs', String(reply.head)]).messages, [reply.message]);
  assert.deepEqual(run(['read', '--status']), run(['roster', '--from', 'human']));
  assert.equal(run(['swarm', 'status']).swarm_id, swarmId);
  assert.match(run(['wait', '--since', String(reply.head)]).wait_command, /--since 2/);
  writeFileSync(join(sessionsDir, 'host-context.json'), JSON.stringify({ conversation_id: 'next-host' }));
  const asked = run(['ask', 'Which scope?', '--from', 'reviewer']);
  assert.equal(asked.swarm_id, await resolveSwarmId({ hostConversationId: 'next-host', sessionsDir }));
  assert.equal(asked.message.from_agent, 'reviewer');
});

test('board CLI binds worker identity, parks at the read cursor and asks with attachments', async (t) => {
  const { sessionsDir, run } = boardCli(t);
  const agent = 'helper';
  const opts = { sessionsDir };
  await upsertWorker('sw-cli', { name: agent, state: 'working' }, opts);
  const post = run(['post', 'progress', '--from', agent], { agent });
  assert.equal(post.message.from_agent, agent);
  assert.equal(post.message.meta.run_id, 'cli-run');
  run(['post', 'wrong sender', '--from', 'coordinator'], { agent, error: /--from/ });
  run(['read', '--swarm', 'other'], { agent, error: /--swarm/ });
  assert.deepEqual(run(['read', '--status'], { agent }), run(['roster'], { agent }));
  const read = run(['read'], { agent });
  const since = String(read.messages.at(-1).seq);
  run(['wait'], { agent, error: /requires --since/ });
  run(['wait', '--since', '99'], { agent, error: /actually read/ });
  assert.equal(run(['wait', '--since', since], { agent }).parked, true);
  const note = await postBoardMessage('sw-cli', { from: 'coordinator', text: 'Visible update' }, opts);
  const waiting = run(['wait', '--since', since, '--no-mentions-only', '--channel', 'main'], { agent });
  assert.equal(waiting.mentions_only, false);
  assert.equal(waiting.parked, false);
  assert.deepEqual(waiting.messages.map((m) => m.seq), [note.seq]);
  const media = ['/tmp/one.png', '/tmp/two views.png', '/tmp/three.png'];
  const ask = run(['ask', '-', '--reason', 'decision', '--context', 'Shipping tomorrow',
    '--media', media[0], media[1], '--media', media[2]], { agent, input: 'Which layout?' });
  assert.equal(ask.message.from_agent, agent);
  assert.equal(ask.message.kind, 'question');
  assert.match(ask.message.text, /^@coordinator Which layout\?\n\nContext: Shipping tomorrow/);
  assert.deepEqual(ask.message.meta, { reason: 'decision', run_id: 'cli-run' });
  assert.deepEqual(ask.message.screenshots.map((s) => s.path), media);
  assert.equal((await getWorker('sw-cli', agent, opts)).parked_on, `board_ask:${ask.message.seq}`);
  run(['ask', 'question', '--reason', 'typo'], { agent, error: /Invalid --reason/ });
});

test('board CLI preserves helper ownership and narrator restrictions for swarm and publish-view', async (t) => {
  const { sessionsDir, run } = boardCli(t);
  const opts = { sessionsDir };
  for (const worker of [
    { name: 'owner' }, { name: 'peer' }, { name: 'child', added_by: 'owner' }, { name: 'observer', role: 'narrator' },
  ]) await upsertWorker('sw-cli', { state: 'stopped', ...worker }, opts);
  const peer = { agent: 'peer' };
  const owner = { agent: 'owner' };
  const narrator = { agent: 'observer' };
  const target = ['--args-json', '{"name":"child","actor":"coordinator","added_by":"peer","swarm_id":"other"}'];
  run(['swarm', 'kill', ...target], { ...peer, error: /Cannot kill/ });
  run(['swarm', 'model', ...target], { ...peer, error: /Cannot switch/ });
  run(['swarm', 'add', '--args-json', '{"name":"child2","brief":"review","fork":"missing"}'], { ...peer, error: /unknown member/ });
  assert.equal(run(['swarm', 'model', '--args-json', '{"name":"child","model":"new"}'], owner).worker.desired_model, 'new');
  assert.equal(run(['swarm', 'kill', ...target], owner).worker.state, 'killed');
  const channels = run(['swarm', 'channel', '--args-json', '-'], { ...owner, input: '{"op":"create","channel":"review","action":"kill"}' });
  assert.ok(channels.channels.some((c) => c.name === 'review' && c.created_by === 'owner'));
  run(['swarm', 'title'], { ...owner, error: /Unsupported worker swarm action/ });
  run(['swarm', 'status', '--args-json', '[]'], { ...owner, error: /JSON object/ });
  assert.ok(run(['swarm', 'models'], narrator).models.length);
  assert.equal(run(['swarm', 'status'], narrator).swarm_id, 'sw-cli');
  for (const args of [['post', 'write'], ['ask', 'write?'], ['swarm', 'channel', '--args-json', '{"op":"create","channel":"other"}']]) {
    run(args, { ...narrator, error: /read-only/ });
  }
  run(['post', 'write', '--from', 'owner'], { ...narrator, error: /--from/ });
  const payload = JSON.stringify({ view: { headline: 'Review' }, html: '<p>Review</p>' });
  run(['publish-view', '--args-json', payload], { ...owner, error: /Only a live narrator/ });
  run(['publish-view', '--from', 'observer', '--args-json', payload], { ...owner, error: /--from/ });
  for (const args of [['--args-json', payload], ['--args-json', '-'], ['-'], []]) {
    const published = run(['publish-view', ...args], { ...narrator, input: payload });
    assert.equal(published.view.json.headline, 'Review');
  }
  assert.equal(readNarratorView(sessionsDir, 'sw-cli').html, '<p>Review</p>');
  await killWorkerRecord('sw-cli', 'observer', opts);
  run(['publish-view', '--args-json', payload], { ...narrator, error: /Only a live narrator/ });
});

test('runAppServer registers board_post/board_read/board_roster dynamic tools and bridges ask_gemini to board', async (t) => {
  const dir = makeTmp(t);
  const sessionsDir = join(dir, 'sessions');
  const runId = 'c'.repeat(32);
  const runDir = join(sessionsDir, 'runs', runId);
  const runFile = join(runDir, 'state.json');
  await ensureSwarm({ swarmId: 'sw-native', cwd: dir, sessionsDir });
  await upsertWorker('sw-native', { name: 'codex-a', run_id: runId, state: 'working' }, { sessionsDir });

  let sawDynamicTools = [];
  const fixture = await startMockAppServer(dir, {
    onThreadStart(params) {
      sawDynamicTools = (params.dynamicTools || []).map((tool) => tool.name);
    },
    async onTurnStart(params, ctx) {
      const postReply = await ctx.callTool('board_post', { text: '@coordinator live progress update' });
      assert.equal(postReply.success, true);
      const readReply = await ctx.callTool('board_read', { since: 0 });
      assert.equal(readReply.success, true);
      const rosterReply = await ctx.callTool('board_roster', {});
      assert.equal(rosterReply.success, true);
      const statusReply = await ctx.callTool('board_swarm', { action: 'status' });
      assert.deepEqual(JSON.parse(statusReply.contentItems[0].text).roster.map(({ name }) => name), ['coordinator', 'codex-a']);
      const waitReply = await ctx.callTool('board_wait', { since: JSON.parse(readReply.contentItems[0].text).head });
      assert.equal(JSON.parse(waitReply.contentItems[0].text).parked, true);
      // Also verify ask_gemini posts a question to the board and parks the worker.
      const askPromise = ctx.callTool('ask_gemini', { reason: 'clarification', question: 'Ship Phase 1?' }, 'call_ask_1');
      let w;
      for (let i = 0; i < 40; i++) {
        w = await getWorker('sw-native', 'codex-a', { sessionsDir });
        if (w?.state === 'waiting_reply') break;
        await new Promise((r) => setTimeout(r, 25));
      }
      assert.equal(w.state, 'waiting_reply');
      assert.equal(w.parked_on, 'call_ask_1');
      const replyRes = await handleBoardAction({
        action: 'post',
        swarm_id: 'sw-native',
        from: 'coordinator',
        text: '@codex-a Yes, ship it!',
      }, { sessionsDir });
      assert.equal(replyRes.deliveries[0].mode, 'reply');
      const askReply = await askPromise;
      assert.equal(askReply.success, true);
      assert.match(askReply.contentItems[0].text, /Yes, ship it/);
      return { text: 'All native board tools succeeded.' };
    },
  });
  t.after(() => fixture.close());

  saveRun(runFile, {
    source: 'magpie_exec',
    run_id: runId,
    session_id: 'exec-native-1',
    cwd: dir,
    state: 'running',
    swarm_id: 'sw-native',
    swarm_agent: 'codex-a',
  });

  const env = { ...process.env, GEMPT_APP_SERVER_STATE: fixture.statePath };
  const res = await runAppServer({
    configPath: join(dir, 'config.json'),
    env,
    runFile,
    sessionId: 'exec-native-1',
    onIdentity() {},
    prompt: 'Test native swarm tools',
    cwd: dir,
    model: 'codex/gpt-5',
    effort: 'medium',
    sandbox: 'workspace-write',
    approvalPolicy: 'never',
    gatewayUrl: fixture.gatewayUrl,
    timeoutSec: 30,
    sessionsDir,
    swarmId: 'sw-native',
    swarmAgent: 'codex-a',
  });

  assert.deepEqual(sawDynamicTools, ['ask_gemini', 'board_post', 'board_read', 'board_roster', 'board_wait', 'board_publish_view', 'board_swarm']);
  assert.match(res.text, /All native board tools succeeded/);
  const msgs = await readBoardMessages('sw-native', { since: 0, sessionsDir });
  assert.equal(msgs.length, 3);
  assert.equal(msgs[0].from_agent, 'codex-a');
  assert.match(msgs[0].text, /live progress update/);
  assert.equal(msgs[1].kind, 'question');
  assert.match(msgs[1].text, /Ship Phase 1\?/);
  assert.equal(msgs[2].from_agent, 'coordinator');
  assert.match(msgs[2].text, /Yes, ship it/);
});

test('Swarm HTTP API serves list/overview/board/post/action and enforces key + opaqueOrigin policy', async (t) => {
  const dir = makeTmp(t, 'gempt-swarm-http-');
  const sessionsDir = join(dir, 'sessions');
  const configPath = join(dir, 'config.json');
  const magpieDir = join(dir, 'magpie');
  const workDir = join(dir, 'repo');
  mkdirSync(workDir, { recursive: true });
  writeFileSync(join(workDir, 'shot.png'), 'png');

  const srv = await startServer({
    port: 0,
    configPath,
    sessionsDir,
    magpieConfigDir: magpieDir,
    manageAppServer: false,
  });
  t.after(() => srv.close());

  const base = `http://127.0.0.1:${srv.port}`;
  const key = await getPageKey(base);
  assert.ok(key, 'index HTML should inject gempt-key');
  const auth = { 'x-gempt-key': key };

  // Unauthenticated request rejected with 401.
  assert.equal((await fetch(`${base}/api/swarm/list`)).status, 401);
  assert.equal((await fetch(`${base}/api/swarm`)).status, 401);

  // Empty list returns cleanly WITHOUT creating swarm.sqlite on disk.
  const emptyList = await (await fetch(`${base}/api/swarm/list`, { headers: auth })).json();
  assert.equal(emptyList.ok, true);
  assert.deepEqual(emptyList.swarms, []);
  assert.equal(emptyList.active_workers_total, 0);
  assert.equal(existsSync(resolveSwarmDbPath(sessionsDir)), false);

  // Seed a Swarm with threads, workers, screenshot, and waiting_reply run.
  const runId = 'd'.repeat(32);
  const runDir = join(sessionsDir, 'runs', runId);
  saveRun(join(runDir, 'state.json'), {
    source: 'magpie_exec',
    run_id: runId,
    session_id: 'exec-swarm-http-1',
    cwd: workDir,
    state: 'waiting_gemini',
    pending_tool_call: { call_id: 'call_ui_1', tool: 'ask_gemini', arguments: { question: 'Approve colors?' } },
  });

  await ensureSwarm({ swarmId: 'sw-http', hostConversationId: 'conv-ui', cwd: workDir, title: 'UI Swarm', sessionsDir });
  await upsertWorker('sw-http', {
    name: 'frontend', cli: 'codex', model: 'codex/gpt-5', workspace: 'share',
    worktree_path: join(workDir, 'wt-fe'), source_cwd: workDir, state: 'waiting_reply',
    run_id: runId, session_id: 'exec-swarm-http-1', parked_on: 'call_ui_1', task_summary: 'Build Swarm tab',
  }, { sessionsDir });
  await upsertWorker('sw-http', {
    name: 'tester', cli: 'codex', model: 'codex/gpt-5', workspace: 'inherit',
    source_cwd: workDir, state: 'waiting', task_summary: 'Run browser suite',
  }, { sessionsDir });
  await setSwarmPlan('sw-http', {
    goal: { title: 'Phase 2 UI', line: 'Ship Swarm Workbench in Sidecar & Magpie' },
    threads: [{ id: 'ui', title: 'Workbench', goal: 'Overview + Board', workers: ['frontend'] }],
  }, { sessionsDir });
  const m1 = await postBoardMessage('sw-http', {
    from: 'frontend',
    kind: 'question',
    text: '@coordinator Approve dark mode contrast?\nScreenshot: shot.png | Dark mode preview',
  }, { sessionsDir });

  const registered = join(workDir, 'shot.png');
  const png = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
  writeFileSync(registered, png);
  const frameDb = await openSwarmDb(sessionsDir);
  try {
    const framePost = await postBoardMessage('sw-http', { from: 'tester', text: `Screenshot: ${registered} | Binary frame` }, { sessionsDir });
    const native = await fetch(`${base}/api/swarm?swarm_id=sw-http&tail=1&omitRaw=1`, { headers: auth }).then((response) => response.json());
    assert.equal(native.swarm_id, 'sw-http');
    assert.equal(native.frames[0].path, registered);
    const frame = await fetch(`${base}/api/frame?` + new URLSearchParams({ swarm_id: 'sw-http', path: registered }), { headers: auth });
    assert.equal(frame.headers.get('content-type'), 'image/png');
    assert.equal(frame.headers.get('cache-control'), 'no-store');
    assert.deepEqual(Buffer.from(await frame.arrayBuffer()), png);
    frameDb.run('DELETE FROM messages WHERE seq = ?', framePost.seq);
  } finally { frameDb.close(); }

  // Opaque Magpie iframe (Origin: null) can read GET endpoints.
  const opaqueListRes = await fetch(`${base}/api/swarm/list`, { headers: { ...auth, Origin: 'null' } });
  assert.equal(opaqueListRes.status, 200);
  const listData = await opaqueListRes.json();
  assert.equal(listData.swarms.length, 1);
  assert.equal(listData.swarms[0].id, 'sw-http');
  assert.equal(listData.swarms[0].counts.visible, 2);
  assert.equal(listData.swarms[0].counts.waiting_reply, 1);
  assert.equal(listData.swarms[0].counts.waiting, 1);
  assert.equal(listData.swarms[0].head, m1.seq);
  assert.equal(listData.active_workers_total, 1);

  const ovRes = await fetch(`${base}/api/swarm/overview?swarm_id=sw-http`, { headers: { ...auth, Origin: 'null' } });
  assert.equal(ovRes.status, 200);
  const ov = await ovRes.json();
  assert.equal(ov.swarm.title, 'UI Swarm');
  assert.equal(ov.roster.length, 2);
  assert.equal(ov.screenshots.length, 1);
  assert.equal(ov.screenshots[0].caption, 'Dark mode preview');
  assert.ok(ov.screenshots[0].file_uri?.startsWith('file:///'));
  assert.equal(ov.counts.active, 1);

  const boardRes = await fetch(`${base}/api/swarm/board?swarm_id=sw-http&since=0&limit=50`, { headers: { ...auth, Origin: 'null' } });
  assert.equal(boardRes.status, 200);
  const board = await boardRes.json();
  assert.equal(board.messages.length, 1);
  assert.equal(board.messages[0].kind, 'question');

  // Opaque origin blocks POST mutations with 403 invalid origin; foreign origin also 403.
  const blockedPost = await fetch(`${base}/api/swarm/post`, {
    method: 'POST',
    headers: { ...auth, Origin: 'null', 'Content-Type': 'application/json' },
    body: JSON.stringify({ swarm_id: 'sw-http', text: '@frontend Approved!' }),
  });
  assert.equal(blockedPost.status, 403);
  const blockedAction = await fetch(`${base}/api/swarm/action`, {
    method: 'POST',
    headers: { ...auth, Origin: 'null', 'Content-Type': 'application/json' },
    body: JSON.stringify({ swarm_id: 'sw-http', action: 'title', title: 'Hacked' }),
  });
  assert.equal(blockedAction.status, 403);
  const foreignPost = await fetch(`${base}/api/swarm/post`, {
    method: 'POST',
    headers: { ...auth, Origin: 'https://evil.test', 'Content-Type': 'application/json' },
    body: JSON.stringify({ swarm_id: 'sw-http', text: 'nope' }),
  });
  assert.equal(foreignPost.status, 403);

  // Same-origin POST /api/swarm/post posts as human and immediately replies to waiting_reply worker!
  const humanPost = await fetch(`${base}/api/swarm/post`, {
    method: 'POST',
    headers: { ...auth, Origin: base, 'Content-Type': 'application/json' },
    body: JSON.stringify({ swarm_id: 'sw-http', text: '@frontend Looks crisp, proceed!', reply_to_seq: m1.seq }),
  });
  assert.equal(humanPost.status, 200);
  const posted = await humanPost.json();
  assert.equal(posted.ok, true);
  assert.equal(posted.message.from_agent, 'human');
  assert.equal(posted.message.reply_to_seq, m1.seq);
  assert.equal(posted.deliveries.length, 1);
  assert.equal(posted.deliveries[0].mode, 'reply');
  assert.equal(posted.deliveries[0].worker, 'frontend');
  const replyFile = JSON.parse(readFileSync(join(runDir, 'tool_replies', 'call_ui_1.json'), 'utf8'));
  assert.match(replyFile.answer, /Looks crisp, proceed/);

  // Incremental board read with since returns only the new human message.
  const incBoard = await (await fetch(`${base}/api/swarm/board?swarm_id=sw-http&since=${m1.seq}`, { headers: auth })).json();
  assert.equal(incBoard.messages.length, 1);
  assert.equal(incBoard.messages[0].from_agent, 'human');

  // Same-origin POST /api/swarm/action supports title and kill; invalid swarm returns 404.
  const renameRes = await (await fetch(`${base}/api/swarm/action`, {
    method: 'POST',
    headers: { ...auth, Origin: base, 'Content-Type': 'application/json' },
    body: JSON.stringify({ swarm_id: 'sw-http', action: 'title', title: 'UI Swarm v2' }),
  })).json();
  assert.equal(renameRes.title, 'UI Swarm v2');

  const killRes = await (await fetch(`${base}/api/swarm/action`, {
    method: 'POST',
    headers: { ...auth, Origin: base, 'Content-Type': 'application/json' },
    body: JSON.stringify({ swarm_id: 'sw-http', action: 'kill', name: 'tester' }),
  })).json();
  assert.equal(killRes.worker.state, 'killed');

  assert.equal((await fetch(`${base}/api/swarm/overview?swarm_id=missing-swarm`, { headers: auth })).status, 404);
  assert.equal((await fetch(`${base}/api/swarm/overview?swarm_id=../bad`, { headers: auth })).status, 400);
});

test('Sidecar Swarm routes enforce workspace/conversation scope and route human steer + interrupt actions', async (t) => {
  const dir = makeTmp(t, 'gempt-swarm-sidecar-');
  const sessionsDir = join(dir, 'sessions');
  const wsA = join(dir, 'ws-a');
  const wsB = join(dir, 'ws-b');
  mkdirSync(wsA, { recursive: true });
  mkdirSync(wsB, { recursive: true });

  await ensureSwarm({ swarmId: 'sw-a', hostConversationId: 'conv-a', cwd: wsA, title: 'Workspace A Swarm', sessionsDir });
  await ensureSwarm({ swarmId: 'sw-b', hostConversationId: 'conv-b', cwd: wsB, title: 'Workspace B Swarm', sessionsDir });
  await upsertWorker('sw-a', {
    name: 'worker-a', cli: 'codex', model: 'codex/gpt-5', state: 'working',
    run_id: 'e'.repeat(32), source_cwd: wsA, task_summary: 'Implement endpoint',
  }, { sessionsDir });

  const steered = [];
  const interrupted = [];
  const sidecarOpts = {
    sessionsDir,
    conversationId: 'conv-a',
    workspaceRoots: [wsA],
    steerRunImpl: async (runId, msg) => { steered.push({ runId, msg }); return { ok: true }; },
    interruptRunImpl: async (runId) => { interrupted.push(runId); return { ok: true }; },
  };

  // Sidecar dispatchPageApi defaults list scope to session and isolates workspaces.
  const listReply = await dispatchPageApi({
    method: 'GET',
    url: new URL('http://localhost/api/swarm/list'),
    rawPath: '/api/swarm/list',
    readBody: () => undefined,
  }, sidecarOpts);
  assert.equal(listReply.status, 200);
  assert.deepEqual(listReply.body.swarms.map((s) => s.id), ['sw-a']);
  assert.equal(listReply.body.all_count, 1);

  // Accessing out-of-workspace swarm returns 403.
  await assert.rejects(
    runSwarmOperation('overview', { swarm_id: 'sw-b' }, sidecarOpts),
    (err) => err.status === 403,
  );

  // Authenticated Sidecar POST /api/swarm/post steers active worker immediately.
  const postReply = await dispatchPageApi({
    method: 'POST',
    url: new URL('http://localhost/api/swarm/post'),
    rawPath: '/api/swarm/post',
    readBody: () => ({ swarm_id: 'sw-a', text: '@worker-a add unit tests too' }),
  }, sidecarOpts);
  assert.equal(postReply.status, 200);
  assert.equal(postReply.body.message.from_agent, 'human');
  assert.equal(postReply.body.deliveries[0].mode, 'steer');
  assert.equal(steered.length, 1);
  assert.match(steered[0].msg, /add unit tests too/);

  // Sidecar POST /api/swarm/action interrupt interrupts active run and updates worker to stopped.
  const intReply = await dispatchPageApi({
    method: 'POST',
    url: new URL('http://localhost/api/swarm/action'),
    rawPath: '/api/swarm/action',
    readBody: () => ({ swarm_id: 'sw-a', action: 'interrupt', name: 'worker-a' }),
  }, sidecarOpts);
  assert.equal(intReply.status, 200);
  assert.equal(intReply.body.interrupted, true);
  assert.equal(intReply.body.worker.state, 'stopped');
  assert.deepEqual(interrupted, ['e'.repeat(32)]);
});

test('Swarm UI helpers group threads, tokenize mentions safely, and built bundle includes Swarm Workbench', () => {
  const roster = [
    { name: 'fe', state: 'working' },
    { name: 'be', state: 'waiting' },
    { name: 'qa', state: 'waiting_reply' },
  ];
  const fallback = groupWorkersByThreads([], roster);
  assert.equal(fallback.length, 1);
  assert.equal(fallback[0].isDefault, true);
  assert.equal(fallback[0].workers.length, 3);

  const grouped = groupWorkersByThreads([
    { id: 't1', title: 'Core API', goal: 'Expose endpoints', workers: ['be'], tasks: ['list', 'overview'] },
    { id: 't2', title: 'UI', goal: 'Render board', tasks: ['fe', 'Dark theme'] },
  ], roster);
  assert.equal(grouped.length, 3);
  assert.deepEqual(grouped[0].workers.map((w) => w.name), ['be']);
  assert.deepEqual(grouped[0].tasks, ['list', 'overview']);
  assert.deepEqual(grouped[1].workers.map((w) => w.name), ['fe']);
  assert.deepEqual(grouped[1].tasks, ['Dark theme']);
  assert.equal(grouped[2].isOther, true);
  assert.deepEqual(grouped[2].workers.map((w) => w.name), ['qa']);

  const tokens = tokenizeBoardText('@coordinator [done] notify @worker-1, ignore a@b.com');
  assert.deepEqual(tokens.filter((x) => x.type !== 'text'), [
    { type: 'mention', value: '@coordinator' },
    { type: 'done', value: '[done]' },
    { type: 'mention', value: '@worker-1' },
  ]);

  const builtHtml = readFileSync(join(REPO_ROOT, 'core', 'assets', 'http-ui', 'index.html'), 'utf8');
  for (const id of ['tab-swarm', 'swarm-tab-badge', 'swarm-panel', 'swarm-subtab-overview', 'swarm-subtab-board', 'swarm-composer-input', 'swarm-subtab-members', 'swarm-sub-members', 'swarm-subtab-graph', 'swarm-subtab-raw', 'swarm-sub-raw', 'swarm-hive-view', 'swarm-mentions-svg']) {
    assert.ok(builtHtml.includes(`id="${id}"`), `built HTTP UI should contain #${id}`);
  }
  assert.doesNotMatch(builtHtml, /swarm-single|swarm-toggle-full|routing-badge/);
  assert.doesNotMatch(builtHtml, /<meta\s+http-equiv=["']Content-Security-Policy["']/i);
});

test('native mention boundaries, role defaults, topic limits and historical channel views', () => {
  assert.deepEqual(mentionsOf('(@TEAM), @all; é@x a@b @team-extra @test-2x', ['team-1', 'team-abc', 'x', 'b', 'test-2']),
    { mentions: ['team-1'], all: true });
  assert.ok(mentions('note.@QA', 'qa-21'));
  assert.ok(!mentions('𐐀@qa @qa𐐀', 'qa'));
  assert.deepEqual(channelMentions('a@#bad @#MAIN @#QA @#qa @#all @#a_b'), ['main', 'qa']);
  assert.deepEqual(effectiveChannels('worker', null, 'monitor-2'), ['*']);
  assert.deepEqual(effectiveChannels('narrator', [], 'narrator'), []);
  assert.deepEqual(effectiveChannels('worker', null, 'qa'), []);
  for (const name of ['QA', 'main', 'here', 'all', '1test', 'a'.repeat(25)]) assert.throws(() => validateChannelName(name));
  assert.equal(validateChannelName('a' + '1'.repeat(23)).length, 24);
  for (const topic of ['é'.repeat(101), '@', 'x\r', 'x\n']) assert.throws(() => validateTopic(topic));
  assert.equal(validateTopic('é'.repeat(100)).length, 100);
  const messages = [
    { seq: 1, channel: 'hidden', text: '@#qa' },
    { seq: 2, agent: 'lead', control: { op: 'create', channel: 'qa', topic: 'Tests' } },
    { seq: 3, channel: 'qa', text: 'before joining' },
    { seq: 4, control: { op: 'subscribe', channel: 'qa', members: ['tester'], prev: { tester: null }, next: { tester: ['qa'] } } },
    { seq: 5, channel: 'hidden', text: '@#qa' },
    { seq: 6, control: { op: 'archive', channel: 'qa' } },
    { seq: 7, channel: 'hidden', text: '@#qa' },
    { seq: 8, channel: 'hidden', text: 'own', from_agent: 'tester' },
  ];
  const registry = channelRegistry(messages);
  assert.deepEqual(registry, [{ name: 'qa', created_by: 'lead', topic: 'Tests', archived: true }]);
  assert.deepEqual(currentChannels('tester', null, messages), ['qa']);
  assert.deepEqual(channelsAt('tester', ['qa'], messages).slice(2, 5), [null, ['qa'], ['qa']]);
  const active = activeAt(registry, messages);
  assert.deepEqual(active.map((fn) => fn('qa')), [false, true, true, true, true, false, false, false]);
  assert.ok(mentionsIn('@#qa', 'monitor', ['*'], active[4]));
  assert.ok(seesIn('tester', [], messages.at(-1), active.at(-1)));
  assert.ok(!countsUnread(messages[3]));
  const view = memberView('tester', 'worker', ['qa'], messages, registry);
  assert.deepEqual(view.unread, [false, false, false, false, true, false, false, true]);
  assert.equal(visibleUnread('tester', 'worker', ['qa'], messages, 5, registry), 1);
  assert.equal(homeChannelOf('worker', ['qa', 'ops'], [...registry, { name: 'ops' }]), 'ops');
  assert.equal(homeChannelOf('monitor', null, registry), 'main');
});

test('migration preserves legacy messages, deliveries and roster across concurrent opens', async (t) => {
  const sessionsDir = makeTmp(t);
  const opts = { sessionsDir };
  await ensureSwarm({ swarmId: 'legacy', ...opts });
  await upsertWorker('legacy', { name: 'w', brief: 'preserved' }, opts);
  const msg = await postBoardMessage('legacy', { text: '@w old row' }, opts);
  await markDelivered('legacy', msg.seq, 'w', opts);
  const legacy = await openDatabase(resolveSwarmDbPath(sessionsDir));
  legacy.exec('DROP INDEX idx_swarm_request_id; DROP TABLE post_window');
  for (const column of ['role', 'added_by', 'channels_json', 'wake_mode', 'wait_since', 'last_read_seq', 'last_nudged_seq', 'last_park_blocked_seq', 'quota_paused_seq']) {
    legacy.exec('ALTER TABLE roster DROP COLUMN ' + column);
  }
  for (const column of ['channel', 'control_json', 'request_id']) legacy.exec('ALTER TABLE messages DROP COLUMN ' + column);
  legacy.close();
  const connections = await Promise.all([openSwarmDb(sessionsDir), openSwarmDb(sessionsDir)]);
  for (const db of connections) db.close();
  const w = await getWorker('legacy', 'w', opts);
  assert.deepEqual([w.brief, w.role, w.channels, w.added_by, w.wake_mode, w.last_read_seq],
    ['preserved', 'worker', null, 'coordinator', 'mentions', 0]);
  assert.equal((await readBoardMessages('legacy', opts))[0].channel, 'main');
  assert.deepEqual(await listUndeliveredForWorker('legacy', 'w', opts), []);
});

test('migration adds fork_mode to a v0.8.0 database with the request index', async (t) => {
  const sessionsDir = makeTmp(t);
  const legacy = await openSwarmDb(sessionsDir);
  try {
    legacy.exec('ALTER TABLE roster DROP COLUMN fork_mode; PRAGMA user_version = 0');
    assert.ok(legacy.get("SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_swarm_request_id'"));
  } finally {
    legacy.close();
  }

  const db = await openSwarmDb(sessionsDir);
  try {
    await upsertWorker('legacy', { name: 'child', fork_mode: 'summary_fallback' }, { db });
    assert.equal(db.get('PRAGMA user_version').user_version, 5);
  } finally {
    db.close();
  }
  assert.equal((await getWorker('legacy', 'child', { sessionsDir })).fork_mode, 'summary_fallback');
});

test('v2 to v5 preserves board data and creates the indexed lineage table once', async (t) => {
  const sessionsDir = makeTmp(t);
  const old = await openSwarmDb(sessionsDir);
  await upsertWorker('migration', { name: 'scout', brief: 'read the code' }, { db: old });
  await postBoardMessage('migration', { from: 'scout', text: 'found the entry point' }, { db: old });
  old.exec('DROP TABLE run_lineage; PRAGMA user_version=2');
  old.close();
  for (let i = 0; i < 2; i++) {
    const db = await openSwarmDb(sessionsDir);
    try {
      assert.equal(db.get('PRAGMA user_version').user_version, 5);
      assert.equal(db.get("SELECT brief FROM roster WHERE name = 'scout'").brief, 'read the code');
      assert.equal(db.get('SELECT text FROM messages').text, 'found the entry point');
      assert.deepEqual(db.all("PRAGMA index_list(run_lineage)").filter((idx) => !idx.unique)
        .map((idx) => db.all(`PRAGMA index_info(${idx.name})`).map((c) => c.name).join(',')).sort(),
      ['cache_group_key', 'native_session_id', 'session_id', 'swarm_id,worker_name']);
    } finally { db.close(); }
  }
});

test('v4 database missing roster.timeout_sec migrates to v5 and accepts worker saves', async (t) => {
  const sessionsDir = makeTmp(t);
  const old = await openSwarmDb(sessionsDir);
  try {
    await upsertWorker('legacy-v4', { name: 'scout', brief: 'keep me' }, { db: old });
    old.exec('ALTER TABLE roster DROP COLUMN timeout_sec; PRAGMA user_version = 4');
    assert.equal(old.all('PRAGMA table_info(roster)').some((c) => c.name === 'timeout_sec'), false);
  } finally {
    old.close();
  }

  const migrated = await openSwarmDb(sessionsDir);
  try {
    assert.equal(migrated.get('PRAGMA user_version').user_version, 5);
    assert.equal(migrated.all('PRAGMA table_info(roster)').some((c) => c.name === 'timeout_sec'), true);
    await upsertWorker('legacy-v4', { name: 'solo', timeout_sec: null, brief: 'runs cleanly' }, { db: migrated });
  } finally {
    migrated.close();
  }
  const scout = await getWorker('legacy-v4', 'scout', { sessionsDir });
  const solo = await getWorker('legacy-v4', 'solo', { sessionsDir });
  assert.equal(scout.brief, 'keep me');
  assert.equal(solo.timeout_sec, null);
});

test('helper auto-summary and board_ask route deliveries to waiting parents and peers', async (t) => {
  const sessionsDir = makeTmp(t);
  const id = 'helper-wake';
  const wakes = [];
  const opts = {
    sessionsDir,
    dispatchRunImpl: async (args) => {
      wakes.push({ agent: args.swarmAgent, goal: args.goal });
      return { run_id: `wake-${wakes.length}`, session_id: args.sessionId || `s-${wakes.length}` };
    },
  };
  await ensureSwarm({ swarmId: id, cwd: sessionsDir, sessionsDir });
  await upsertWorker(id, { name: 'lead', state: 'waiting', session_id: 'lead-sess', source_cwd: sessionsDir }, opts);
  await upsertWorker(id, { name: 'helper-ok', added_by: 'lead', state: 'working', run_id: 'run-ok', session_id: 'h1', source_cwd: sessionsDir }, opts);
  await upsertWorker(id, { name: 'helper-fail', added_by: 'lead', state: 'working', run_id: 'run-fail', session_id: 'h2', source_cwd: sessionsDir }, opts);
  await upsertWorker(id, { name: 'reviewer', state: 'waiting', session_id: 'rev-sess', source_cwd: sessionsDir }, opts);

  const okRes = await onWorkerTurnFinished({
    ...opts, swarmId: id, swarmAgent: 'helper-ok', runId: 'run-ok',
    outcome: { state: 'finished' }, result: { text: 'all checks green' },
  });
  assert.ok(okRes.auto_post);
  assert.equal(okRes.deliveries.length, 1);
  assert.equal(okRes.deliveries[0].worker, 'lead');
  assert.equal(okRes.deliveries[0].mode, 'wake');

  await updateWorker(id, 'lead', { state: 'waiting' }, opts);
  const failRes = await onWorkerTurnFinished({
    ...opts, swarmId: id, swarmAgent: 'helper-fail', runId: 'run-fail',
    outcome: { state: 'failed', error: { message: 'boom' } },
  });
  assert.ok(failRes.auto_post);
  assert.equal(failRes.deliveries.length, 1);
  assert.equal(failRes.deliveries[0].worker, 'lead');

  const askRes = JSON.parse((await handleBoardMcpTool(
    'board_ask',
    { question: 'Need approval; @reviewer please inspect too' },
    {},
    { ...opts, swarmId: id, agentName: 'helper-ok' },
  )).content[0].text);
  assert.equal(askRes.ok, true);
  assert.equal((await getWorker(id, 'helper-ok', opts)).state, 'waiting_reply');
  assert.equal(askRes.deliveries.length, 1);
  assert.equal(askRes.deliveries[0].worker, 'reviewer');
  assert.equal(askRes.deliveries[0].mode, 'wake');
});

test('forks retain the logical root while cache keys follow the model and wire protocol', (t) => {
  const sessionsDir = makeTmp(t);
  const add = (run_id, patch) => upsertRunLineage({ cli: 'codex', model: 'm', swarm_id: 's', run_id, ...patch }, { sessionsDir });
  const root = add('root', { worker_name: 'scout', cache_group_key: 'persisted-parent-key' });
  const child = add('child', { parent_run_id: root.run_id });
  const chat = add('chat', { parent_run_id: child.run_id, protocol: 'openai-chat', native_session_id: 'chat-thread' });
  const sibling = add('sibling', { protocol: 'openai-chat' });
  const otherModel = add('other-model', { parent_run_id: root.run_id, model: 'other' });
  assert.equal(child.cache_group_key, root.cache_group_key);
  assert.equal(child.parent_worker, 'scout');
  assert.equal(chat.root_run_id, root.run_id);
  assert.equal(chat.cache_group_key, sibling.cache_group_key);
  assert.equal(new Set([root, chat, otherModel].map((r) => r.cache_group_key)).size, 3);
  assert.equal(findCacheKeyByNativeSession('chat-thread', { sessionsDir }), null);
  for (const row of [root, child, chat, sibling, otherModel]) assert.ok(row.cache_group_key.length <= 64);
});

test('posts accept large text and enforce channel limits, idempotency and shared sliding rate windows', async (t) => {
  const db = await openSwarmDb(makeTmp(t));
  t.after(() => db.close());
  const id = 'limits', opts = { db };
  const post = (input, extra) => postBoardMessage(id, input, { ...opts, ...extra });
  const long = 'é'.repeat(65537);
  assert.equal((await post({ text: long }, { skipRateLimit: true })).text, long);
  await assert.rejects(post({ text: '@#MAIN' }), /@#main is everyone; write @all/);
  await assert.rejects(post({ text: 'x', channel: 'uncreated' }), /channel/);
  assert.equal((await post({ text: 'é'.repeat(32768), channel: '' }, { skipRateLimit: true })).channel, 'main');
  const retry = { text: '@#unknown', requestId: 'stable' };
  const [one, two] = await Promise.all([post(retry), post(retry)]);
  assert.deepEqual(one.channel_mentions, ['unknown']);
  assert.match(one.warnings[0], /addressed nobody \(unknown channel\)/);
  assert.deepEqual(one, two);
  for (let i = 1; i < 60; i++) await post({ text: 'tick' });
  await assert.rejects(post({ text: 'over' }), /60 posts/);
  assert.equal((await post(retry)).seq, one.seq);
  await post({ text: 'another sender', from: 'other' });
  await post({ text: 'bypassed' }, { skipRateLimit: true });
  db.run('UPDATE post_window SET posted_ms = ? WHERE rowid = (SELECT MIN(rowid) FROM post_window)', Date.now() - 60001);
  await post({ text: 'one slot reopened' });
  await assert.rejects(post({ text: 'full again' }), /60 posts/);
  resetRateLimiter();
  await post({ text: 'reset window' });
});

test('channel controls and membership commit atomically and reads honor historical visibility', async (t) => {
  const id = 'history', opts = { sessionsDir: makeTmp(t) };
  await upsertWorker(id, { name: 'qa-1' }, opts);
  await upsertWorker(id, { name: 'monitor-1' }, opts);
  const create = await createChannel(id, { actor: 'qa-1', channel: 'qa', topic: 'Tests' }, opts);
  assert.equal(create.text, '[channel] qa-1 created #qa: Tests');
  await assert.rejects(createChannel(id, { channel: 'qa' }, opts), /already exists/);
  await assert.rejects(archiveChannel(id, { channel: 'qa', actor: 'other' }, opts), /creator/);
  const post = (text, channel = 'qa') => postBoardMessage(id, { text, channel }, opts);
  const before = await post('before');
  await assert.rejects(subscribeChannels(id, { channel: 'qa', op: 'subscribe', targets: ['qa-1', 'missing'] }, opts), /Unknown/);
  assert.equal((await getWorker(id, 'qa-1', opts)).channels, null);
  assert.equal(await getBoardHead(id, opts), before.seq);
  const change = (op) => subscribeChannels(id, { op, channel: 'qa', targets: ['qa-1'] }, opts);
  const joined = await change('subscribe');
  assert.deepEqual(joined.control.prev, { 'qa-1': null });
  const inside = await post('inside');
  const left = await change('unsubscribe');
  assert.deepEqual(left.control.next, { 'qa-1': [] });
  const after = await post('after');
  const direct = await post('@qa action');
  const ordinary = await post('main message', 'main');
  const read = (args) => readBoardMessages(id, { viewer: 'qa-1', includeControl: false, ...args, ...opts });
  assert.deepEqual((await read({ limit: 2 })).map((m) => m.seq), [direct.seq, ordinary.seq]);
  assert.deepEqual((await read({ since: 0, limit: 1 })).map((m) => m.seq), [inside.seq]);
  assert.deepEqual((await read({ seqs: [after.seq, before.seq, inside.seq] })).map((m) => m.seq), [inside.seq]);
  assert.equal((await getWorker(id, 'qa-1', opts)).last_read_seq, ordinary.seq);
  await markRead(id, 'qa-1', before.seq, opts);
  assert.equal((await getWorker(id, 'qa-1', opts)).last_read_seq, ordinary.seq);
  assert.equal((await read({ channel: 'qa' })).length, 4);
  assert.equal((await read({ channel: '*' })).length, 5);
  assert.equal((await listChannels(id, opts))[0].members.includes('monitor-1'), true);
  await archiveChannel(id, { actor: 'qa-1', channel: 'qa' }, opts);
  await assert.rejects(post('closed'), /archived/);
  assert.match((await post('@#qa check', 'main')).warnings[0], /archived channel/);
  assert.equal((await read({ channel: 'qa' }))[0].seq, before.seq);
});

test('wake modes handle cross-channel doorbells, archive history, quota and parked reads', async (t) => {
  const id = 'wake-policy', opts = { sessionsDir: makeTmp(t) };
  await upsertWorker(id, { name: 'qa-1', channels: ['qa'] }, opts);
  await createChannel(id, { channel: 'qa' }, opts);
  await createChannel(id, { channel: 'other' }, opts);
  const send = (text, channel, from = 'human') => postBoardMessage(id, { text, channel, from }, opts);
  await send('hidden', 'other');
  await send('just visible', 'qa');
  const bell = await send('@#QA please', 'other');
  const group = await send('@qa group', 'other');
  const all = await send('@all rule', 'other');
  await send('@qa self', 'qa', 'qa-1');
  await archiveChannel(id, { channel: 'qa' }, opts);
  await send('@#qa closed', 'other');
  const pending = async () => (await listUndeliveredForWorker(id, 'qa-1', opts)).map((m) => m.seq);
  assert.deepEqual(await pending(), [bell.seq, group.seq, all.seq]);
  const head = await getBoardHead(id, opts);
  await updateWorker(id, 'qa-1', { quota_paused_seq: head }, opts);
  assert.deepEqual(await pending(), []);
  const visible = await send('new work', 'main');
  assert.deepEqual(await pending(), [bell.seq, group.seq, all.seq]);
  assert.equal((await getWorker(id, 'qa-1', opts)).quota_paused_seq, 0);
  await parkWorker(id, 'qa-1', { ...opts, since: head, mentionsOnly: false });
  assert.deepEqual(await pending(), [visible.seq]);
  await markRead(id, 'qa-1', visible.seq, opts);
  assert.deepEqual(await pending(), []);
});

test('mention graph distinguishes direct, broadcast and historical channel edges; attention explains parked work', () => {
  const time = Date.parse('2026-10-10T12:00:00Z');
  const roster = [
    { name: 'build-1', channels: ['build'], state: 'waiting' },
    { name: 'qa', state: 'stopped', error: 'test failed' },
    { name: 'limit', state: 'stopped', error: 'HTTP 429' },
    { name: 'monitor', role: 'monitor', state: 'working', updated_at: new Date(time).toISOString() },
  ];
  const messages = [
    { seq: 1, from_agent: 'coordinator', control: { op: 'create', channel: 'build' } },
    { seq: 2, from_agent: 'qa', text: '@build' },
    { seq: 3, from_agent: 'qa', text: '@#build' },
    { seq: 4, from_agent: 'qa', text: '@all' },
    { seq: 5, from_agent: 'coordinator', control: { op: 'archive', channel: 'build' } },
    { seq: 6, from_agent: 'qa', text: '@#build' },
  ];
  const channels = channelRegistry(messages);
  const edges = computeMentionEdges(messages, roster, channels);
  assert.deepEqual(edges.find((e) => e.from === 'qa' && e.to === 'build-1'),
    { from: 'qa', to: 'build-1', count: 3, direct: 1, viaAll: 1, viaChannel: 1 });
  assert.ok(!edges.some((e) => e.to === e.from || e.to === 'monitor'));
  const attention = (posts, workers = roster) => detectSystemAttention({}, workers, posts, channels, time);
  assert.deepEqual(attention(messages).map((a) => a.kind).sort(), ['error', 'quota', 'stall']);
  assert.match(attention(messages).at(-1).text, /Unanswered mention/);
  assert.match(attention([{ seq: 1, from_agent: 'qa', text: 'result' }]).at(-1).text, /Result #1/);
  assert.match(attention([]).at(-1).text, /All workers/);
  assert.ok(!attention([...messages, { seq: 7, from_agent: 'coordinator', text: '[done] shipped' }]).some((a) => a.kind === 'stall'));
  const old = new Date(time - 600_001).toISOString();
  assert.equal(attention([], [{ name: 'qa', state: 'working', updated_at: old }])[0].kind, 'stuck');
  assert.deepEqual(attention([{ from_agent: 'qa', created_at: new Date(time).toISOString() }], [{ name: 'qa', state: 'working', updated_at: old }]), []);
});

test('worker tool ownership, channel briefs, forks, sharding and group management', async (t) => {
  const sessionsDir = makeTmp(t), id = 'helpers', goals = [];
  const opts = { sessionsDir, configPath: join(sessionsDir, 'config.json'),
    dispatchRunImpl: async (args) => { goals.push(args); return { run_id: 'run-' + goals.length, session_id: 's' + goals.length }; },
    interruptRunImpl: async () => {} };
  const action = (input) => handleSwarmAction({ swarm_id: id, cwd: sessionsDir, ...input }, opts);
  const tool = async (name, args, agentName = 'owner') => JSON.parse((await handleBoardMcpTool(name, args, {}, { ...opts, swarmId: id, agentName })).content[0].text);
  await action({ action: 'add', name: 'owner', brief: 'Analyze parser' });
  await action({ action: 'add', name: 'outsider', brief: 'Review' });
  await tool('board_swarm', { action: 'channel', op: 'create', channel: 'parser', topic: 'Parser work' });
  await tool('board_swarm', { action: 'channel', op: 'subscribe', channel: 'parser' });
  const note = await tool('board_post', { text: 'Parser accepts UTF-8. @#unk' });
  assert.equal(note.message.channel, 'parser');
  assert.match(note.warnings[0], /unknown channel/);
  const copies = await tool('board_swarm', { action: 'add', added_by: 'outsider', actor: 'coordinator', swarm_id: 'other',
    name: 'test', n: 2, brief: 'Verify', fork: 'owner', channels: ['parser'] });
  assert.deepEqual(copies.workers.map((w) => w.added_by), ['owner', 'owner']);
  assert.match(goals.at(-1).goal, /0-based index mod 2 == 1/);
  const siblingBrief = await readBoardMessages(id, { ...opts, seqs: [copies.workers[1].brief_seq] });
  assert.deepEqual(siblingBrief[0].mentions, ['test-2']);
  assert.match(goals.at(-1).goal, /Everything before this turn is @owner's memory/);
  assert.match(goals.at(-1).goal, /Parser accepts UTF-8/);
  assert.match(goals.at(-1).goal, /wakeup doorbell/);
  assert.match(goals.at(-1).goal, /#parser by default/);
  assert.equal((await readBoardMessages(id, { ...opts, seqs: [copies.workers[0].brief_seq] }))[0].channel, 'parser');
  await assert.rejects(tool('board_swarm', { action: 'kill', name: 'test', actor: 'coordinator' }, 'outsider'), /Cannot kill/);
  await assert.rejects(tool('board_swarm', { action: 'model', name: 'test-1', model: 'demo' }, 'outsider'), /Cannot switch/);
  await updateWorker(id, 'test-1', { state: 'waiting' }, opts);
  const switched = await tool('board_swarm', { action: 'model', name: 'test-1', model: 'demo' });
  assert.equal(switched.worker.model, 'demo');
  await tool('board_swarm', { action: 'add', name: 'child', brief: 'Check' }, 'test-1');
  assert.equal((await tool('board_swarm', { action: 'kill', name: 'test' })).workers.length, 2);
  assert.equal((await getWorker(id, 'child', opts)).state, 'working');
  const special = await action({ action: 'add', name: 'narrator-2', brief: 'Observe' });
  assert.equal(special.workers[0].role, 'narrator');
  assert.deepEqual(special.workers[0].channels, ['*']);
  await assert.rejects(action({ action: 'add', name: 'bad', brief: 'x', fork: 'unknown' }), /unknown member/);
  await assert.rejects(action({ action: 'add', name: 'bad', brief: 'x', channels: ['missing'] }), /archived channel/);
  const status = await tool('board_read', { status: true });
  const owner = status.roster.find((w) => w.name === 'owner');
  assert.ok(owner.last_post_seq > 0 && owner.last_post_time);
  assert.deepEqual(owner.channels, ['parser']);
  assert.equal(typeof owner.unread, 'number');
});

test('concurrent adds respect the 32-worker cap and failed group reservations roll back', async (t) => {
  const sessionsDir = makeTmp(t), id = 'cap', opts = { sessionsDir, dispatchRunImpl: async () => ({ run_id: 'stub', session_id: 'stub' }) };
  const db = await openSwarmDb(sessionsDir);
  try {
    for (let i = 0; i < 30; i++) await upsertWorker(id, { name: 'w' + i }, { db });
  } finally { db.close(); }
  const add = (name) => handleSwarmAction({ action: 'add', name, n: 2, brief: 'task', swarm_id: id, cwd: sessionsDir }, opts);
  const outcomes = await Promise.allSettled([add('a'), add('b')]);
  assert.equal(outcomes.filter((o) => o.status === 'fulfilled').length, 1);
  assert.match(outcomes.find((o) => o.status === 'rejected').reason.message, /32 live workers/);
  assert.equal((await listRoster(id, opts)).length, 32);
  assert.equal((await readBoardMessages(id, opts)).length, 2);
});

test('board_wait and turn-finish guard deliver racing posts; quota resumes only after a later post', async (t) => {
  const sessionsDir = makeTmp(t), id = 'park', wakes = [];
  const opts = { sessionsDir, dispatchRunImpl: async (args) => { wakes.push(args); return { run_id: 'wake', session_id: 'session' }; } };
  await ensureSwarm({ swarmId: id, cwd: sessionsDir, sessionsDir });
  await upsertWorker(id, { name: 'w', run_id: 'original', session_id: 'session', source_cwd: sessionsDir }, opts);
  const tool = async (args) => JSON.parse((await handleBoardMcpTool('board_wait', args, {}, { ...opts, agentName: 'w', swarmId: id })).content[0].text);
  const post = (text) => postBoardMessage(id, { from: 'peer', text }, opts);
  await assert.rejects(tool({}), /since/);
  const first = await post('@w review');
  await markDelivered(id, first.seq, 'w', opts);
  const blocked = await tool({ since: 0 });
  assert.ok(blocked.park_blocked);
  assert.equal(blocked.messages[0].seq, first.seq);
  const parked = await tool({ since: first.seq, mentions_only: false, channel: 'main' });
  assert.ok(parked.parked);
  const late = await handleBoardAction({ action: 'post', swarm_id: id, text: 'last-minute work' }, opts);
  assert.deepEqual(late.deliveries, []);
  const finish = (outcome) => onWorkerTurnFinished({ ...opts, swarmId: id, swarmAgent: 'w', outcome });
  const guard = await finish({ state: 'finished' });
  assert.equal(guard.auto_post, null);
  assert.deepEqual(guard.deliveries[0].seqs, [late.message.seq]);
  assert.equal((await getWorker(id, 'w', opts)).last_park_blocked_seq, late.message.seq);
  await post('@w still pending');
  await finish({ state: 'failed', error: 'RESOURCE_EXHAUSTED' });
  assert.equal((await getWorker(id, 'w', opts)).quota_paused_seq, await getBoardHead(id, opts));
  assert.deepEqual(await listUndeliveredForWorker(id, 'w', opts), []);
  const retry = await handleBoardAction({ action: 'post', swarm_id: id, text: '@w continue' }, opts);
  assert.equal(retry.deliveries[0].mode, 'wake');
  assert.equal(wakes.length, 2);
  assert.equal((await getWorker(id, 'w', opts)).quota_paused_seq, 0);
});

test('shared HTTP and sidecar routes expose channel actions, filtered history, raw files and scoped access', async (t) => {
  const sessionsDir = makeTmp(t), id = 'api-native', ws = join(sessionsDir, 'repo');
  mkdirSync(ws);
  const opts = { sessionsDir, configPath: join(sessionsDir, 'c.json'), workspaceRoots: [ws] };
  await ensureSwarm({ swarmId: id, title: 'Native', cwd: ws, sessionsDir });
  await upsertWorker(id, { name: 'w-1', state: 'waiting', task_summary: 'test' }, opts);
  const route = async (operation, args, post = false, options = opts) => {
    const url = new URL('http://localhost/api/swarm/' + operation);
    if (!post) url.search = new URLSearchParams(args).toString();
    return (await dispatchPageApi({ method: post ? 'POST' : 'GET', url, readBody: () => args }, options)).body;
  };
  const input = { swarm_id: id, action: 'channel', channel: 'api' };
  await route('action', { ...input, op: 'create', topic: 'HTTP' }, true);
  await route('post', { swarm_id: id, text: 'API result', channel: 'api' }, true);
  const result = await route('board', { swarm_id: id, channel: 'api' });
  assert.deepEqual(result.messages.map((m) => m.text), ['API result']);
  const overview = await route('overview', { swarm_id: id });
  assert.equal(overview.channels[0].topic, 'HTTP');
  assert.equal(overview.system_attention[0].kind, 'stall');
  assert.ok(Array.isArray(overview.edges));
  const raw = (kind) => route('raw', { swarm_id: id, kind });
  const board = await raw('board');
  assert.equal(board.filename, 'board.jsonl');
  const lines = board.content.trim().split('\n').map((s) => JSON.parse(s));
  assert.deepEqual(lines[0].control, { op: 'create', channel: 'api', topic: 'HTTP' });
  assert.equal(lines[1].agent, 'human');
  assert.equal(JSON.parse((await raw('members')).content).members['w-1'].added_by, 'coordinator');
  assert.equal(JSON.parse((await raw('plan')).content).tasks['w-1'], 'test');
  await assert.rejects(route('raw', { swarm_id: id, kind: 'board' }, false, { ...opts, workspaceRoots: [join(sessionsDir, 'elsewhere')] }), (e) => e.status === 403);
  const server = await startServer({ sessionsDir, configPath: opts.configPath,
    magpieConfigDir: join(sessionsDir, 'magpie'), manageAppServer: false, port: 0 });
  t.after(() => server.close());
  const base = 'http://127.0.0.1:' + server.port;
  const key = await getPageKey(base);
  const path = base + '/api/swarm/raw?swarm_id=' + id + '&kind=board';
  assert.equal((await fetch(path)).status, 401);
  assert.equal((await (await fetch(path, { headers: { 'x-gempt-key': key } })).json()).content, board.content);
  await route('action', { ...input, op: 'archive' }, true);
  assert.equal((await route('action', { swarm_id: id, action: 'kill', name: 'w' }, true)).workers[0].state, 'killed');
});

test('coordinator wait filters channel mentions, finds later matches and wakes for silent stalls', async (t) => {
  const id = 'wait-channel', opts = { sessionsDir: makeTmp(t) };
  await upsertWorker(id, { name: 'qa', state: 'working' }, opts);
  await createChannel(id, { channel: 'qa' }, opts);
  await postBoardMessage(id, { text: '@coordinator hidden by filter', from: 'qa' }, opts);
  for (let i = 0; i < 102; i++) {
    await postBoardMessage(id, { channel: 'qa', from: 'qa', text: 'chatter' }, { ...opts, skipRateLimit: true });
  }
  const message = await postBoardMessage(id, { channel: 'qa', from: 'qa', text: '@#qa decision' }, opts);
  const output = execFileSync(process.execPath, ['core/wait.mjs', '--board', id, '--channel', 'qa'], {
    cwd: REPO_ROOT, env: { ...process.env, GEMPT_SESSIONS_DIR: opts.sessionsDir }, encoding: 'utf8', timeout: 8000,
  });
  const snapshot = JSON.parse(output);
  assert.equal(snapshot.head, message.seq);
  assert.deepEqual(snapshot.messages.map((m) => m.seq), Array.from({ length: 103 }, (_, i) => message.seq - 102 + i));
  assert.ok(snapshot.messages.every((m) => m.channel === 'qa'));
  await updateWorker(id, 'qa', { state: 'waiting' }, opts);
  const stalled = await waitForBoard(id, { ...opts, since: message.seq, timeoutMs: 3000 });
  assert.ok(stalled.system_attention.some((item) => item.kind === 'stall'));
  assert.deepEqual(stalled.messages, []);
});


test('Board tokens preserve channel mentions and standalone sequence citations', () => {
  const input = 'Send @#API to @worker-1 via #12 (#4), ignore pr#7 #4th and x@#bad';
  const tokens = tokenizeBoardText(input);
  assert.equal(tokens.map(t => t.value).join(''), input);
  assert.deepEqual(tokens.filter(t => t.type !== 'text'), [
    { type: 'channel_mention', value: '@#API', channel: 'api' },
    { type: 'mention', value: '@worker-1' },
    { type: 'seq_ref', value: '#12', seq: 12 },
    { type: 'seq_ref', value: '#4', seq: 4 },
  ]);
});

test('GIF89a encoder emits a looping animation with a cropped transparent delta', () => {
  const width = 10, height = 6;
  const base = new Uint8Array(width * height * 4).fill(255);
  const edited = base.slice();
  for (const [x, y] of [[2, 1], [5, 3]]) edited.set([16, 32, 96, 255], 4 * (width * y + x));
  const encoder = createGifEncoder({ width, height, sampleFrames: [base, edited] });
  encoder.addFrame(base, 120);
  encoder.addFrame(edited, 250);
  const gif = Buffer.from(encoder.finish());
  assert.equal(gif.toString('ascii', 0, 6), 'GIF89a');
  assert.equal(gif.at(-1), 0x3b);
  assert.equal(gif.readUInt16LE(6), width);
  assert.equal(gif.readUInt16LE(8), height);
  assert.ok(gif.includes(Buffer.from('NETSCAPE2.0')));
  const frames = [];
  let p = 13 + 3 * (2 << (gif[10] & 7));
  let control;
  const skipBlocks = () => { while (gif[p]) p += 1 + gif[p]; p++; };
  while (gif[p] !== 0x3b) {
    if (gif[p] === 0x21) {
      const tag = gif[p + 1]; p += 2;
      if (tag === 0xf9) control = { transparent: Boolean(gif[p + 1] & 1), delay: gif.readUInt16LE(p + 2) };
      skipBlocks();
    } else {
      assert.equal(gif[p++], 0x2c);
      const rect = [0, 2, 4, 6].map(offset => gif.readUInt16LE(p + offset));
      frames.push({ rect, ...control });
      p += 8;
      const flags = gif[p++];
      if (flags & 128) p += 3 * (2 << (flags & 7));
      p++; // LZW minimum code size, followed by data sub-blocks
      skipBlocks();
    }
  }
  assert.deepEqual(frames.map(f => f.rect), [[0, 0, 10, 6], [2, 1, 4, 3]]);
  assert.deepEqual(frames.map(f => f.delay), [12, 25]);
  assert.equal(frames[1].transparent, true);
});

test('worker identity and opening cursor follow the entire shared protocol and assignment', () => {
  const options = { swarmId: 'prefix-test', brief: 'Verify the parser', owner: 'lead' };
  const split = (workerName, seq) => buildWorkerInstructions({ ...options, workerName, seq }).split('Worker identity:');
  const [alphaPrefix, alphaTail] = split('alpha', 1);
  const [bravoPrefix, bravoTail] = split('bravo', 2);
  assert.equal(alphaPrefix, bravoPrefix);
  assert.match(alphaPrefix, /board_wait[\s\S]*## Assignment\nVerify the parser[\s\S]*@lead/);
  assert.equal(alphaTail, ' @alpha\nSwarm: prefix-test\nOpening brief: #1');
  assert.equal(bravoTail, ' @bravo\nSwarm: prefix-test\nOpening brief: #2');
});

test('board_ask reply warm-wakes worker, model switch uses pre-allocated run_id, cross-CLI helper skips parent model, and idle interrupt returns 409', async (t) => {
  const dir = makeTmp(t, 'gempt-regressions-');
  const sessionsDir = join(dir, 'sessions');
  const configPath = join(dir, 'config.json');
  const dispatched = [];
  const mocks = {
    sessionsDir,
    configPath,
    dispatchRunImpl: async (args) => {
      dispatched.push(args);
      return { run_id: args.run_id || 'a'.repeat(32), session_id: args.session_id || `exec-${dispatched.length}`, cwd: args.cwd };
    },
    readRunImpl: () => ({ state: 'finished' }),
    interruptRunImpl: async () => ({ ok: true }),
  };

  await ensureSwarm({ swarmId: 'sw-reg', cwd: dir, sessionsDir });
  await upsertWorker('sw-reg', {
    name: 'claude-w', cli: 'claude', model: 'claude-sonnet-4', state: 'working',
    run_id: '1'.repeat(32), session_id: 'exec-claude-1', source_cwd: dir,
  }, { sessionsDir });

  // 1. board_ask sets waiting_reply + parked_on: board_ask:<seq>; coordinator reply must warm-wake (not reply_failed).
  await handleBoardMcpTool('board_ask', { question: 'Which port?' }, {}, { swarmId: 'sw-reg', agentName: 'claude-w', sessionsDir });
  assert.match((await getWorker('sw-reg', 'claude-w', { sessionsDir })).parked_on, /^board_ask:\d+$/);
  const replyPost = await handleBoardAction({ action: 'post', swarm_id: 'sw-reg', from: 'coordinator', text: '@claude-w Use 3498' }, mocks);
  assert.equal(replyPost.deliveries[0].mode, 'wake');
  assert.equal(dispatched.at(-1).session_id, 'exec-claude-1');

  // 2. Cross-CLI helper spawned by claude-w with cli='codex' must NOT inherit claude-sonnet-4.
  await handleBoardMcpTool('board_swarm', { action: 'add', name: 'codex-helper', brief: 'Run tests', cli: 'codex' }, {}, {
    ...mocks, swarmId: 'sw-reg', agentName: 'claude-w',
  });
  assert.equal(dispatched.at(-1).cli, 'codex');
  assert.equal(dispatched.at(-1).model, undefined);

  // 3. Model switch pre-allocates run_id so fast turn finish is not skipped as superseded.
  await updateWorker('sw-reg', 'claude-w', { state: 'waiting' }, { sessionsDir });
  const fastMocks = {
    ...mocks,
    dispatchRunImpl: async (args) => {
      assert.match(args.run_id, /^[a-f0-9]{32}$/);
      dispatched.push(args);
      const [notice] = await readBoardMessages('sw-reg', { sessionsDir, limit: 1 });
      assert.deepEqual(notice.meta.model_switch, { cli: 'codex', model: 'codex/gpt-5', tier: null });
      assert.match(args.goal, new RegExp(`^\\[Board #${notice.seq} from @coordinator\\]`));
      assert.deepEqual(await listUndeliveredForWorker('sw-reg', 'claude-w', { sessionsDir }), []);
      const later = await postBoardMessage('sw-reg', { text: '@claude-w One more thing' }, { sessionsDir });
      await markDelivered('sw-reg', [later.seq], 'claude-w', { sessionsDir });
      await onWorkerTurnFinished({
        swarmId: 'sw-reg', swarmAgent: 'claude-w', runId: args.run_id, sessionId: args.session_id,
        outcome: { state: 'finished' }, result: { text: 'switched ok', session_id: args.session_id }, sessionsDir,
      });
      return { run_id: args.run_id, session_id: args.session_id, cwd: args.cwd };
    },
  };
  const switched = await handleSwarmAction({ action: 'model', swarm_id: 'sw-reg', name: 'claude-w', cli: 'codex', model: 'codex/gpt-5' }, fastMocks);
  assert.equal(switched.worker.state, 'waiting');
  assert.ok(switched.worker.last_seq > switched.seq);

  // 4. Interrupting an idle waiting worker via swarm-service returns 409.
  await assert.rejects(
    runSwarmOperation('action', { swarm_id: 'sw-reg', action: 'interrupt', name: 'claude-w' }, { sessionsDir }),
    (err) => err.status === 409 && /not currently running/i.test(err.message),
  );
});

test('board_ask reply rechecks the worker after a lost active claim', async (t) => {
  const dir = makeTmp(t);
  const sessionsDir = join(dir, 'sessions');
  const swarmId = 'sw-ask-race';
  const oldRun = '1'.repeat(32);
  const sessionId = 'exec-ask-race';
  await ensureSwarm({ swarmId, cwd: dir, sessionsDir });
  await upsertWorker(swarmId, {
    name: 'worker', state: 'waiting_reply', parked_on: 'board_ask:1', run_id: oldRun, session_id: sessionId,
  }, { sessionsDir });

  let finish;
  const steered = [];
  const woken = [];
  const reply = await handleBoardAction({
    action: 'post', swarm_id: swarmId, from: 'coordinator', text: '@worker Use 3498.',
  }, {
    sessionsDir,
    readRunImpl: () => {
      // Commit the finish after the liveness check, before the conditional claim.
      finish ??= updateWorker(swarmId, 'worker', { state: 'waiting', parked_on: null }, { sessionsDir });
      return { state: 'running' };
    },
    steerRunImpl: async (runId) => steered.push(runId),
    dispatchRunImpl: async (args) => {
      woken.push(args);
      return { run_id: args.run_id, session_id: args.session_id };
    },
  });
  await finish;
  assert.deepEqual(steered, []);
  assert.equal(woken.length, 1);
  assert.equal(woken[0].session_id, sessionId);
  assert.notEqual(woken[0].run_id, oldRun);
  assert.equal(reply.deliveries[0].mode, 'wake');
  assert.deepEqual(await listUndeliveredForWorker(swarmId, 'worker', { sessionsDir }), []);

  // If the claim wins but the turn ends before steer, the warm wake uses the claimed state.
  await updateWorker(swarmId, 'worker', { state: 'waiting_reply', parked_on: 'board_ask:2' }, { sessionsDir });
  let finished = false;
  const lateReply = await handleBoardAction({
    action: 'post', swarm_id: swarmId, from: 'coordinator', text: '@worker Use 3499 instead.',
  }, {
    sessionsDir,
    readRunImpl: () => ({ state: finished ? 'finished' : 'running' }),
    steerRunImpl: async () => {
      finished = true;
      throw new Error('Run has already finished.');
    },
    dispatchRunImpl: async (args) => ({ run_id: args.run_id, session_id: args.session_id }),
  });
  assert.equal(lateReply.deliveries[0].mode, 'wake');
  assert.notEqual(lateReply.deliveries[0].run_id, woken[0].run_id);
});

test('model switch dispatch failure restores the prior worker runtime and state', async (t) => {
  const dir = makeTmp(t);
  const sessionsDir = join(dir, 'sessions');
  const swarmId = 'sw-switch-failure';
  const previous = {
    state: 'waiting', run_id: '1'.repeat(32), session_id: 'exec-old', parked_on: 'board_wait',
    model: 'claude-sonnet-4', tier: 'sonnet', cli: 'claude',
  };
  await ensureSwarm({ swarmId, cwd: dir, sessionsDir });
  await upsertWorker(swarmId, { name: 'worker', ...previous }, { sessionsDir });
  const failure = new Error('dispatch failed');

  await assert.rejects(handleSwarmAction({
    action: 'model', swarm_id: swarmId, name: 'worker', model: 'codex/gpt-5', tier: '', cli: 'codex',
  }, {
    sessionsDir,
    dispatchRunImpl: async () => { throw failure; },
  }), (error) => error === failure);
  const worker = await getWorker(swarmId, 'worker', { sessionsDir });
  assert.deepEqual(Object.fromEntries(Object.keys(previous).map((key) => [key, worker[key]])), previous);
  assert.equal(worker.error, failure.message);
  const [notice] = await readBoardMessages(swarmId, { sessionsDir });
  assert.match(notice.text, /Switching runtime/);
});

test('native fork instructions append concise board reporting protocol after shared assignment', () => {
  const text = buildWorkerInstructions({
    swarmId: 'sw-fork-prompt',
    workerName: 'auditor',
    brief: 'Audit the parser',
    seq: 12,
    nativeParent: 'scout',
    owner: 'lead',
  });
  const briefIdx = text.indexOf('Audit the parser');
  const protoIdx = text.indexOf('Shared message board protocol:');
  assert.ok(briefIdx >= 0 && protoIdx > briefIdx, 'board protocol should follow the shared assignment');
  assert.match(text, /board_post/);
  assert.match(text, /@lead \[done\]/);
  assert.match(text, /@peer \[handoff\]/);
  assert.match(text, /board_read/);
  assert.match(text, /ask_gemini/);
});

test('board CLI instructions cover worker and narrator commands and the park contract', () => {
  for (const role of [{}, { nativeParent: 'parent' }, { role: 'narrator' }]) {
    const prompt = buildWorkerInstructions({ workerName: 'member', swarmId: 'test', brief: 'Review', seq: 1, ...role });
    const commands = role.role === 'narrator' ? ['read', 'roster', 'swarm status', 'publish-view'] : ['read', 'roster', 'post', 'ask', 'swarm'];
    for (const command of commands) assert.ok(prompt.includes(`gempt board ${command}`), command);
    assert.match(prompt, /gempt board wait --since N.*highest seq you actually read.*End your turn.*parked: true/);
  }
});

test('auto_summary runs after interim [progress] posts, skips after [done] or [handoff], and records turn board telemetry', async (t) => {
  const dir = makeTmp(t);
  const sessionsDir = join(dir, 'sessions');
  const swarmId = 'sw-turn-summary';
  await ensureSwarm({ swarmId, cwd: dir, sessionsDir });

  // 1) Interim [progress] post should NOT suppress end-of-turn auto_summary.
  const run1 = 'a'.repeat(32);
  const t1 = new Date(Date.now() - 1000).toISOString();
  await upsertWorker(swarmId, { name: 'builder', state: 'working', run_id: run1, session_id: 's1' }, { sessionsDir });
  await postBoardMessage(swarmId, { from: 'builder', text: '[progress] halfway through files', meta: { run_id: run1 } }, { sessionsDir });
  let captured1 = null;
  const fin1 = await onWorkerTurnFinished({
    swarmId, swarmAgent: 'builder', runId: run1, sessionId: 's1', startedAt: t1,
    outcome: { state: 'finished', cli: 'codex', model: 'codex/gpt-6.2-sol' },
    result: { text: 'Finished implementing parser.' },
    sessionsDir,
    beforeDelivery: (stats) => { captured1 = stats; },
  });
  assert.ok(fin1.auto_post, 'interim [progress] post must not suppress auto_summary');
  assert.equal(fin1.auto_post.meta.auto_summary, true);
  assert.deepEqual(captured1, { board_posts_worker: 1, board_posts_auto: 1 });

  // 2) Explicit [done] post SHOULD suppress duplicate auto_summary.
  const run2 = 'b'.repeat(32);
  const t2 = new Date(Date.now() - 1000).toISOString();
  await upsertWorker(swarmId, { name: 'builder', state: 'working', run_id: run2, session_id: 's1' }, { sessionsDir });
  await postBoardMessage(swarmId, { from: 'builder', text: '@coordinator [done] Parser passes tests.', meta: { run_id: run2 } }, { sessionsDir });
  const fin2 = await onWorkerTurnFinished({
    swarmId, swarmAgent: 'builder', runId: run2, sessionId: 's1', startedAt: t2,
    outcome: { state: 'finished', cli: 'codex', model: 'codex/gpt-6.2-sol' },
    result: { text: 'Done.' },
    sessionsDir,
  });
  assert.equal(fin2.auto_post, null);
  assert.equal(fin2.board_posts_worker, 1);
  assert.equal(fin2.board_posts_auto, 0);

  // 3) Direct peer [handoff] post SHOULD suppress auto_summary.
  const run3 = 'c'.repeat(32);
  const t3 = new Date(Date.now() - 1000).toISOString();
  await upsertWorker(swarmId, { name: 'builder', state: 'working', run_id: run3, session_id: 's1' }, { sessionsDir });
  await postBoardMessage(swarmId, { from: 'builder', text: '@verifier [handoff] Check #2', meta: { run_id: run3 } }, { sessionsDir });
  const fin3 = await onWorkerTurnFinished({
    swarmId, swarmAgent: 'builder', runId: run3, sessionId: 's1', startedAt: t3,
    outcome: { state: 'finished', cli: 'codex', model: 'codex/gpt-6.2-sol' },
    result: { text: 'Handed off.' },
    sessionsDir,
  });
  assert.equal(fin3.auto_post, null);
  assert.equal(fin3.board_posts_worker, 1);
  assert.equal(fin3.board_posts_auto, 0);
});

test('Raw tab snapshot flags require an explicit true in either environment variable', async (t) => {
  const sessionsDir = makeTmp(t);
  const options = { sessionsDir, conversationId: 'raw-tab-owner' };
  await ensureSwarm({ swarmId: 'raw-tab', hostConversationId: options.conversationId, cwd: sessionsDir, sessionsDir });
  const keys = ['SWARM_RAW_TAB', 'GEMPT_SWARM_RAW_TAB'];
  for (const key of keys) {
    const previous = process.env[key];
    t.after(() => {
      if (previous === undefined) delete process.env[key];
      else process.env[key] = previous;
    });
    delete process.env[key];
  }
  const snapshot = async (enabled, query = {}) => {
    const snap = await runSwarmOperation('snapshot', query, options);
    assert.equal(snap.rawTab, enabled);
    assert.equal(snap.flags.rawTab, enabled);
    return snap;
  };
  const initial = await snapshot(false);
  for (const key of keys) {
    process.env[key] = 'true';
    const enabled = await snapshot(true, { etag: initial.etag });
    process.env[key] = 'TRUE';
    await snapshot(false, { etag: enabled.etag });
    process.env[key] = '1';
  }
  await snapshot(false);
});

test('native snapshot aliases resolve a conversation and preserve sparse cursor, tail, delta, reset and raw contracts', async (t) => {
  const sessionsDir = makeTmp(t);
  const opts = { sessionsDir, conversationId: 'owner-conv' };
  const http = (route, query = {}) => dispatchPageApi({
    method: 'GET', url: new URL(route + '?' + new URLSearchParams(query), 'http://local'),
  }, opts).then((response) => response.body);
  assert.equal((await http('/api/swarm')).empty, true);
  await ensureSwarm({ swarmId: 'board', hostConversationId: 'owner-conv', cwd: sessionsDir, sessionsDir });
  await upsertWorker('board', { name: 'alice', session_id: 'alice-conv', task_summary: 'Review parser' }, opts);
  const first = await postBoardMessage('board', { from: 'alice', text: 'First result' }, opts);
  await postBoardMessage('unrelated', { text: 'Creates a sparse seq' }, opts);
  const last = await postBoardMessage('board', { text: '@alice Continue' }, opts);
  const full = await http('/api/swarm/snapshot', { tail: 1, omitRaw: 1 });
  assert.equal(full.self, 'coordinator');
  assert.deepEqual(full.members.map(({ name, isSelf }) => [name, isSelf]), [['alice', false], ['coordinator', true]]);
  assert.equal(full.members[0].last_post_seq, first.seq);
  assert.equal(full.plan.tasks.alice, 'Review parser');
  assert.equal(full.head, last.seq);
  assert.deepEqual(full.messages.map(({ seq }) => seq), [last.seq]);
  assert.equal(full.messagesOffset, 1);
  assert.equal(full.totalMessages, 2);
  assert.equal(full.hasMore, true);
  assert.equal('raw' in full, false);
  const cursor = { swarm_id: 'board', etag: full.etag, rosterKey: full.rosterKey, tail: 1, omitRaw: 1, known: 2, since: last.seq };
  assert.equal((await http('/api/swarm', cursor)).mode, 'unchanged');
  const old = { swarm_id: 'board', before: full.offset, beforeSeq: last.seq, rosterKey: full.rosterKey };
  assert.deepEqual((await http('/api/swarm/messages', { ...old, fromSeq: first.seq })).messages.map(({ seq }) => seq), [first.seq]);
  assert.equal((await http('/api/swarm/messages', { ...old, fromSeq: first.seq + 1 })).notFound, true);
  assert.equal((await http('/api/swarm/messages', { ...old, beforeSeq: 900 })).stale, true);
  const append = await postBoardMessage('board', { from: 'alice', text: '@coordinator [done] Done' }, opts);
  const delta = await http('/api/swarm', cursor);
  assert.equal(delta.mode, 'delta');
  assert.deepEqual(delta.messages.map(({ seq }) => seq), [append.seq]);
  await createChannel('board', { channel: 'parser' }, opts);
  assert.equal((await http('/api/swarm', cursor)).reset, true);
  assert.equal((await http('/api/swarm/messages', old)).stale, true);
  const raw = await http('/api/swarm/raw', { swarm_id: 'board', kind: 'members' });
  assert.equal(raw.content, raw.members);
  assert.equal(JSON.parse(raw.members).members.coordinator.conversation_id, 'owner-conv');
  assert.equal(raw.board.split('\n').filter(Boolean).length, 4);
  assert.equal((await http('/api/swarm', { swarm_id: 'unrelated', conversationId: 'untrusted' })).empty, true);
});

test('board_read channel override and a blocked board_wait preserve the global read cursor', async (t) => {
  const sessionsDir = makeTmp(t), opts = { sessionsDir };
  await ensureSwarm({ swarmId: 'cursor', sessionsDir });
  await upsertWorker('cursor', { name: 'alice', state: 'working' }, opts);
  await createChannel('cursor', { channel: 'parser' }, opts);
  const bell = await postBoardMessage('cursor', { channel: 'parser', text: '@alice read separately' }, opts);
  await postBoardMessage('another', { text: 'seq gap' }, opts);
  assert.deepEqual(await readBoardMessages('cursor', { viewer: 'alice', seqs: [] , ...opts }), []);
  assert.equal((await readBoardMessages('cursor', { viewer: 'alice', channel: 'parser', ...opts })).at(-1).seq, bell.seq);
  assert.equal((await readBoardMessages('cursor', { viewer: 'alice', channel: '*', ...opts })).at(-1).seq, bell.seq);
  assert.equal((await getWorker('cursor', 'alice', opts)).last_read_seq, 0);
  const blocked = await handleBoardAction({ action: 'wait', swarm_id: 'cursor', from: 'alice', since: 0 }, opts);
  assert.equal(blocked.park_blocked, true);
  assert.equal((await getWorker('cursor', 'alice', opts)).last_read_seq, 0);
  assert.equal((await getWorker('cursor', 'alice', opts)).last_park_blocked_seq, bell.seq);
  await readBoardMessages('cursor', { viewer: 'alice', seqs: [bell.seq], ...opts });
  assert.equal((await getWorker('cursor', 'alice', opts)).last_read_seq, bell.seq);
});

test('frame and narrator APIs enforce registration, root, magic and byte limits and relay scoped feedback', async (t) => {
  const sessionsDir = makeTmp(t);
  const cwd = join(sessionsDir, 'workspace');
  mkdirSync(cwd);
  const opts = { sessionsDir, workspaceRoots: [cwd], conversationId: 'viewer' };
  await ensureSwarm({ swarmId: 'visual', hostConversationId: 'viewer', cwd, sessionsDir });
  await upsertWorker('visual', { name: 'alice', state: 'waiting', source_cwd: cwd }, opts);
  await upsertWorker('visual', { name: 'narrator', role: 'narrator', state: 'waiting', source_cwd: cwd }, opts);
  const good = join(cwd, 'good.png'), wrong = join(cwd, 'wrong.png'), large = join(cwd, 'large.png');
  const external = join(sessionsDir, 'outside.png'), symlink = join(cwd, 'symlink.png');
  const png = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
  for (const path of [good, external]) writeFileSync(path, png);
  writeFileSync(wrong, 'plaintext');
  writeFileSync(large, Buffer.alloc(8 * 1024 * 1024 + 1));
  symlinkSync(external, symlink);
  const post = await postBoardMessage('visual', { from: 'alice', text: [good, wrong, large, external, symlink]
    .map((path) => `Screenshot: ${path} | Frame`).join('\n') }, opts);
  const request = (path, format, sidecarJson = false) => dispatchPageApi({ method: 'GET',
    url: new URL('/api/frame?' + new URLSearchParams({ swarm_id: 'visual', path, ...(format && { format }) }), 'http://local'),
  }, { ...opts, sidecarJson });
  const binary = await request(good);
  assert.equal(binary.type, 'image/png');
  assert.deepEqual(binary.body, png);
  assert.equal((await request(good, undefined, true)).body.data_url, 'data:image/png;base64,' + png.toString('base64'));
  for (const invalid of [wrong, large, external, symlink, join(cwd, 'unregistered.jpg')]) await assert.rejects(request(invalid), /Frame not found/);
  const publish = (agentName, args) => handleBoardMcpTool('board_publish_view', args, {}, { ...opts, swarmId: 'visual', agentName });
  const view = { headline: 'Progress', as_of_seq: post.seq, attention: [{ id: 'card', title: 'Review' }],
    reel: { frames: [{ path: good, seq: post.seq, caption: 'Narrator frame' }] } };
  await assert.rejects(publish('alice', { view }), /Only a live narrator/);
  await publish('narrator', { view, html: '<p>Review</p>' });
  await assert.rejects(publish('narrator', { view, html: 'x'.repeat(65537) }), /64 KiB/);
  await assert.rejects(handleBoardMcpTool('board_post', { text: 'No' }, {}, { ...opts, swarmId: 'visual', agentName: 'narrator' }), /read-only/);
  const frameState = await runSwarmOperation('snapshot', { swarm_id: 'visual' }, opts);
  assert.equal(frameState.frames[0].caption, 'Narrator frame');
  assert.equal(frameState.postedFrames.length, 5);
  assert.match(frameState.frames[0].frameUrl, /^\.\/api\/swarm\/frame\?/);
  const dir = narratorDirectory(sessionsDir, 'visual');
  for (const bad of ['{', ' '.repeat(65537)]) {
    writeFileSync(join(dir, 'view.json'), bad);
    assert.equal(readNarratorView(sessionsDir, 'visual').json.headline, 'Progress');
    assert.ok(readNarratorView(sessionsDir, 'visual').jsonError);
  }
  const feedback = (target_id, route = 'feedback') => runSwarmOperation(route, { swarm_id: 'visual', target_id,
    text: 'Looks good @all', ...(route === 'action' ? { action: 'feedback' } : {}) }, {
    ...opts, dispatchRunImpl: async ({ run_id }) => ({ run_id, session_id: 'narrator-session' }),
  });
  await assert.rejects(feedback('unlisted'), /Unknown narrator feedback target/);
  const relay = await feedback('card', 'action');
  assert.deepEqual(relay.message.mentions, ['narrator']);
  await publish('narrator', { view: { ...view, reel: { frames: [] }, feedback_ack: { through_seq: relay.message.seq, text: 'Read' } } });
  const fallback = await runSwarmOperation('snapshot', { swarm_id: 'visual' }, opts);
  assert.equal(fallback.frames[0].caption, 'Frame');
  assert.equal(fallback.view.json.feedback_ack.through_seq, relay.message.seq);
});

test('branch workspace clones isolate Git storage and kill cleans only clean share and branch roots', async (t) => {
  const sessionsDir = makeTmp(t);
  const cwd = join(sessionsDir, 'repo');
  const env = { ...process.env, GEMPT_WORKTREES_DIR: join(sessionsDir, 'worktrees') };
  delete env.GEMPT_WORKSPACE_ROOT;
  mkdirSync(cwd);
  const git = (dir, ...args) => execFileSync('git', ['-C', dir, ...args], { stdio: 'pipe', encoding: 'utf8' }).trim();
  git(cwd, 'init');
  writeFileSync(join(cwd, 'task.txt'), 'source');
  git(cwd, 'add', 'task.txt');
  git(cwd, '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.test', 'commit', '-m', 'start');
  const opts = { sessionsDir, env, dispatchRunImpl: async ({ run_id, workspace }, dispatch) => {
    assert.equal(workspace, 'share');
    const session_id = 'clean-branch';
    const allocation = ensureShareWorktree(cwd, session_id, dispatch.env);
    return { run_id, session_id, worktree_path: allocation.worktree_root, source_cwd: allocation.source_cwd };
  } };
  const add = await handleSwarmAction({ action: 'add', swarm_id: 'isolated', cwd, workspace: 'branch', name: 'clean', brief: 'Test' }, opts);
  const cloned = add.workers[0].worktree_path;
  assert.equal(git(cloned, 'remote', 'get-url', 'origin'), cwd);
  assert.equal(git(cloned, 'rev-parse', 'HEAD'), git(cwd, 'rev-parse', 'HEAD'));
  const dirty = ensureBranchWorkspace(cwd, 'dirty-branch', env).worktree_root;
  writeFileSync(join(dirty, 'keep.txt'), 'in progress');
  const share = ensureShareWorktree(cwd, 'clean-share', env).worktree_root;
  for (const [name, workspace, path] of [['dirty', 'branch', dirty], ['share', 'share', share]]) {
    await upsertWorker('isolated', { name, state: 'waiting', workspace, worktree_path: path, source_cwd: cwd }, opts);
  }
  await updateWorker('isolated', 'clean', { state: 'waiting' }, opts);
  await upsertWorker('isolated', { name: 'helper', added_by: 'clean', state: 'waiting' }, opts);
  for (const name of ['clean', 'dirty', 'share']) await handleSwarmAction({ action: 'kill', swarm_id: 'isolated', name }, opts);
  assert.equal(existsSync(cloned), false);
  assert.equal(existsSync(share), false);
  assert.equal(existsSync(join(dirty, 'keep.txt')), true);
  assert.equal((await getWorker('isolated', 'helper', opts)).state, 'waiting');
});

test('models tools expose installed-runner metadata and desired model switches at the next safe turn', async (t) => {
  const sessionsDir = makeTmp(t);
  const configPath = join(sessionsDir, 'config.json');
  const gateway = createServer((_, response) => {
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify({ data: [{ id: 'openai/catalog-example' }] }));
  });
  await new Promise((listen) => gateway.listen(0, '127.0.0.1', listen));
  t.after(() => gateway.close());
  writeFileSync(configPath, JSON.stringify({ magpie: { base_url: 'http://127.0.0.1:' + gateway.address().port },
    tiers: { sonnet: { exec_model: 'claude/custom-sonnet', fallback_models: ['codex/example'] } } }));
  const launches = [];
  const opts = { sessionsDir, configPath, dispatchRunImpl: async (args) => {
    launches.push(args);
    return { run_id: args.run_id, session_id: args.session_id ?? 'worker-sid', model: args.model };
  } };
  const add = await handleSwarmAction({ action: 'add', swarm_id: 'models', cwd: sessionsDir, name: 'alice', brief: 'Test', model: 'old' }, opts);
  assert.ok(add.open_board);
  const catalog = JSON.parse((await handleTool('gpt_swarm', { action: 'models', all: true }, {}, opts)).content[0].text);
  assert.ok(catalog.models.some(({ id, cli, tiers }) => id === 'claude/custom-sonnet' && cli === 'claude' && tiers.includes('sonnet')));
  assert.ok(catalog.models.some(({ id }) => id === 'codex/example'));
  assert.ok(catalog.models.some(({ id }) => id === 'openai/catalog-example'));
  assert.equal(catalog.gateway_reachable, true);
  const asAlice = { ...opts, swarmId: 'models', agentName: 'alice' };
  const workerCatalog = JSON.parse((await handleBoardMcpTool('board_swarm', { action: 'models', all: true }, {}, asAlice)).content[0].text);
  assert.deepEqual(workerCatalog.tiers, catalog.tiers);
  const run = add.workers[0].run_id;
  await handleBoardMcpTool('board_swarm', { action: 'model', model: 'new' }, {}, asAlice);
  assert.equal(launches.length, 1);
  let worker = await getWorker('models', 'alice', opts);
  assert.equal(worker.desired_model, 'new');
  assert.equal(worker.applied_model, 'old');
  await onWorkerTurnFinished({ swarmId: 'models', swarmAgent: 'alice', runId: run, result: { model: 'old' },
    outcome: { state: 'finished' }, ...opts });
  worker = await getWorker('models', 'alice', opts);
  assert.equal(worker.applied_model, 'new');
  assert.match(launches.at(-1).goal, /\[board\] The latest message is #/);
  await updateWorker('models', 'alice', { state: 'stopped' }, opts);
  const queued = await handleSwarmAction({ action: 'model', swarm_id: 'models', name: 'alice', model: 'third' }, opts);
  assert.equal(queued.pending, true);
  assert.equal(launches.length, 2);
  await handleBoardAction({ action: 'post', swarm_id: 'models', text: '@alice resume' }, opts);
  assert.equal((await getWorker('models', 'alice', opts)).applied_model, 'third');
});

test('workers keep explicit unlimited and finite timeouts on board wake and model switch', async (t) => {
  const sessionsDir = makeTmp(t);
  for (const [name, timeout] of [['unlimited', null], ['finite', 9000.5]]) {
    const launches = [];
    const opts = { sessionsDir, dispatchRunImpl: async (args) => {
      launches.push(args);
      return { run_id: args.run_id, session_id: name, model: args.model };
    } };
    const common = { swarm_id: name, name, cwd: sessionsDir };
    const long = '完整内容😀'.repeat(800);
    await handleSwarmAction({ ...common, action: 'add', brief: 'Initial work', timeout_sec: timeout }, opts);
    assert.equal((await getWorker(name, name, opts)).timeout_sec, timeout);
    const finish = (state) => onWorkerTurnFinished({ swarmId: name, swarmAgent: name, runId: launches.at(-1).run_id,
      result: { text: long }, error: state === 'failed' ? long : null, outcome: { state }, ...opts });
    assert.equal((await finish('finished')).auto_post.text, '@coordinator [done] ' + long);
    await handleBoardAction({ ...common, action: 'post', text: '@' + name + ' resume' }, opts);
    await finish('finished');
    await handleSwarmAction({ ...common, action: 'model', model: 'next' }, opts);
    assert.deepEqual(launches.map((args) => args.timeout_sec), [timeout, timeout, timeout]);
    assert.equal((await finish('failed')).auto_post.text, '@coordinator [failed] ' + long);
  }
});

test('monitor identifies all five native stalls, throttles retries, dormancy and resume', async (t) => {
  const sessionsDir = makeTmp(t);
  const opts = { sessionsDir };
  const swarmId = 'monitored';
  await ensureSwarm({ swarmId, sessionsDir });
  for (const [name, role, channels] of [['alice', 'worker', ['ui']], ['bob', 'worker', ['ui']], ['monitor', 'monitor', ['*']]]) {
    await upsertWorker(swarmId, { name, role, channels, state: 'waiting' }, opts);
  }
  await createChannel(swarmId, { channel: 'ui' }, opts);
  const { readSwarmSnapshot } = await import('../core/swarm-bus.mjs');
  const base = await readSwarmSnapshot({ swarmId, scope: 'all', sessionsDir });
  const message = (seq, from_agent, text, channel = 'main') => ({
    seq, from_agent, text, channel, mentions: parseMentions(text), channel_mentions: channelMentions(text), kind: 'message',
  });
  const snapshot = (...posts) => ({ ...base, messages: [...base.messages, ...posts] });
  const classify = (...posts) => detectMonitorStall(snapshot(...posts)).kind;
  assert.equal(classify(message(10, 'alice', '[done] Fixed UI')), 'unaddressed');
  assert.equal(classify(message(10, 'alice', '@coordinator Read this')), 'coordinator_unread');
  assert.equal(classify(message(10, 'alice', '@bob Please verify')), 'unanswered_mention');
  assert.equal(classify(message(10, 'alice', '@bob Please verify'), message(11, 'bob', '@alice Need your diff')), 'waiting_chain');
  assert.equal(classify(message(10, 'alice', '@bob Please verify', 'ui'), message(11, 'bob', '@alice Need your diff', 'ui')), 'channel_deadlock');
  const idle = snapshot(message(10, 'alice', '@bob Please verify'));
  const first = runMonitorTick(idle, {}, 100);
  assert.equal(first.alert.target, 'bob');
  let state = first.state;
  let alerts = 1;
  for (let i = 1; i <= 9; i++) {
    const tick = runMonitorTick(idle, state, 100 + i * MONITOR_INTERVAL_MS);
    state = tick.state;
    if (tick.alert) {
      assert.ok(i === 3 || i === 6);
      assert.equal(tick.alert.target, 'coordinator');
      alerts++;
    }
  }
  assert.equal(alerts, 3);
  assert.equal(state.dormant, true);
  const resume = runMonitorTick(snapshot(message(11, 'coordinator', '@monitor resume')), state, 100 + 10 * MONITOR_INTERVAL_MS);
  assert.equal(resume.state.dormant, false);
  const done = runMonitorTick(snapshot(message(12, 'coordinator', '[done] Complete')), resume.state, 100 + 11 * MONITOR_INTERVAL_MS);
  assert.equal(done.state.dormant, true);
  assert.equal(done.alert, null);
  const [published] = await sweepSwarmMonitors(opts, 5000);
  assert.equal(published.from_agent, 'monitor');
  assert.deepEqual(await sweepSwarmMonitors(opts, 5000), []);
});
