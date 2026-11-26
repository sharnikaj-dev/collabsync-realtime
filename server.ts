import { spawn, type ChildProcess } from 'child_process';
import express from 'express';
import http from 'http';
import net from 'net';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = Number(process.env.PORT || 3000);
const PY_HOST = '127.0.0.1';
const PY_PORT = Number(process.env.COLLABSYNC_WS_PORT || 8765);

let pyProcess: ChildProcess | null = null;

function startPythonBackend(): void {
  pyProcess = spawn(
    'python3',
    ['-m', 'server.main', '--host', PY_HOST, '--port', String(PY_PORT)],
    {
      cwd: __dirname,
      stdio: 'inherit',
      env: { ...process.env, PYTHONPATH: __dirname },
    }
  );

  pyProcess.on('exit', (code, signal) => {
    if (signal !== 'SIGTERM' && signal !== 'SIGINT') {
      console.error(`Python WebSocket server exited unexpectedly (code=${code}, signal=${signal})`);
    }
  });
}

function stopPythonBackend(): void {
  if (pyProcess && !pyProcess.killed) {
    pyProcess.kill('SIGTERM');
    pyProcess = null;
  }
}

function connectUpstreamWithRetry(
  retriesLeft: number,
  onConnect: (socket: net.Socket) => void,
  onError: (err: Error) => void
): void {
  const upstream = net.connect({ host: PY_HOST, port: PY_PORT });
  upstream.once('connect', () => onConnect(upstream));
  upstream.once('error', (err) => {
    upstream.destroy();
    if (retriesLeft > 0) {
      setTimeout(() => connectUpstreamWithRetry(retriesLeft - 1, onConnect, onError), 60);
    } else {
      onError(err);
    }
  });
}

async function main(): Promise<void> {
  startPythonBackend();

  const app = express();
  const clientDir = path.resolve(__dirname, 'client');

  app.get('/health', (_req, res) => {
    const req = http.get({ host: PY_HOST, port: PY_PORT, path: '/health', timeout: 1500 }, (upstreamRes) => {
      let body = '';
      upstreamRes.on('data', (chunk) => {
        body += chunk;
      });
      upstreamRes.on('end', () => {
        res.status(upstreamRes.statusCode || 200).type('application/json').send(body);
      });
    });
    req.on('error', () => {
      res.status(503).json({ status: 'starting' });
    });
  });

  app.use(express.static(clientDir));

  app.get('*', (_req, res) => {
    res.sendFile(path.join(clientDir, 'index.html'));
  });

  const server = http.createServer(app);

  server.on('upgrade', (req, clientSocket, head) => {
    connectUpstreamWithRetry(
      25,
      (upstream) => {
        const headerLines: string[] = [`${req.method} ${req.url} HTTP/${req.httpVersion}`];
        for (let i = 0; i < req.rawHeaders.length; i += 2) {
          headerLines.push(`${req.rawHeaders[i]}: ${req.rawHeaders[i + 1]}`);
        }
        upstream.write(headerLines.join('\r\n') + '\r\n\r\n');
        if (head && head.length > 0) {
          upstream.write(head);
        }

        upstream.pipe(clientSocket);
        clientSocket.pipe(upstream);

        upstream.on('error', () => clientSocket.destroy());
        clientSocket.on('error', () => upstream.destroy());
        upstream.on('close', () => clientSocket.destroy());
        clientSocket.on('close', () => upstream.destroy());
      },
      () => {
        clientSocket.write('HTTP/1.1 502 Bad Gateway\r\n\r\n');
        clientSocket.destroy();
      }
    );
  });

  server.listen(PORT, '0.0.0.0', () => {
    console.log(`CollabSync HTTP + WS proxy listening on http://0.0.0.0:${PORT}`);
  });

  const shutdown = () => {
    stopPythonBackend();
    server.close(() => process.exit(0));
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
  process.on('exit', stopPythonBackend);
}

main().catch((err) => {
  console.error(err);
  stopPythonBackend();
  process.exit(1);
});
